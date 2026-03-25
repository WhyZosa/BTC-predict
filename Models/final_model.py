from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Union
import math

import joblib
import numpy as np
import pandas as pd
import requests
import torch
from catboost import CatBoostRegressor
from transformers import PatchTSTForRegression


RAW_REQUIRED_COLUMNS = ["timestamp_utc", "open", "high", "low", "close", "volume"]

ENSEMBLE_WEIGHTS = {
    "6h": {"w_catboost": 0.530444, "w_patchtst": 0.469556},
    "1d": {"w_catboost": 0.477574, "w_patchtst": 0.522426},
    "1w": {"w_catboost": 0.500000, "w_patchtst": 0.500000},
}


FALLBACK_HALVING_HEIGHT = 840_000
FALLBACK_HALVING_TIMESTAMP = pd.Timestamp("2024-04-20 00:09:00", tz="UTC")
BLOCK_SECONDS_APPROX = 600.0
HALVING_INTERVAL = 210_000
BLOCKS_PER_DAY_APPROX = 144


@dataclass(frozen=True)
class HorizonPaths:
    catboost_path: Path
    patch_dir: Path


class FinalModel:
    def __init__(
        self,
        project_dir = None,
        models_dir = None,
        device = None,
        request_timeout = 10,
    ):
        self.project_dir = Path(project_dir).resolve() if project_dir else Path(__file__).resolve().parent
        if models_dir is not None:
            self.models_dir = Path(models_dir).resolve()
        else:
            lower = self.project_dir / "models"
            upper = self.project_dir / "Models"
            if lower.exists():
                self.models_dir = lower
            elif upper.exists():
                self.models_dir = upper
            else:
                raise FileNotFoundError()
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.request_timeout = int(request_timeout)
        self.horizon_paths = {
            "6h": HorizonPaths(
                catboost_path=self.models_dir / "tuning_catboost" / "best_catboost_6h_multiquantile.cbm",
                patch_dir=self.models_dir / "patchtst_low_ram" / "patchtst_6h",
            ),
            "1d": HorizonPaths(
                catboost_path=self.models_dir / "tuning_catboost" / "best_catboost_1d_multiquantile.cbm",
                patch_dir=self.models_dir / "patchtst_low_ram" / "patchtst_1d",
            ),
            "1w": HorizonPaths(
                catboost_path=self.models_dir / "tuning_catboost" / "best_catboost_1w_multiquantile.cbm",
                patch_dir=self.models_dir / "patchtst_low_ram" / "patchtst_1w",
            ),
        }

        self.catboost_models = {}
        self.patch_models = {}
        self.patch_preprocessing = {}
        self.catboost_feature_cols = {}
        self.patch_feature_cols = {}
        self.patch_context_length = {}
        self._load_all_models()

    def predict(self, raw_input, horizon):
        horizon = self._normalize_horizon(horizon)
        raw_df = self._read_raw_input(raw_input)
        feature_df = self._build_feature_frame(raw_df)
        cat_pred_logret = self._predict_catboost(feature_df, horizon)
        patch_pred_logret = self._predict_patchtst(feature_df, horizon)
        weights = ENSEMBLE_WEIGHTS[horizon]
        final_logret = (
            weights["w_catboost"] * cat_pred_logret
            + weights["w_patchtst"] * patch_pred_logret
        )
        current_close = float(feature_df["close"].iloc[-1])
        current_timestamp = pd.Timestamp(feature_df["timestamp_utc"].iloc[-1])
        cat_price = current_close * math.exp(cat_pred_logret)
        patch_price = current_close * math.exp(patch_pred_logret)
        final_price = current_close * math.exp(final_logret)
        return {
            "horizon": horizon,
            "timestamp_utc": current_timestamp.isoformat(),
            "current_close": current_close,
            "catboost_logret": float(cat_pred_logret),
            "patchtst_logret": float(patch_pred_logret),
            "final_logret": float(final_logret),
            "catboost_price": float(cat_price),
            "patchtst_price": float(patch_price),
            "final_price": float(final_price),
            "weights": dict(weights),
        }

    def required_raw_columns(self):
        return RAW_REQUIRED_COLUMNS.copy()

    def required_history_rows(self, horizon):
        horizon = self._normalize_horizon(horizon)
        # CatBoost needs rolling window 2016 + prior diff/shift safety.
        return max(2017, self.patch_context_length[horizon])

    def required_columns(self, horizon):
        horizon = self._normalize_horizon(horizon)
        return {
            "raw_required_columns": self.required_raw_columns(),
            "catboost_feature_cols": self.catboost_feature_cols[horizon].copy(),
            "patchtst_feature_cols": self.patch_feature_cols[horizon].copy(),
        }

    def _load_all_models(self) -> None:
        for horizon, paths in self.horizon_paths.items():
            if not paths.catboost_path.exists():
                raise FileNotFoundError(f"Не найден CatBoost для {horizon}: {paths.catboost_path}")

            prep_path = paths.patch_dir / "preprocessing.joblib"
            hf_dir = paths.patch_dir / "hf_model"
            if not prep_path.exists():
                raise FileNotFoundError()
            if not hf_dir.exists():
                raise FileNotFoundError()
            cat_model = CatBoostRegressor()
            cat_model.load_model(str(paths.catboost_path))
            self.catboost_models[horizon] = cat_model
            cat_features = list(getattr(cat_model, "feature_names_", []) or [])
            if not cat_features:
                raise ValueError(f"Не удалось извлечь feature_names_ у CatBoost для {horizon}")
            self.catboost_feature_cols[horizon] = cat_features
            patch_model = PatchTSTForRegression.from_pretrained(str(hf_dir)).to(self.device)
            patch_model.eval()
            self.patch_models[horizon] = patch_model
            prep = joblib.load(prep_path)
            self.patch_preprocessing[horizon] = prep
            self.patch_feature_cols[horizon] = list(prep["seq_features"])
            self.patch_context_length[horizon] = int(prep["context_length"])

    def _read_raw_input(self, raw_input):
        if isinstance(raw_input, pd.DataFrame):
            df = raw_input.copy()
        else:
            path = Path(raw_input)
            if not path.exists():
                raise FileNotFoundError(f"Не найден файл: {path}")
            df = pd.read_csv(path, parse_dates=["timestamp_utc"], low_memory=False)
        missing = [c for c in RAW_REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(
                "Во входных сырых данных отсутствуют обязательные колонки: "
                + ", ".join(missing)
            )
        df = df[RAW_REQUIRED_COLUMNS].copy()
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = (
            df.dropna(subset=RAW_REQUIRED_COLUMNS)
              .sort_values("timestamp_utc")
              .drop_duplicates(subset=["timestamp_utc"])
              .reset_index(drop=True)
        )
        if len(df) < 2017:
            raise ValueError(
                "Сырых строк недостаточно для расчёта всех признаков. "
                "Нужно минимум ~2017 строк истории (лучше с запасом)."
            )

        return df

    def _normalize_horizon(self, horizon):
        h = str(horizon).strip().lower()
        mapping = {
            "6h": "6h",
            "6hours": "6h",
            "6 hours": "6h",
            "1d": "1d",
            "1day": "1d",
            "1 day": "1d",
            "1w": "1w",
            "1week": "1w",
            "1 week": "1w",
        }
        if h not in mapping:
            raise ValueError("horizon должен быть одним из: 6h, 1d, 1w")
        return mapping[h]

    def _build_feature_frame(self, raw_df):
        df = raw_df.copy()
        tip_height, tip_timestamp = self._get_tip_height_and_time()
        df["block_height_work"] = self._estimate_block_height(df["timestamp_utc"], tip_height, tip_timestamp)
        df["log_close"] = np.log(df["close"].clip(lower=1e-8)).astype("float32")
        df["log_open"] = np.log(df["open"].clip(lower=1e-8)).astype("float32")
        df["log_high"] = np.log(df["high"].clip(lower=1e-8)).astype("float32")
        df["log_low"] = np.log(df["low"].clip(lower=1e-8)).astype("float32")
        df["log_volume"] = np.log1p(df["volume"]).astype("float32")
        df["log_ret_1"] = np.log(df["close"] / df["close"].shift(1)).astype("float32")
        df["candle_body"] = ((df["close"] - df["open"]) / (df["open"] + 1e-8)).astype("float32")
        df["candle_range"] = ((df["high"] - df["low"]) / (df["close"] + 1e-8)).astype("float32")
        df["upper_wick"] = ((df["high"] - df[["open", "close"]].max(axis=1)) / (df["close"] + 1e-8)).astype("float32")
        df["lower_wick"] = ((df[["open", "close"]].min(axis=1) - df["low"]) / (df["close"] + 1e-8)).astype("float32")
        lag_windows_ret = [1, 2, 3, 6, 12, 24, 72, 288]
        lag_windows_vol = [1, 2, 3, 6, 12, 24, 72]
        for lag in lag_windows_ret:
            df[f"log_ret_lag_{lag}"] = df["log_ret_1"].shift(lag).astype("float32")
        for lag in lag_windows_vol:
            df[f"log_volume_lag_{lag}"] = df["log_volume"].shift(lag).astype("float32")
        for lag in [1, 2, 3, 6, 12, 24]:
            df[f"body_lag_{lag}"] = df["candle_body"].shift(lag).astype("float32")
            df[f"range_lag_{lag}"] = df["candle_range"].shift(lag).astype("float32")
        roll_windows = [3, 12, 24, 72, 288, 2016]
        for w in roll_windows:
            df[f"log_ret_mean_{w}"] = df["log_ret_1"].rolling(w, min_periods=w).mean().astype("float32")
            df[f"log_ret_std_{w}"] = df["log_ret_1"].rolling(w, min_periods=w).std().astype("float32")
            df[f"log_ret_min_{w}"] = df["log_ret_1"].rolling(w, min_periods=w).min().astype("float32")
            df[f"log_ret_max_{w}"] = df["log_ret_1"].rolling(w, min_periods=w).max().astype("float32")
            df[f"log_volume_mean_{w}"] = df["log_volume"].rolling(w, min_periods=w).mean().astype("float32")
            df[f"log_volume_std_{w}"] = df["log_volume"].rolling(w, min_periods=w).std().astype("float32")
            df[f"range_mean_{w}"] = df["candle_range"].rolling(w, min_periods=w).mean().astype("float32")
            df[f"range_std_{w}"] = df["candle_range"].rolling(w, min_periods=w).std().astype("float32")
        for span in [12, 72, 288]:
            df[f"ema_{span}"] = df["close"].ewm(span=span, adjust=False).mean().astype("float32")
            df[f"close_to_ema_{span}"] = (df["close"] / (df[f"ema_{span}"] + 1e-8) - 1).astype("float32")
        df["ema_12_to_72"] = (df["ema_12"] / (df["ema_72"] + 1e-8) - 1).astype("float32")
        df["ema_72_to_288"] = (df["ema_72"] / (df["ema_288"] + 1e-8) - 1).astype("float32")
        ema_fast = df["close"].ewm(span=12, adjust=False).mean()
        ema_slow = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = (ema_fast - ema_slow).astype("float32")
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean().astype("float32")
        df["macd_hist"] = (df["macd"] - df["macd_signal"]).astype("float32")
        df["rsi_14"] = self._make_rsi(df["close"], 14).astype("float32")
        df["rsi_72"] = self._make_rsi(df["close"], 72).astype("float32")
        df["atr_14"] = self._make_atr(df, 14).astype("float32")
        df["atr_72"] = self._make_atr(df, 72).astype("float32")
        df["atr_14_norm"] = (df["atr_14"] / (df["close"] + 1e-8)).astype("float32")
        df["atr_72_norm"] = (df["atr_72"] / (df["close"] + 1e-8)).astype("float32")
        for w in [20, 72]:
            ma = df["close"].rolling(w, min_periods=w).mean()
            sd = df["close"].rolling(w, min_periods=w).std()
            df[f"bb_width_{w}"] = ((4 * sd) / (ma + 1e-8)).astype("float32")
            df[f"bb_zscore_{w}"] = ((df["close"] - ma) / (sd + 1e-8)).astype("float32")
        ts = pd.to_datetime(df["timestamp_utc"], utc=True)
        df["hour"] = ts.dt.hour.astype("int16")
        df["dayofweek"] = ts.dt.dayofweek.astype("int16")
        df["dayofmonth"] = ts.dt.day.astype("int16")
        df["month"] = ts.dt.month.astype("int16")
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24).astype("float32")
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24).astype("float32")
        df["dow_sin"] = np.sin(2 * np.pi * df["dayofweek"] / 7).astype("float32")
        df["dow_cos"] = np.cos(2 * np.pi * df["dayofweek"] / 7).astype("float32")
        df["month_sin"] = np.sin(2 * np.pi * (df["month"] - 1) / 12).astype("float32")
        df["month_cos"] = np.cos(2 * np.pi * (df["month"] - 1) / 12).astype("float32")
        df = self._add_halving_features_by_block(df, block_col="block_height_work")
        df = df.replace([np.inf, -np.inf], np.nan)
        return df

    def _predict_catboost(self, feature_df, horizon):
        model = self.catboost_models[horizon]
        feature_cols = self.catboost_feature_cols[horizon]
        recent = feature_df.tail(1).copy()
        missing = [c for c in feature_cols if c not in recent.columns]
        if missing:
            raise ValueError(f"Во входных признаках CatBoost не хватает колонок: {missing}")
        recent = recent[feature_cols]
        if recent.isna().any(axis=None):
            bad = recent.columns[recent.isna().any()].tolist()
            raise ValueError(
                "В последней строке есть NaN в признаках CatBoost. "
                f"Проблемные колонки: {bad}"
            )
        pred = np.asarray(model.predict(recent))
        if pred.ndim == 2:
            return float(pred[0, 1])
        return float(pred.reshape(-1)[0])

    def _predict_patchtst(self, feature_df, horizon):
        model = self.patch_models[horizon]
        prep = self.patch_preprocessing[horizon]
        seq_features = self.patch_feature_cols[horizon]
        context_length = self.patch_context_length[horizon]
        if len(feature_df) < context_length:
            raise ValueError(
                f"Для PatchTST горизонта {horizon} нужно минимум {context_length} строк, "
                f"а передано только {len(feature_df)}"
            )
        recent = feature_df.tail(context_length).copy()
        missing = [c for c in seq_features if c not in recent.columns]
        if missing:
            raise ValueError(f"Во входных признаках PatchTST не хватает колонок: {missing}")
        recent = recent[seq_features]
        if recent.isna().any(axis=None):
            bad = recent.columns[recent.isna().any()].tolist()
            raise ValueError(
                f"В последних {context_length} строках есть NaN в признаках PatchTST. "
                f"Проблемные колонки: {bad}"
            )
        x_mean = np.asarray(prep["x_mean"], dtype=np.float32)
        x_std = np.asarray(prep["x_std"], dtype=np.float32)
        y_mean = float(prep["y_mean"])
        y_std = float(prep["y_std"])
        x_std[x_std < 1e-6] = 1.0
        if y_std < 1e-6:
            y_std = 1.0
        window = recent.to_numpy(dtype=np.float32)
        window = (window - x_mean) / x_std
        past_values = torch.tensor(window[None, ...], dtype=torch.float32, device=self.device)
        with torch.no_grad():
            outputs = model(past_values=past_values)
            pred_scaled = outputs.regression_outputs.detach().cpu().numpy().reshape(-1)[0]
        pred_raw = pred_scaled * y_std + y_mean
        return float(pred_raw)

    def _get_tip_height_and_time(self):
        providers = [
            "https://mempool.space/api",
            "https://blockstream.info/api",
        ]
        for base in providers:
            try:
                height_resp = requests.get(f"{base}/blocks/tip/height", timeout=self.request_timeout)
                height_resp.raise_for_status()
                tip_height = int(str(height_resp.text).strip())

                hash_resp = requests.get(f"{base}/blocks/tip/hash", timeout=self.request_timeout)
                hash_resp.raise_for_status()
                tip_hash = str(hash_resp.text).strip()

                block_resp = requests.get(f"{base}/block/{tip_hash}", timeout=self.request_timeout)
                block_resp.raise_for_status()
                block_json = block_resp.json()

                tip_timestamp = pd.to_datetime(int(block_json["timestamp"]), unit="s", utc=True)
                return tip_height, tip_timestamp
            except Exception:
                continue
        now_utc = pd.Timestamp.utcnow().tz_localize("UTC") if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow().tz_convert("UTC")
        blocks_since = int(round((now_utc - FALLBACK_HALVING_TIMESTAMP).total_seconds() / BLOCK_SECONDS_APPROX))
        tip_height = max(FALLBACK_HALVING_HEIGHT + blocks_since, FALLBACK_HALVING_HEIGHT)
        return tip_height, now_utc

    def _estimate_block_height(self, timestamps, tip_height, tip_timestamp):
        delta_sec = (tip_timestamp - pd.to_datetime(timestamps, utc=True)).dt.total_seconds()
        est = tip_height - np.round(delta_sec / BLOCK_SECONDS_APPROX)
        est = np.clip(est, 0, None)
        return pd.Series(est.astype(np.int64), index=timestamps.index)

    @staticmethod
    def _make_rsi(series, window = 14):
        delta = series.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        roll_up = up.ewm(alpha=1 / window, adjust=False).mean()
        roll_down = down.ewm(alpha=1 / window, adjust=False).mean()
        rs = roll_up / (roll_down + 1e-12)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _make_atr(df, window = 14):
        prev_close = df["close"].shift(1)
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - prev_close).abs()
        tr3 = (df["low"] - prev_close).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return true_range.ewm(alpha=1 / window, adjust=False).mean()

    @staticmethod
    def _add_halving_features_by_block(df, block_col = "block_height_work"):
        df = df.copy()
        df[block_col] = pd.to_numeric(df[block_col], errors="coerce")
        df["last_halving_height"] = (df[block_col] // HALVING_INTERVAL) * HALVING_INTERVAL
        df["next_halving_height"] = df["last_halving_height"] + HALVING_INTERVAL
        df["blocks_since_last_halving"] = (df[block_col] - df["last_halving_height"]).astype("float32")
        df["blocks_to_next_halving"] = (df["next_halving_height"] - df[block_col]).astype("float32")
        df["halving_cycle_progress"] = (df["blocks_since_last_halving"] / HALVING_INTERVAL).clip(0, 1).astype("float32")
        tau_blocks = 90 * BLOCKS_PER_DAY_APPROX
        min_blocks = df[["blocks_since_last_halving", "blocks_to_next_halving"]].min(axis=1)
        df["halving_proximity"] = np.exp(-min_blocks / tau_blocks).astype("float32")
        for days in [30, 90, 180]:
            thr = days * BLOCKS_PER_DAY_APPROX
            df[f"is_pre_halving_{days}"] = (df["blocks_to_next_halving"] <= thr).astype("int8")
            df[f"is_post_halving_{days}"] = (df["blocks_since_last_halving"] <= thr).astype("int8")
        return df

def load_model(*args: Any, **kwargs: Any):
    return FinalModel(*args, **kwargs)


def final_model(*args: Any, **kwargs: Any):
    return FinalModel(*args, **kwargs)

