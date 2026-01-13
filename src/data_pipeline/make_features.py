from __future__ import annotations

import json
import os
import numpy as np
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / (avg_loss + 1e-12)
    return 100 - (100 / (1 + rs))


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def macd(close: pd.Series):
    fast = ema(close, 12)
    slow = ema(close, 26)
    macd_line = fast - slow
    signal = ema(macd_line, 9)
    hist = macd_line - signal
    return macd_line, signal, hist


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def hurst_simple(x: np.ndarray) -> float:
    x = x.astype(float)
    if len(x) < 64 or np.any(np.isnan(x)):
        return np.nan

    lags = np.array([2, 4, 8, 16, 32], dtype=int)
    tau = []
    for lag in lags:
        diff = x[lag:] - x[:-lag]
        tau.append(np.sqrt(np.std(diff)))
    tau = np.array(tau)

    if np.any(tau <= 0):
        return np.nan

    poly = np.polyfit(np.log(lags), np.log(tau), 1)
    return float(poly[0] * 2.0)


def make_time_features(ts: pd.Series) -> pd.DataFrame:
    hour = ts.dt.hour.astype(int)
    dow = ts.dt.dayofweek.astype(int)
    out = pd.DataFrame(index=ts.index)
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    out["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    return out


def main():
    setup_logger()
    s = get_settings()

    if not os.path.exists(s.data_raw_path):
        raise RuntimeError("❌ Нет raw-данных. Сначала запусти download_ohlcv и fix_gaps.")

    df = pd.read_parquet(s.data_raw_path).copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    logger.info(f"📌 Загружено raw-строк: {len(df)}\n")

    # Доходность
    df["ret_1"] = np.log(df["close"]).diff()

    # Лаги
    for k in range(1, 25):
        df[f"ret_lag_{k}"] = df["ret_1"].shift(k)

    # Rolling
    for w in [6, 12, 24, 72, 168]:
        df[f"ret_mean_{w}"] = df["ret_1"].rolling(w).mean()
        df[f"ret_std_{w}"] = df["ret_1"].rolling(w).std()
        df[f"vol_{w}"] = df["ret_1"].rolling(w).std()

    # Range + ATR
    df["hl_range"] = (df["high"] - df["low"]) / (df["close"] + 1e-12)
    df["atr_14"] = atr(df, 14)

    # RSI / MACD / Bollinger
    df["rsi_14"] = rsi(df["close"], 14)
    macd_line, signal, hist = macd(df["close"])
    df["macd_line"] = macd_line
    df["macd_signal"] = signal
    df["macd_hist"] = hist

    bb_w = 20
    mid = df["close"].rolling(bb_w).mean()
    std = df["close"].rolling(bb_w).std()
    df["bb_mid"] = mid
    df["bb_up"] = mid + 2 * std
    df["bb_low"] = mid - 2 * std
    df["bb_width"] = (df["bb_up"] - df["bb_low"]) / (df["close"] + 1e-12)

    # Время
    df = pd.concat([df, make_time_features(df["timestamp_utc"])], axis=1)

    # Фракталы (Hurst)
    df["hurst_128"] = df["close"].rolling(128).apply(lambda x: hurst_simple(x.values), raw=False)
    df["hurst_256"] = df["close"].rolling(256).apply(lambda x: hurst_simple(x.values), raw=False)

    # Таргеты
    df["y_1h"] = np.log(df["close"].shift(-1) / df["close"])
    df["y_1d"] = np.log(df["close"].shift(-24) / df["close"])

    df_feat = df.dropna().reset_index(drop=True)
    logger.info(f"✅ После dropna осталось строк: {len(df_feat)}\n")

    os.makedirs(os.path.dirname(s.data_features_path), exist_ok=True)
    df_feat.to_parquet(s.data_features_path, index=False)
    logger.info(f"✅ Фичи сохранены: {s.data_features_path}\n")

    exclude = {"open", "high", "low", "close", "volume", "y_1h", "y_1d"}
    feature_cols = [c for c in df_feat.columns if c not in exclude]

    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/feature_list.json", "w", encoding="utf-8") as f:
        json.dump(feature_cols, f, indent=2, ensure_ascii=False)

    # Сплит по времени 70/15/15
    n = len(df_feat)
    i1 = int(n * 0.70)
    i2 = int(n * 0.85)
    splits = {
        "train": {"start": str(df_feat["timestamp_utc"].iloc[0]), "end": str(df_feat["timestamp_utc"].iloc[i1 - 1])},
        "val": {"start": str(df_feat["timestamp_utc"].iloc[i1]), "end": str(df_feat["timestamp_utc"].iloc[i2 - 1])},
        "test": {"start": str(df_feat["timestamp_utc"].iloc[i2]), "end": str(df_feat["timestamp_utc"].iloc[-1])},
    }
    with open("data/processed/splits.json", "w", encoding="utf-8") as f:
        json.dump(splits, f, indent=2, ensure_ascii=False)

    logger.info("✅ Сохранены: data/processed/feature_list.json и data/processed/splits.json\n")


if __name__ == "__main__":
    main()
