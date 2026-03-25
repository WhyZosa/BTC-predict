from __future__ import annotations

import csv
import importlib.util
import os
import sys
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

from app.config import settings
from app.schemas import Candle, Horizon, PredictionResponse


RAW_INPUT_COLUMNS = ["timestamp_utc", "open", "high", "low", "close", "volume"]
HORIZON_TO_HOURS = {"6h": 6, "1d": 24, "1w": 24 * 7}
DEFAULT_REQUIRED_HISTORY_ROWS = 2017
ENSEMBLE_MODEL_NAME = "FinalModelEnsemble"


class PredictorUnavailableError(RuntimeError):
    pass


class PredictorService:
    def __init__(self) -> None:
        self._model: Any | None = None
        self._model_module: ModuleType | None = None
        self._model_error: str | None = None
        self._quality_cache: dict[str, dict[str, object]] | None = None

    def _load_model_module(self) -> ModuleType:
        model_path = settings.models_source_dir / "final_model.py"
        if not model_path.exists():
            raise PredictorUnavailableError(f"FinalModel file is missing: {model_path}")

        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
        spec = importlib.util.spec_from_file_location("btc_forecast_final_model", model_path)
        if spec is None or spec.loader is None:
            raise PredictorUnavailableError(f"Could not load FinalModel from {model_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    def _ensure_model(self) -> Any:
        if self._model is not None:
            return self._model

        if self._model_error is not None:
            raise PredictorUnavailableError(self._model_error)

        try:
            module = self._load_model_module()
            final_model_cls = getattr(module, "FinalModel", None)
            if final_model_cls is None:
                raise PredictorUnavailableError("FinalModel class was not found in Models/final_model.py")

            self._model_module = module
            self._model = final_model_cls(
                project_dir=settings.models_source_dir,
                models_dir=settings.models_source_dir,
                request_timeout=int(settings.request_timeout),
            )
            return self._model
        except ModuleNotFoundError as exc:
            missing_name = exc.name or "unknown dependency"
            self._model_error = (
                f"FinalModel dependencies are not installed in .venv. Missing module: {missing_name}. "
                "Install updated requirements before requesting predictions."
            )
            raise PredictorUnavailableError(self._model_error) from exc
        except PredictorUnavailableError as exc:
            self._model_error = str(exc)
            raise
        except Exception as exc:
            self._model_error = f"FinalModel could not be initialized: {exc}"
            raise PredictorUnavailableError(self._model_error) from exc

    def required_history_rows(self, horizon: Horizon) -> int:
        try:
            model = self._ensure_model()
            return max(DEFAULT_REQUIRED_HISTORY_ROWS, int(model.required_history_rows(horizon)))
        except PredictorUnavailableError:
            return DEFAULT_REQUIRED_HISTORY_ROWS

    def predict(self, horizon: Horizon, candles: list[Candle]) -> PredictionResponse:
        model = self._ensure_model()
        required_rows = max(DEFAULT_REQUIRED_HISTORY_ROWS, int(model.required_history_rows(horizon)))
        if len(candles) < required_rows:
            raise ValueError(
                f"FinalModel needs at least {required_rows} hourly candles for horizon {horizon}, "
                f"but only {len(candles)} candles were provided."
            )

        raw_frame = self._candles_to_frame(candles)
        pandas_module = getattr(self._model_module, "pd", None) if self._model_module is not None else None
        performance_warning = getattr(getattr(pandas_module, "errors", None), "PerformanceWarning", Warning)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", performance_warning)
            result = model.predict(raw_frame, horizon)

        current_price = float(result["current_close"])
        predicted_price = float(result["final_price"])
        catboost_price = float(result.get("catboost_price", predicted_price))
        patchtst_price = float(result.get("patchtst_price", predicted_price))
        disagreement_band = max(abs(catboost_price - predicted_price), abs(patchtst_price - predicted_price))

        metrics = self._load_backtest_metrics().get(horizon, {})
        rmse_band = float(metrics.get("rmse", 0.0) or 0.0)
        mae_band = float(metrics.get("mae", 0.0) or 0.0)
        confidence_band = max(disagreement_band, rmse_band, mae_band, current_price * 0.003)

        target_at = candles[-1].timestamp + timedelta(hours=HORIZON_TO_HOURS[horizon])
        delta_abs = predicted_price - current_price
        delta_pct = (delta_abs / current_price) * 100 if current_price else 0.0

        return PredictionResponse(
            symbol=settings.symbol,
            horizon=horizon,
            current_price=current_price,
            predicted_price=predicted_price,
            target_at=target_at,
            delta_abs=delta_abs,
            delta_pct=delta_pct,
            confidence_low=max(0.0, predicted_price - confidence_band),
            confidence_high=max(0.0, predicted_price + confidence_band),
            model_name=ENSEMBLE_MODEL_NAME,
            generated_at=datetime.now(UTC),
            features_used=RAW_INPUT_COLUMNS[:],
        )

    def get_quality_summary(self) -> dict[str, dict[str, object]]:
        metrics_by_horizon = self._load_backtest_metrics()
        summary: dict[str, dict[str, object]] = {}

        for horizon in ("6h", "1d", "1w"):
            metrics = metrics_by_horizon.get(horizon)
            if metrics is None:
                summary[horizon] = {"available": False}
                continue

            summary[horizon] = {
                "available": True,
                "model_name": ENSEMBLE_MODEL_NAME,
                "metrics": {
                    "mae": round(float(metrics["mae"]), 2) if metrics.get("mae") is not None else None,
                    "rmse": round(float(metrics["rmse"]), 2) if metrics.get("rmse") is not None else None,
                    "mape": round(float(metrics["mape"]), 4) if metrics.get("mape") is not None else None,
                    "directional_accuracy": round(float(metrics["directional_accuracy"]), 4)
                    if metrics.get("directional_accuracy") is not None
                    else None,
                },
                "metrics_source": "patchtst_test_reference",
            }

        return summary

    def _candles_to_frame(self, candles: list[Candle]) -> Any:
        if self._model_module is None:
            self._ensure_model()
        assert self._model_module is not None
        pandas_module = getattr(self._model_module, "pd")
        rows = [
            {
                "timestamp_utc": candle.timestamp.isoformat().replace("+00:00", "Z"),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
            for candle in candles
        ]
        return pandas_module.DataFrame(rows, columns=RAW_INPUT_COLUMNS)

    def _load_backtest_metrics(self) -> dict[str, dict[str, object]]:
        if self._quality_cache is not None:
            return self._quality_cache

        quality: dict[str, dict[str, object]] = {}
        for horizon in ("6h", "1d", "1w"):
            metrics_path = settings.models_source_dir / "patchtst_low_ram" / f"patchtst_{horizon}" / "metrics_val_test.csv"
            if not metrics_path.exists():
                continue

            with metrics_path.open("r", encoding="utf-8", newline="") as file:
                rows = list(csv.DictReader(file))

            if not rows:
                continue

            row = next((item for item in rows if item.get("split") == "test"), rows[-1])
            quality[horizon] = {
                "mae": self._to_float(row.get("mae_price")),
                "rmse": self._to_float(row.get("rmse_price")),
                "mape": self._to_float(row.get("mape_price")),
                "directional_accuracy": self._to_percent(row.get("directional_accuracy")),
            }

        self._quality_cache = quality
        return quality

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value in (None, "", "null"):
            return None
        return float(value)

    @staticmethod
    def _to_percent(value: object) -> float | None:
        number = PredictorService._to_float(value)
        if number is None:
            return None
        return number * 100 if number <= 1 else number


predictor_service = PredictorService()
