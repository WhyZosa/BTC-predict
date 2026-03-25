from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.config import settings
from app.ml.features import FEATURE_NAMES, HORIZON_TO_STEPS, build_training_rows
from app.ml.metrics import directional_accuracy, mae, rmse, smape
from app.schemas import Candle


def download_binance(symbol: str, interval: str, limit: int) -> list[Candle]:
    response = httpx.get(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": min(limit, 1000)},
        timeout=20,
        headers={"User-Agent": "btc-forecast-miniapp-trainer/1.0"},
    )
    response.raise_for_status()
    rows = []
    for item in response.json():
        rows.append(
            Candle.from_dict(
                {
                    "timestamp": datetime.fromtimestamp(item[0] / 1000, UTC).isoformat().replace("+00:00", "Z"),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                }
            )
        )
    return rows


def split_rows(rows: list[dict[str, float]], targets: list[float], prices: list[float]) -> tuple[list[dict[str, float]], list[dict[str, float]], list[float], list[float], list[float]]:
    split = max(1, int(len(rows) * 0.8))
    return rows[:split], rows[split:], targets[:split], targets[split:], prices[split:]


def fit_linear_regression(rows: list[dict[str, float]], targets: list[float], epochs: int = 450, lr: float = 0.02, l2: float = 1e-4) -> dict[str, object]:
    means = {}
    stds = {}
    for name in FEATURE_NAMES:
        values = [row[name] for row in rows]
        mean_value = sum(values) / len(values)
        variance = sum((value - mean_value) ** 2 for value in values) / len(values)
        means[name] = mean_value
        stds[name] = variance ** 0.5 or 1.0

    normalized_rows = []
    for row in rows:
        normalized_rows.append({name: (row[name] - means[name]) / stds[name] for name in FEATURE_NAMES})

    weights = {name: 0.0 for name in FEATURE_NAMES}
    bias = 0.0
    sample_count = max(len(normalized_rows), 1)

    for _ in range(epochs):
        grad_w = {name: 0.0 for name in FEATURE_NAMES}
        grad_b = 0.0
        for row, target in zip(normalized_rows, targets):
            prediction = bias + sum(weights[name] * row[name] for name in FEATURE_NAMES)
            error = prediction - target
            for name in FEATURE_NAMES:
                grad_w[name] += error * row[name]
            grad_b += error
        for name in FEATURE_NAMES:
            weights[name] -= lr * ((grad_w[name] / sample_count) + l2 * weights[name])
        bias -= lr * (grad_b / sample_count)

    return {
        "model_name": "LinearRegressionGD",
        "feature_names": FEATURE_NAMES,
        "means": means,
        "stds": stds,
        "weights": weights,
        "bias": bias,
    }


def predict_return(model: dict[str, object], row: dict[str, float]) -> float:
    prediction = float(model["bias"])
    means = dict(model["means"])
    stds = dict(model["stds"])
    weights = dict(model["weights"])
    for name in FEATURE_NAMES:
        normalized = (row[name] - float(means[name])) / (float(stds[name]) or 1.0)
        prediction += float(weights[name]) * normalized
    return prediction


def train_for_horizon(candles: list[Candle], horizon: str) -> dict[str, float]:
    rows, targets, current_prices = build_training_rows(candles, horizon)
    train_rows, test_rows, train_targets, test_targets, test_prices = split_rows(rows, targets, current_prices)
    model = fit_linear_regression(train_rows, train_targets)

    predicted_returns = [predict_return(model, row) for row in test_rows]
    predicted_prices = [price * (1 + predicted) for price, predicted in zip(test_prices, predicted_returns)]
    true_prices = [price * (1 + target) for price, target in zip(test_prices, test_targets)]
    baseline_prices = test_prices[:]

    residuals = [pred - true for pred, true in zip(predicted_returns, test_targets)]
    residual_std_return = (sum(error * error for error in residuals) / max(len(residuals), 1)) ** 0.5
    metrics = {
        "mae": mae(true_prices, predicted_prices),
        "rmse": rmse(true_prices, predicted_prices),
        "smape": smape(true_prices, predicted_prices),
        "directional_accuracy": directional_accuracy(true_prices, predicted_prices, test_prices),
        "baseline_mae": mae(true_prices, baseline_prices),
        "baseline_rmse": rmse(true_prices, baseline_prices),
    }

    model["residual_std_return"] = residual_std_return
    model["steps_ahead"] = HORIZON_TO_STEPS[horizon]
    model["metrics"] = metrics
    model["use_baseline"] = metrics["baseline_mae"] < metrics["mae"]

    target_dir = settings.model_dir / horizon
    target_dir.mkdir(parents=True, exist_ok=True)
    with (target_dir / "model.json").open("w", encoding="utf-8") as file:
        json.dump(model, file, indent=2)

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Train BTC price forecast models")
    parser.add_argument("--provider", choices=["binance"], default="binance")
    parser.add_argument("--symbol", default=settings.symbol)
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    if args.provider != "binance":
        raise ValueError(f"Unsupported provider: {args.provider}")

    candles = download_binance(args.symbol, args.interval, args.limit)
    print(f"Downloaded {len(candles)} rows for {args.symbol}")
    settings.model_dir.mkdir(parents=True, exist_ok=True)

    for horizon in ("6h", "1d", "1w"):
        metrics = train_for_horizon(candles, horizon)
        print(horizon, metrics)


if __name__ == "__main__":
    main()
