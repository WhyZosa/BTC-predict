from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.schemas import Candle


HORIZON_TO_STEPS = {
    "6h": 6,
    "1d": 24,
    "1w": 24 * 7,
}

FEATURE_NAMES = [
    "close",
    "return_1",
    "return_6",
    "return_24",
    "range_pct",
    "body_pct",
    "volatility_6",
    "volatility_24",
    "volume_ratio",
    "ma_gap_6",
    "ma_gap_24",
    "ma_gap_72",
    "momentum_6",
    "momentum_24",
    "hurst_proxy",
    "vol_regime",
]


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def pct_change(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (current - previous) / previous


def hurst_proxy(closes: list[float]) -> float:
    if len(closes) < 8:
        return 0.5
    avg = mean(closes)
    centered = [value - avg for value in closes]
    cumulative = []
    running = 0.0
    for value in centered:
        running += value
        cumulative.append(running)
    r = max(cumulative) - min(cumulative)
    s = std(closes)
    if r == 0 or s == 0:
        return 0.5
    return math.log(r / s) / math.log(len(closes))


def extract_features(candles: list["Candle"], index: int) -> dict[str, float] | None:
    if index < 72:
        return None

    current = candles[index]
    prev_1 = candles[index - 1]
    prev_6 = candles[index - 6]
    prev_24 = candles[index - 24]

    closes_6 = [candles[item].close for item in range(index - 5, index + 1)]
    closes_24 = [candles[item].close for item in range(index - 23, index + 1)]
    closes_72 = [candles[item].close for item in range(index - 71, index + 1)]
    volumes_24 = [candles[item].volume for item in range(index - 23, index + 1)]
    returns_6 = [pct_change(candles[item].close, candles[item - 1].close) for item in range(index - 5, index + 1)]
    returns_24 = [pct_change(candles[item].close, candles[item - 1].close) for item in range(index - 23, index + 1)]

    ma_6 = mean(closes_6)
    ma_24 = mean(closes_24)
    ma_72 = mean(closes_72)
    vol_6 = std(returns_6)
    vol_24 = std(returns_24)
    volume_mean_24 = mean(volumes_24)

    return {
        "close": current.close,
        "return_1": pct_change(current.close, prev_1.close),
        "return_6": pct_change(current.close, prev_6.close),
        "return_24": pct_change(current.close, prev_24.close),
        "range_pct": (current.high - current.low) / current.close if current.close else 0.0,
        "body_pct": (current.close - current.open) / current.open if current.open else 0.0,
        "volatility_6": vol_6,
        "volatility_24": vol_24,
        "volume_ratio": current.volume / volume_mean_24 if volume_mean_24 else 1.0,
        "ma_gap_6": (current.close - ma_6) / ma_6 if ma_6 else 0.0,
        "ma_gap_24": (current.close - ma_24) / ma_24 if ma_24 else 0.0,
        "ma_gap_72": (current.close - ma_72) / ma_72 if ma_72 else 0.0,
        "momentum_6": current.close - prev_6.close,
        "momentum_24": current.close - prev_24.close,
        "hurst_proxy": hurst_proxy(closes_24[-12:]),
        "vol_regime": vol_6 / vol_24 if vol_24 else 1.0,
    }


def latest_features(candles: list["Candle"]) -> tuple[dict[str, float], list[str]]:
    features = extract_features(candles, len(candles) - 1)
    if features is None:
        raise ValueError("Not enough history for feature extraction")
    return features, FEATURE_NAMES[:]


def build_training_rows(candles: list["Candle"], horizon: str) -> tuple[list[dict[str, float]], list[float], list[float]]:
    steps = HORIZON_TO_STEPS[horizon]
    rows: list[dict[str, float]] = []
    targets: list[float] = []
    current_prices: list[float] = []

    for index in range(72, len(candles) - steps):
        features = extract_features(candles, index)
        if features is None:
            continue
        current_close = candles[index].close
        future_close = candles[index + steps].close
        future_return = pct_change(future_close, current_close)
        rows.append(features)
        targets.append(future_return)
        current_prices.append(current_close)

    return rows, targets, current_prices
