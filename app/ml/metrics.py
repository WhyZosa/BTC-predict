from __future__ import annotations

import math


def mae(y_true: list[float], y_pred: list[float]) -> float:
    return sum(abs(true - pred) for true, pred in zip(y_true, y_pred)) / max(len(y_true), 1)


def rmse(y_true: list[float], y_pred: list[float]) -> float:
    mse = sum((true - pred) ** 2 for true, pred in zip(y_true, y_pred)) / max(len(y_true), 1)
    return math.sqrt(mse)


def smape(y_true: list[float], y_pred: list[float]) -> float:
    total = 0.0
    for true, pred in zip(y_true, y_pred):
        denominator = abs(true) + abs(pred)
        total += 0.0 if denominator == 0 else (2 * abs(pred - true) / denominator)
    return (total / max(len(y_true), 1)) * 100


def directional_accuracy(y_true: list[float], y_pred: list[float], current: list[float]) -> float:
    matches = 0
    for true, pred, now in zip(y_true, y_pred, current):
        true_dir = 1 if true - now >= 0 else -1
        pred_dir = 1 if pred - now >= 0 else -1
        if true_dir == pred_dir:
            matches += 1
    return (matches / max(len(y_true), 1)) * 100
