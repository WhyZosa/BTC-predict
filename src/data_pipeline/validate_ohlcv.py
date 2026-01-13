from __future__ import annotations

import json
import os
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def validate(path: str, timeframe: str) -> dict:
    if not os.path.exists(path):
        raise RuntimeError(f"❌ Файл с данными не найден: {path}. Сначала запусти download_ohlcv.")

    df = pd.read_parquet(path).copy()
    if df.empty:
        raise RuntimeError("❌ Файл есть, но данных нет (пустой датафрейм).")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    # Дубликаты
    duplicates = int(df.duplicated(subset=["timestamp_utc"]).sum())

    # Валидность свечей
    invalid_high = int((df["high"] < df[["open", "close"]].max(axis=1)).sum())
    invalid_low = int((df["low"] > df[["open", "close"]].min(axis=1)).sum())

    # Отрицательные значения
    negative_prices = int(((df[["open", "high", "low", "close"]] <= 0).any(axis=1)).sum())
    negative_volume = int((df["volume"] < 0).sum())

    # Пропуски по времени (для 1h)
    gaps = None
    if timeframe == "1h":
        expected = pd.date_range(
            df["timestamp_utc"].min(),
            df["timestamp_utc"].max(),
            freq="1h",
            tz="UTC",
        )
        missing = expected.difference(df["timestamp_utc"])
        gaps = int(len(missing))

    report = {
        "строк": int(len(df)),
        "начало": str(df["timestamp_utc"].min()),
        "конец": str(df["timestamp_utc"].max()),
        "дубликаты_timestamp": duplicates,
        "ошибки_high": invalid_high,
        "ошибки_low": invalid_low,
        "отрицательные_цены": negative_prices,
        "отрицательный_объём": negative_volume,
        "пропуски_1h": gaps,
    }
    return report


def main():
    setup_logger()
    s = get_settings()

    rep = validate(s.data_raw_path, s.timeframe)

    logger.info("📋 Отчёт проверки данных:\n")
    logger.info(json.dumps(rep, indent=2, ensure_ascii=False) + "\n")

    os.makedirs("data/processed", exist_ok=True)
    out = "data/processed/validation_report.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Отчёт сохранён: {out}\n")


if __name__ == "__main__":
    main()
