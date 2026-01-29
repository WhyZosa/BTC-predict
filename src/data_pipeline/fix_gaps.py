from __future__ import annotations

import os
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def _pandas_freq(timeframe: str) -> str:
    tf = timeframe.strip().lower()
    if tf.endswith("m"):
        return f"{int(tf[:-1])}min"
    if tf.endswith("h"):
        return f"{int(tf[:-1])}h"
    if tf.endswith("d"):
        return f"{int(tf[:-1])}d"
    raise RuntimeError(f"❌ Не понимаю TIMEFRAME={timeframe}. Пример: 5m, 1h, 1d")


def fix_gaps(df: pd.DataFrame, timeframe: str) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    freq = _pandas_freq(timeframe)
    full_index = pd.date_range(df["timestamp_utc"].min(), df["timestamp_utc"].max(), freq=freq, tz="UTC")

    df = df.set_index("timestamp_utc").reindex(full_index)
    missing = int(df["close"].isna().sum())

    # плоская свеча на пропуске
    df["close"] = df["close"].ffill()
    for c in ["open", "high", "low"]:
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0)

    df = df.reset_index().rename(columns={"index": "timestamp_utc"})
    return df, missing


def main():
    setup_logger()
    s = get_settings()

    if not os.path.exists(s.data_raw_path):
        raise RuntimeError("❌ Raw-файл не найден. Сначала скачай данные.")

    df = pd.read_parquet(s.data_raw_path)
    df_fixed, missing = fix_gaps(df, s.timeframe)

    os.makedirs(os.path.dirname(s.data_fixed_path), exist_ok=True)
    df_fixed.to_parquet(s.data_fixed_path, index=False)

    logger.info("✅ Исправление пропусков завершено.\n")
    logger.info(f"Пропущенных свечей было: {missing}\n")
    logger.info(f"Сохранено: {s.data_fixed_path}\n")
    logger.info(f"Строк теперь: {len(df_fixed)}\n")


if __name__ == "__main__":
    main()
