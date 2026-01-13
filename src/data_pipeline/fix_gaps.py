from __future__ import annotations

import os
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def fix_hourly_gaps(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    start = df["timestamp_utc"].min()
    end = df["timestamp_utc"].max()

    full_index = pd.date_range(start, end, freq="1h", tz="UTC")
    df = df.set_index("timestamp_utc").reindex(full_index)

    missing = int(df["close"].isna().sum())

    # Заполняем цену предыдущим close
    df["close"] = df["close"].ffill()

    # open/high/low тоже заполним close (это “плоская свеча”)
    for c in ["open", "high", "low"]:
        df[c] = df[c].fillna(df["close"])

    # volume: 0, если была пропущенная свеча
    df["volume"] = df["volume"].fillna(0)

    df = df.reset_index().rename(columns={"index": "timestamp_utc"})
    return df, missing


def main():
    setup_logger()
    s = get_settings()

    if not os.path.exists(s.data_raw_path):
        raise RuntimeError("❌ Raw-файл не найден. Сначала скачай данные download_ohlcv.")

    df = pd.read_parquet(s.data_raw_path)
    df_fixed, missing = fix_hourly_gaps(df)

    out_path = s.data_raw_path.replace(".parquet", "_fixed.parquet")
    df_fixed.to_parquet(out_path, index=False)

    logger.info(f"✅ Исправление пропусков завершено.\n")
    logger.info(f"Пропущенных свечей было: {missing}\n")
    logger.info(f"Сохранено: {out_path}\n")
    logger.info(f"Строк теперь: {len(df_fixed)}\n")
    logger.info(f"Период: {df_fixed['timestamp_utc'].min()}  →  {df_fixed['timestamp_utc'].max()}\n")


if __name__ == "__main__":
    main()
