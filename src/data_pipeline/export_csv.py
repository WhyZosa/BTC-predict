from __future__ import annotations

import os
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def main():
    setup_logger()
    s = get_settings()

    if not os.path.exists(s.data_fixed_path):
        raise RuntimeError("❌ Нет fixed-файла. Сначала запусти: python -m src.data_pipeline.fix_gaps")

    df = pd.read_parquet(s.data_fixed_path).copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    # Оставляем строго нужные колонки
    cols = ["timestamp_utc", "open", "high", "low", "close", "volume"]
    df = df[cols].sort_values("timestamp_utc").reset_index(drop=True)

    os.makedirs(s.data_export_dir, exist_ok=True)

    csv_path = os.path.join(s.data_export_dir, f"{s.data_export_basename}.csv")
    gz_path = os.path.join(s.data_export_dir, f"{s.data_export_basename}.csv.gz")

    # Обычный CSV (может быть большим)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    # Сжатый CSV (лучше для передачи)
    df.to_csv(gz_path, index=False, compression="gzip", encoding="utf-8")

    logger.info("✅ Экспорт завершён.\n")
    logger.info(f"CSV: {csv_path}\n")
    logger.info(f"CSV.GZ: {gz_path}\n")
    logger.info(f"Строк: {len(df)}\n")
    logger.info(f"Период: {df['timestamp_utc'].min()} → {df['timestamp_utc'].max()}\n")


if __name__ == "__main__":
    main()
