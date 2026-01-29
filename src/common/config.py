from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)


@dataclass(frozen=True)
class Settings:
    telegram_token: str
    model_api_url: str
    symbol: str
    timeframe: str
    exchange: str
    data_raw_path: str
    data_fixed_path: str
    data_export_dir: str
    data_export_basename: str
    tz: str


def get_settings() -> Settings:
    return Settings(
        telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
        model_api_url=os.getenv("MODEL_API_URL", "http://127.0.0.1:8000").strip(),
        symbol=os.getenv("SYMBOL", "BTC/USDT").strip(),
        timeframe=os.getenv("TIMEFRAME", "5m").strip(),
        exchange=os.getenv("EXCHANGE", "binance").strip(),
        data_raw_path=os.getenv("DATA_RAW_PATH", "data/raw/btcusdt_5m.parquet").strip(),
        data_fixed_path=os.getenv("DATA_FIXED_PATH", "data/raw/btcusdt_5m_fixed.parquet").strip(),
        data_export_dir=os.getenv("DATA_EXPORT_DIR", "data/share").strip(),
        data_export_basename=os.getenv("DATA_EXPORT_BASENAME", "btcusdt_5m_clean").strip(),
        tz=os.getenv("TZ", "UTC").strip(),
    )
