from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

# Корень проекта: .../src/common/config.py -> поднимаемся на 2 уровня вверх
ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"

# Явно загружаем .env и разрешаем перезапись переменных
load_dotenv(dotenv_path=ENV_PATH, override=True)


@dataclass(frozen=True)
class Settings:
    telegram_token: str
    model_api_url: str
    symbol: str
    timeframe: str
    exchange: str
    data_raw_path: str
    data_features_path: str
    tz: str


def get_settings() -> Settings:
    return Settings(
        telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
        model_api_url=os.getenv("MODEL_API_URL", "http://127.0.0.1:8000").strip(),
        symbol=os.getenv("SYMBOL", "BTC/USDT").strip(),
        timeframe=os.getenv("TIMEFRAME", "1h").strip(),
        exchange=os.getenv("EXCHANGE", "binance").strip(),
        data_raw_path=os.getenv("DATA_RAW_PATH", "data/raw/btcusdt_1h_fixed.parquet").strip(),
        data_features_path=os.getenv("DATA_FEATURES_PATH", "data/processed/features_1h.parquet").strip(),
        tz=os.getenv("TZ", "UTC").strip(),
    )


def require_telegram_token() -> str:
    """
    Используем в боте, чтобы сразу явно падать, если токен не задан.
    """
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    if (not token) or ("ВСТАВЬ_ТОКЕН" in token):
        raise RuntimeError("❌ TELEGRAM_TOKEN пустой или не заполнен. Открой .env и вставь токен бота.")
    return token
