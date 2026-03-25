from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


@dataclass(slots=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "BTC Forecast Mini App")
    app_env: str = os.getenv("APP_ENV", "development")
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "12"))
    symbol: str = os.getenv("SYMBOL", "BTCUSDT")
    vs_currency: str = os.getenv("VS_CURRENCY", "usd")
    price_refresh_seconds: int = int(os.getenv("PRICE_REFRESH_SECONDS", "15"))
    history_limit: int = int(os.getenv("HISTORY_LIMIT", "240"))
    redis_url: str = os.getenv("REDIS_URL", "")
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_mini_app_url: str = os.getenv("TELEGRAM_MINI_APP_URL", "http://127.0.0.1:8000")
    telegram_menu_button_text: str = os.getenv("TELEGRAM_MENU_BUTTON_TEXT", "Open BTC Forecast")
    alert_check_seconds: int = int(os.getenv("ALERT_CHECK_SECONDS", "30"))
    ngrok_authtoken: str = os.getenv("NGROK_AUTHTOKEN", "")
    data_dir: Path = BASE_DIR / os.getenv("DATA_DIR", "data")
    database_path: Path = BASE_DIR / os.getenv("DATABASE_PATH", "data/app.db")
    model_dir: Path = BASE_DIR / os.getenv("MODEL_DIR", "artifacts")
    models_source_dir: Path = BASE_DIR / os.getenv("MODELS_SOURCE_DIR", "Models")
    static_dir: Path = BASE_DIR / "app" / "static"


settings = Settings()
