from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

# --- FastAPI app ---
app = FastAPI(title="BTC Predict API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        # потом добавим домен телеграм webapp
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TF = Literal["5m", "1h", "4h", "1d"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # BTC-predict/
DATA_RAW = PROJECT_ROOT / "data" / "raw"

# Порядок предпочтения файлов
CANDIDATES = [
    DATA_RAW / "btcusdt_5m_fixed.parquet",
    DATA_RAW / "btcusdt_5m.parquet",
    DATA_RAW / "btcusdt_1h_fixed.parquet",
    DATA_RAW / "btcusdt_1h.parquet",
]

BASE_DF: Optional[pd.DataFrame] = None
BASE_SOURCE: str = ""


def detect_source_file() -> Path:
    for p in CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError(
        "Не найден parquet в data/raw. Ожидал один из:\n" + "\n".join(str(x) for x in CANDIDATES)
    )


def load_base_df() -> tuple[pd.DataFrame, str]:
    path = detect_source_file()
    df = pd.read_parquet(path)

    # ожидаем колонки: timestamp/open/high/low/close/volume
    # иногда timestamp может быть индексом — обработаем оба варианта
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.drop(columns=["timestamp"])
        df.index = ts
    else:
        # индекс
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    # чистим мусор
    df = df[~df.index.isna()].copy()
    df = df.sort_index()

    needed = {"open", "high", "low", "close"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"Parquet не содержит нужных колонок {needed}. Есть: {list(df.columns)}")

    # иногда volume может отсутствовать
    if "volume" not in df.columns:
        df["volume"] = 0.0

    # убираем нулевые/отрицательные цены на всякий
    for c in ["open", "high", "low", "close"]:
        df = df[df[c] > 0]

    source = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    logger.info(f"Загрузил базу: {source}, rows={len(df)}, {df.index.min()} -> {df.index.max()}")
    return df, source


def ensure_loaded() -> None:
    global BASE_DF, BASE_SOURCE
    if BASE_DF is None:
        BASE_DF, BASE_SOURCE = load_base_df()


def resample_df(df: pd.DataFrame, tf: TF) -> tuple[pd.DataFrame, str]:
    if tf == "5m":
        return df, f"parquet:{tf} ({BASE_SOURCE})"

    rule_map = {
        "1h": "1H",
        "4h": "4H",
        "1d": "1D",
    }
    rule = rule_map[tf]

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    out = (
        df.resample(rule, label="right", closed="right")
        .agg(agg)
        .dropna()
    )

    return out, f"resample:{tf} from base ({BASE_SOURCE})"


@app.get("/")
def root():
    return {"ok": True, "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/ohlcv")
def ohlcv(
    tf: TF = Query("1h"),
    limit: int = Query(800, ge=10, le=5000),
):
    ensure_loaded()
    assert BASE_DF is not None

    df_tf, src = resample_df(BASE_DF, tf)
    if df_tf.empty:
        raise HTTPException(status_code=404, detail="Нет данных для этого tf")

    df_tail = df_tf.tail(limit).copy()

    # ВАЖНО: отдаём time в UNIX seconds (int) и строго по возрастанию
    candles = []
    for ts, row in df_tail.iterrows():
        candles.append(
            {
                "time": int(ts.timestamp()),  # seconds!
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
        )

    # на всякий — сортировка
    candles.sort(key=lambda x: x["time"])

    last_ts = df_tail.index[-1]
    updated_at = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    logger.info(f"Отдал свечи: tf={tf}, limit={limit}, last={last_ts}")

    return {
        "tf": tf,
        "limit": limit,
        "source": src,
        "updated_at": updated_at,
        "last_candle_iso": last_ts.isoformat(),
        "data": candles,
    }
