import os
import time
from typing import Literal, Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision")
SYMBOL_DEFAULT = os.getenv("SYMBOL_DEFAULT", "BTCUSDT")

Interval = Literal["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"]

app = FastAPI(title="BTC-predict API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_client: Optional[httpx.AsyncClient] = None

@app.on_event("startup")
async def _startup():
    global _client
    _client = httpx.AsyncClient(timeout=10.0)

@app.on_event("shutdown")
async def _shutdown():
    global _client
    if _client:
        await _client.aclose()
        _client = None

async def _get_json(path: str, params: dict) -> Any:
    assert _client is not None
    url = f"{BINANCE_BASE_URL}{path}"
    r = await _client.get(url, params=params)
    r.raise_for_status()
    return r.json()

@app.get("/api/health")
async def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/api/price")
async def price(symbol: str = SYMBOL_DEFAULT):
    data = await _get_json("/api/v3/ticker/price", {"symbol": symbol})
    return {"symbol": data["symbol"], "price": float(data["price"]), "ts": int(time.time())}

@app.get("/api/ohlcv")
async def ohlcv(
    symbol: str = SYMBOL_DEFAULT,
    interval: Interval = Query("1h"),
    limit: int = Query(300, ge=1, le=1000),
    startTime: Optional[int] = Query(None, description="ms timestamp"),
    endTime: Optional[int] = Query(None, description="ms timestamp"),
):
    params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime

    raw: List[list] = await _get_json("/api/v3/klines", params)

    out = []
    for k in raw:
        out.append({
            "time": int(k[0] // 1000),
            "open": float(k[1]),
            "high": float(k[2]),
            "low":  float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return {"symbol": symbol, "interval": interval, "candles": out}

@app.get("/")
async def root():
    return {"service": "BTC-predict API", "docs": "/docs"}
