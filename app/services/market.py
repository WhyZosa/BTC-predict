from __future__ import annotations

import threading
from datetime import UTC, datetime

import httpx

from app.config import settings
from app.schemas import Candle, MarketSnapshot, PricePoint
from app.services.cache import MemoryCache


class MarketDataError(RuntimeError):
    pass


class MarketDataService:
    def __init__(self) -> None:
        self._cache = MemoryCache()
        self._refresh_lock = threading.Lock()
        self._client = httpx.Client(timeout=settings.request_timeout, headers={"User-Agent": "btc-forecast-miniapp/1.0"})

    def close(self) -> None:
        self._client.close()

    def get_snapshot(self, limit: int | None = None) -> MarketSnapshot:
        limit = limit or settings.history_limit
        cache_key = f"snapshot:{settings.symbol}:{limit}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return MarketSnapshot.from_dict(cached)

        with self._refresh_lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return MarketSnapshot.from_dict(cached)

            latest = self._fetch_latest_price()
            candles = self._fetch_history(limit=limit)
            change_24h = None
            if len(candles) >= 24:
                old_close = candles[-24].close
                if old_close:
                    change_24h = ((latest.price - old_close) / old_close) * 100

            payload = MarketSnapshot(symbol=settings.symbol, latest=latest, change_24h_pct=change_24h, candles=candles)
            self._cache.set(cache_key, payload.to_dict(), ttl_seconds=settings.price_refresh_seconds)
            return payload

    def _fetch_latest_price(self) -> PricePoint:
        providers = (self._fetch_latest_binance, self._fetch_latest_coingecko)
        errors: list[str] = []
        for provider in providers:
            try:
                return provider()
            except Exception as exc:
                errors.append(f"{provider.__name__}: {exc}")
        raise MarketDataError("; ".join(errors))

    def _fetch_history(self, limit: int) -> list[Candle]:
        providers = (lambda: self._fetch_history_binance(limit), lambda: self._fetch_history_coingecko(limit))
        errors: list[str] = []
        for provider in providers:
            try:
                candles = provider()
                if candles:
                    return candles
            except Exception as exc:
                errors.append(str(exc))
        raise MarketDataError("; ".join(errors))

    def _fetch_latest_binance(self) -> PricePoint:
        response = self._client.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": settings.symbol},
        )
        response.raise_for_status()
        payload = response.json()
        return PricePoint(timestamp=datetime.now(UTC), price=float(payload["price"]), source="binance")

    def _fetch_history_binance(self, limit: int) -> list[Candle]:
        collected: list[list[object]] = []
        remaining = max(1, limit)
        end_time: int | None = None

        while remaining > 0:
            chunk_limit = min(remaining, 1000)
            params: dict[str, object] = {
                "symbol": settings.symbol,
                "interval": "1h",
                "limit": chunk_limit,
            }
            if end_time is not None:
                params["endTime"] = end_time

            response = self._client.get("https://api.binance.com/api/v3/klines", params=params)
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break

            collected = rows + collected
            remaining -= len(rows)

            if len(rows) < chunk_limit:
                break

            oldest_open_time = int(rows[0][0])
            end_time = oldest_open_time - 1

        unique_rows: dict[int, list[object]] = {}
        for row in collected:
            unique_rows[int(row[0])] = row

        candles = []
        for row in [unique_rows[key] for key in sorted(unique_rows)[-limit:]]:
            candles.append(
                Candle(
                    timestamp=datetime.fromtimestamp(row[0] / 1000, UTC),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                )
            )
        return candles

    def _fetch_latest_coingecko(self) -> PricePoint:
        response = self._client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": settings.vs_currency},
        )
        response.raise_for_status()
        payload = response.json()
        return PricePoint(timestamp=datetime.now(UTC), price=float(payload["bitcoin"][settings.vs_currency]), source="coingecko")

    def _fetch_history_coingecko(self, limit: int) -> list[Candle]:
        days = max(2, min(90, int(limit / 24) + 2))
        response = self._client.get(
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
            params={"vs_currency": settings.vs_currency, "days": days, "interval": "hourly"},
        )
        response.raise_for_status()
        payload = response.json()
        prices = payload.get("prices", [])
        volumes = payload.get("total_volumes", [])
        volume_map = {int(ts): float(volume) for ts, volume in volumes}
        candles: list[Candle] = []
        sliced = prices[-limit:]
        for index, (ts, price) in enumerate(sliced):
            next_index = min(index + 1, len(sliced) - 1)
            next_price = float(sliced[next_index][1])
            candles.append(
                Candle(
                    timestamp=datetime.fromtimestamp(ts / 1000, UTC),
                    open=float(price),
                    high=max(float(price), next_price),
                    low=min(float(price), next_price),
                    close=next_price,
                    volume=volume_map.get(int(ts), 0.0),
                )
            )
        return candles


market_data_service = MarketDataService()
