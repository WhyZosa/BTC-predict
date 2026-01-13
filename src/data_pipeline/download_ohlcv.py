from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import ccxt
import pandas as pd
from loguru import logger

from src.common.config import get_settings
from src.common.logging import setup_logger


def _utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _make_exchange(name: str) -> ccxt.Exchange:
    ex_cls = getattr(ccxt, name, None)
    if ex_cls is None:
        raise RuntimeError(f"❌ Неизвестная биржа: {name}")

    ex = ex_cls({"enableRateLimit": True})
    ex.load_markets()
    return ex


def download_incremental(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    out_path: str,
    since: str | None = None,
    max_batches: int = 10000,
) -> pd.DataFrame:
    """
    Скачивает свечи OHLCV и сохраняет в parquet.
    Если файл уже есть — докачивает с последней свечи.
    since: 'YYYY-MM-DD' (UTC), используется только если файла ещё нет.
    """
    _ensure_parent(out_path)
    ex = _make_exchange(exchange_name)

    df_existing = pd.DataFrame()

    if os.path.exists(out_path):
        df_existing = pd.read_parquet(out_path)
        if not df_existing.empty:
            df_existing["timestamp_utc"] = pd.to_datetime(df_existing["timestamp_utc"], utc=True)
            last_dt = df_existing["timestamp_utc"].max()
            since_ms = int(last_dt.timestamp() * 1000) + 1
            logger.info(f"Продолжаю докачку с: {last_dt} (UTC)\n")
        else:
            since_ms = _utc_ms(datetime(2017, 1, 1, tzinfo=timezone.utc))
    else:
        if since:
            since_ms = _utc_ms(datetime.fromisoformat(since).replace(tzinfo=timezone.utc))
        else:
            since_ms = _utc_ms(datetime(2017, 1, 1, tzinfo=timezone.utc))

        logger.info(f"Файла нет — начинаю скачивание с: {datetime.fromtimestamp(since_ms/1000, tz=timezone.utc)}\n")

    all_rows = []
    batch = 0
    rate_ms = int(getattr(ex, "rateLimit", 1000) or 1000)

    while batch < max_batches:
        batch += 1
        try:
            ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=1000)
        except Exception as e:
            logger.error(f"❌ Ошибка при запросе свечей: {e}\n")
            break

        if not ohlcv:
            break

        df = pd.DataFrame(ohlcv, columns=["ts_ms", "open", "high", "low", "close", "volume"])
        df["timestamp_utc"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df = df.drop(columns=["ts_ms"])

        all_rows.append(df)

        max_ts = int(df["timestamp_utc"].max().timestamp() * 1000)
        since_ms = max_ts + 1

        # пауза по лимитам биржи
        time.sleep(rate_ms / 1000.0)

        # если партия меньше 1000 — скорее всего дошли до конца
        if len(df) < 1000:
            break

        if batch % 10 == 0:
            logger.info(f"Скачано партий: {batch}, последняя свеча: {df['timestamp_utc'].max()}\n")

    if all_rows:
        df_new = pd.concat(all_rows, ignore_index=True)
    else:
        df_new = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "timestamp_utc"])

    if not df_existing.empty:
        df_all = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_all = df_new

    if not df_all.empty:
        df_all["timestamp_utc"] = pd.to_datetime(df_all["timestamp_utc"], utc=True)
        df_all = (
            df_all.drop_duplicates(subset=["timestamp_utc"])
            .sort_values("timestamp_utc")
            .reset_index(drop=True)
        )

    df_all.to_parquet(out_path, index=False)

    logger.info(f"✅ Сохранено: {out_path}\n")
    logger.info(f"Строк: {len(df_all)}\n")
    logger.info(f"Период: {df_all['timestamp_utc'].min()}  →  {df_all['timestamp_utc'].max()}\n")
    return df_all


def main():
    setup_logger()
    s = get_settings()
    download_incremental(
        exchange_name=s.exchange,
        symbol=s.symbol,
        timeframe=s.timeframe,
        out_path=s.data_raw_path,
        since=None,
    )


if __name__ == "__main__":
    main()
