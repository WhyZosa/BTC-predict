from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import mplfinance as mpf


def make_candles_chart(df: pd.DataFrame, out_path: str) -> str:
    """
    Рисует свечи и сохраняет картинку.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    tmp = df.copy()
    tmp = tmp.rename(columns={
        "timestamp_utc": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })
    tmp["Date"] = pd.to_datetime(tmp["Date"], utc=True)
    tmp = tmp.set_index("Date")

    title = f"BTC/USDT — последние {len(tmp)} свечей (1h)"
    mpf.plot(
        tmp,
        type="candle",
        volume=True,
        title=title,
        style="yahoo",
        savefig=dict(fname=out_path, dpi=140, bbox_inches="tight"),
    )
    return out_path
