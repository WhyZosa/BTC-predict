from __future__ import annotations

import pandas as pd
import numpy as np


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / (avg_loss + 1e-12)
    return 100 - (100 / (1 + rs))


def macd(close: pd.Series):
    fast = ema(close, 12)
    slow = ema(close, 26)
    macd_line = fast - slow
    signal = ema(macd_line, 9)
    hist = macd_line - signal
    return macd_line, signal, hist


def calc_indicators(df: pd.DataFrame) -> dict:
    close = df["close"].astype(float)

    rsi_series = rsi(close, 14)
    macd_line, signal, hist = macd(close)

    rsi_last = float(rsi_series.iloc[-1])
    macd_last = float(macd_line.iloc[-1])
    signal_last = float(signal.iloc[-1])
    hist_last = float(hist.iloc[-1])

    # интерпретация RSI
    if rsi_last >= 70:
        rsi_text = "RSI > 70: возможна перекупленность (осторожно, может быть откат)."
    elif rsi_last <= 30:
        rsi_text = "RSI < 30: возможна перепроданность (возможен отскок)."
    else:
        rsi_text = "RSI в нейтральной зоне."

    # интерпретация MACD
    if macd_last > signal_last and hist_last > 0:
        macd_text = "MACD выше сигнальной: импульс скорее бычий."
    elif macd_last < signal_last and hist_last < 0:
        macd_text = "MACD ниже сигнальной: импульс скорее медвежий."
    else:
        macd_text = "MACD без явного сигнала."

    return {
        "rsi": rsi_last,
        "macd": macd_last,
        "signal": signal_last,
        "hist": hist_last,
        "rsi_text": rsi_text,
        "macd_text": macd_text,
    }
