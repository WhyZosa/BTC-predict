import os
import pandas as pd
from src.common.config import get_settings


def load_df_last_n(n: int = 300) -> pd.DataFrame:
    s = get_settings()
    if not os.path.exists(s.data_raw_path):
        raise RuntimeError("Нет файла с данными. Сначала запусти download_ohlcv и fix_gaps.")
    df = pd.read_parquet(s.data_raw_path)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").tail(n).reset_index(drop=True)
    return df


def get_last_candle():
    df = load_df_last_n(1)
    return df.iloc[-1].to_dict()
