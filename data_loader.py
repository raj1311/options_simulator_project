from __future__ import annotations
import pandas as pd
from typing import Tuple, Optional, List

def parse_datetime(df: pd.DataFrame, col_candidates: List[str]) -> pd.DataFrame:
    for c in col_candidates:
        if c in df.columns:
            # try a few formats
            for fmt in [None, "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M"]:
                try:
                    df["Datetime"] = pd.to_datetime(df[c], format=fmt, errors="raise")
                    return df
                except Exception:
                    pass
            # final attempt
            df["Datetime"] = pd.to_datetime(df[c], errors="coerce")
            return df
    raise ValueError("No datetime-like column found. Tried: " + ", ".join(col_candidates))

def load_spot_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize columns
    rename_map = {c:c.strip() for c in df.columns}
    df = df.rename(columns=rename_map)
    if "Ticker" not in df.columns:
        # try SYMBOL
        if "SYMBOL" in df.columns:
            df = df.rename(columns={"SYMBOL":"Ticker"})
        else:
            df["Ticker"] = df.get("ticker", "NIFTY")
    df = parse_datetime(df, ["Datetime","TIMESTAMP","Timestamp","DATE","Date"])
    # normalize OHLC
    for a,b in [("Open","OPEN"),("High","HIGH"),("Low","LOW"),("Close","CLOSE"),("Close","CLOSE_PRICE")]:
        if a not in df.columns and b in df.columns:
            df[a] = df[b]
    keep = ["Ticker","Datetime","Open","High","Low","Close"]
    return df[keep].sort_values("Datetime").reset_index(drop=True)

def load_fo_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={c:c.strip() for c in df.columns})
    # Normalize column names we rely on
    if "Timestamp" not in df.columns:
        for c in ["TIMESTAMP","Date","DATE"]:
            if c in df.columns:
                df = df.rename(columns={c:"Timestamp"}); break
    if "CLOSE" not in df.columns:
        for c in ["CLOSE_PRICE","Close"]:
            if c in df.columns:
                df = df.rename(columns={c:"CLOSE"}); break
    if "EXPIRY_DT" not in df.columns:
        for c in ["EXPIRY","Expiry","EXPIRY DATE"]:
            if c in df.columns:
                df = df.rename(columns={c:"EXPIRY_DT"}); break
    # Type normalize
    for c in ["STRIKE_PR"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["EXPIRY_DT"] = pd.to_datetime(df["EXPIRY_DT"], errors="coerce")
    return df
