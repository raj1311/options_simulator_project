from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict, Tuple
import pandas as pd
import math

# Best-effort historical lot-size map (indices). These are approximate ranges.
# You can extend these in the UI or by editing this file.
# Format: (start_date_inclusive, end_date_exclusive, lot)
HISTORICAL_LOTS = {
    "NIFTY": [
        # NOTE: These values are *approximate*. Feel free to refine.
        ("2007-01-01","2014-10-31", 50),
        ("2014-11-01","2015-10-31", 25),
        ("2015-11-01","2020-02-28", 75),
        ("2020-03-01","2023-09-27", 50),
        ("2023-09-28","2100-01-01", 25),
    ],
    "BANKNIFTY": [
        ("2007-01-01","2014-10-31", 25),
        ("2014-11-01","2019-06-30", 30),
        ("2019-07-01","2023-09-03", 20),
        ("2023-09-04","2100-01-01", 15),
    ],
    "FINNIFTY": [
        ("2021-01-01","2023-09-28", 40),
        ("2023-09-29","2100-01-01", 50),
    ],
}

def _date_in_range(d: datetime, start: str, end: str) -> bool:
    return pd.Timestamp(start) <= pd.Timestamp(d) < pd.Timestamp(end)

def from_mapping(symbol: str, trade_date: datetime) -> Optional[int]:
    sym = symbol.upper()
    if sym in HISTORICAL_LOTS:
        for start, end, lot in HISTORICAL_LOTS[sym]:
            if _date_in_range(trade_date, start, end):
                return int(lot)
    return None

def infer_from_fo_row(row: pd.Series) -> Optional[int]:
    # If turnover-like fields exist, try: lot ≈ (VALUE / (price * contracts))
    # Works only if those columns are present and clean.
    candidates = ["VAL_INLAKH","VAL_IN_LAKH","VAL_INCR","VALUE_IN_LAKH","VAL"]
    contracts_cols = ["CONTRACTS","NO_OF_CONTRACTS"]
    price_cols = ["CLOSE","SETTLE_PR","CLOSE_PRICE"]
    val = None
    for c in candidates:
        if c in row and pd.notna(row[c]) and row[c] > 0:
            val = float(row[c]) * 100000.0   # lakhs -> rupees approx
            break
    if val is None:
        return None
    contracts = None
    for c in contracts_cols:
        if c in row and pd.notna(row[c]) and row[c] > 0:
            contracts = float(row[c]); break
    if contracts is None or contracts <= 0:
        return None
    price = None
    for c in price_cols:
        if c in row and pd.notna(row[c]) and row[c] > 0:
            price = float(row[c]); break
    if price is None or price <= 0:
        return None
    est = val / (price * contracts)
    if est > 1 and est < 50000:
        # round to nearest common lot multiples
        return int(round(est / 5.0)*5)
    return None

def resolve_lot_size(symbol: str, trade_date: datetime, fo_slice: Optional[pd.DataFrame]=None, override: Optional[int]=None) -> int:
    if override and override > 0:
        return int(override)
    # 1) mapping
    m = from_mapping(symbol, trade_date)
    if m:
        return int(m)
    # 2) try infer from fo data slice (same date & symbol)
    if fo_slice is not None and len(fo_slice):
        # pick a near-the-money option or a future
        row = fo_slice.iloc[0]
        inf = infer_from_fo_row(row)
        if inf:
            return int(inf)
    # 3) default
    return 50  # sensible default for indices historically
