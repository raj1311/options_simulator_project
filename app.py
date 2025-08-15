import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
from data_loader import load_spot_csv, load_fo_csv
import duckdb, os, glob
from lot_size import resolve_lot_size

st.set_page_config(page_title="Options Simulator", layout="wide")

# Load Drive service account JSON from secrets (Streamlit Cloud) into env for scripts
try:
    sa_json = st.secrets["drive"]["service_account_json"]
    import os as _os
    _os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = sa_json
    default_drive_folder = st.secrets["drive"].get("folder_id", "")
except Exception:
    default_drive_folder = ""


# --- Sidebar: data sources ---
st.sidebar.header("Data Sources")

with st.sidebar.expander("Google Drive ingest", expanded=False):
    st.caption("Paste Google Drive **share links or file IDs** to auto-download and build Parquet/DuckDB.")
    spot_link = st.text_input("Spot CSV (Drive link or ID)", value="")
    fo_link = st.text_input("FO CSV (Drive link or ID)", value="")
    target_parquet = st.text_input("Target Parquet dir (if building Parquet)", value="")
    upload_to_drive = st.checkbox("Upload to Drive after build", value=True)
    drive_folder_id = st.text_input("Drive folder ID (optional, else My Drive root)", value=default_drive_folder)
    target_duckdb = st.text_input("Target DuckDB file (if building DuckDB)", value="")
    if st.button("Ingest from Drive"):
        import subprocess, sys
        from pathlib import Path
root_dir = Path(__file__).resolve().parent
cmd = [sys.executable, "-m", "scripts.ingest_from_drive"]

        if spot_link: cmd += ["--spot", spot_link]
        if fo_link: cmd += ["--fo", fo_link]
        if target_parquet:
            cmd += ["--parquet", target_parquet]
        elif target_duckdb:
            cmd += ["--duckdb", target_duckdb]
        else:
            st.error("Specify Parquet dir OR DuckDB file.")
            st.stop()
        st.info("Starting ingestion... this may take a long time for 15GB + network latency.")
        if upload_to_drive:
            cmd += ["--upload-to-drive"]
            if drive_folder_id:
                cmd += ["--drive-folder-id", drive_folder_id]
        code = subprocess.call(cmd, cwd=str(root_dir))
        if code == 0:
            st.success("Ingestion completed. Fill paths above and reload the app.")
        else:
            st.error(f"Ingestion failed with code {code}. Check logs.")

mode = st.sidebar.radio('Data Mode', ['CSV Upload/Path','Parquet/DuckDB'], index=0)
spot_file = st.sidebar.file_uploader("Spot/Strike CSV", type=["csv"], key="spot")
fo_file = st.sidebar.file_uploader("F&O CSV (options+futures)", type=["csv"], key="fo")

default_spot_path = st.sidebar.text_input("Or path to Spot CSV", value="")
default_fo_path = st.sidebar.text_input("Or path to F&O CSV", value="")
parquet_dir = st.sidebar.text_input("Parquet directory (if using Parquet/DuckDB mode)", value="")
duckdb_file = st.sidebar.text_input("DuckDB file (optional)", value="")

@st.cache_data(show_spinner=False)
def load_all(_spot_bytes, _fo_bytes, _spot_path, _fo_path):
    spot_df = None; fo_df = None
    if _spot_bytes is not None:
        spot_df = load_spot_csv(_spot_bytes)
    elif _spot_path:
        spot_df = load_spot_csv(_spot_path)
    if _fo_bytes is not None:
        fo_df = load_fo_csv(_fo_bytes)
    elif _fo_path:
        fo_df = load_fo_csv(_fo_path)
    return spot_df, fo_df

if mode == 'Parquet/DuckDB' and (duckdb_file or parquet_dir):
    spot_df = load_spot_csv(spot_file or default_spot_path)
    # Delay-load FO; we won't materialize entire 15GB—later we query with filters.
    fo_df = None
else:
    spot_df, fo_df = load_all(spot_file, fo_file, default_spot_path, default_fo_path)

colA, colB, colC = st.columns([1,2,2])
with colA:
    st.markdown("### Options Simulator")

if spot_df is None or fo_df is None:
    st.info("Upload or point to the two CSVs in the sidebar to begin.")
    st.stop()

symbols = sorted(list(spot_df['Ticker'].dropna().unique()))
symbol = st.selectbox("Select Index/Stock", options=symbols, index=0)

# date range
min_dt = spot_df['Datetime'].min().date()
max_dt = spot_df['Datetime'].max().date()
d1, d2 = st.columns(2)
with d1:
    start_date = st.date_input("Start Date", value=min_dt, min_value=min_dt, max_value=max_dt)
with d2:
    end_date = st.date_input("Payoff Date", value=max_dt, min_value=min_dt, max_value=max_dt)

# Expiry selection from FO
if mode == 'Parquet/DuckDB' and (duckdb_file or parquet_dir):
    con = duckdb.connect(database=duckdb_file if duckdb_file else ':memory:')
    if not duckdb_file:
        con.sql(f"CREATE OR REPLACE VIEW fo AS SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet')")
    expiries = con.sql(f"SELECT DISTINCT DATE(EXPIRY_DT) AS e FROM fo WHERE SYMBOL = '{symbol}' ORDER BY e").df()['e'].dt.date.tolist()
else:
    expiries = fo_df.loc[fo_df['SYMBOL'].eq(symbol), 'EXPIRY_DT'].dropna().dt.date.unique()
expiries = sorted(list(set(expiries)))
expiry = st.selectbox("Select Expiry", options=expiries, index=0 if expiries else None)

# Timeframe + speed
tf = st.select_slider("Timeframe", options=["1 MIN","5 MIN","15 MIN","30 MIN","1 DAY"], value="5 MIN")
speed = st.select_slider("Speed", options=["1x","2x","4x"], value="1x")

# Slice data
mask_spot = spot_df['Ticker'].eq(symbol) & spot_df['Datetime'].dt.date.between(start_date, end_date)
spot_slice = spot_df.loc[mask_spot].copy()

# Latest values
def latest_price_at(ts: pd.Timestamp) -> float:
    x = spot_slice.loc[spot_slice['Datetime']<=ts]
    return float(x['Close'].iloc[-1]) if len(x) else np.nan

# UI header tiles
hdr1, hdr2, hdr3, hdr4 = st.columns([1,1,1,1])
now_ts = spot_slice['Datetime'].min()
if pd.isna(now_ts):
    st.warning("No spot data in selected window.")
    st.stop()

# resolve futures price near ts from FO (FUTIDX/FUTSTK rows)
def futures_price_at(ts: pd.Timestamp) -> float:
    if mode == 'Parquet/DuckDB' and (duckdb_file or parquet_dir):
        con = duckdb.connect(database=duckdb_file if duckdb_file else ':memory:')
        if not duckdb_file:
            con.sql(f"CREATE OR REPLACE VIEW fo AS SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet')")
        q = f"""
            SELECT CLOSE FROM fo
            WHERE SYMBOL = '{symbol}' AND INSTRUMENT ILIKE 'FUT%'
              AND Timestamp <= TIMESTAMP '{ts}'
            ORDER BY Timestamp DESC
            LIMIT 1
        """
        r = con.sql(q).df()
        return float(r['CLOSE'].iloc[0]) if len(r) else float('nan')
    else:
        r = fo_df[(fo_df['SYMBOL'].eq(symbol)) & (fo_df['INSTRUMENT'].str.contains('FUT', na=False)) & (fo_df['Timestamp']<=ts)]
        return float(r['CLOSE'].iloc[-1]) if len(r) else np.nan

lot_override = st.sidebar.number_input("Lot size override", min_value=1, value=0, help="Leave 0 to auto-resolve")
if mode == 'Parquet/DuckDB' and (duckdb_file or parquet_dir):
    lot = resolve_lot_size(symbol, now_ts.to_pydatetime(), fo_slice=None, override=lot_override if lot_override>0 else None)
else:
    lot = resolve_lot_size(symbol, now_ts.to_pydatetime(), fo_slice=fo_df[fo_df['SYMBOL'].eq(symbol)], override=lot_override if lot_override>0 else None)

with hdr1:
    st.metric("Spot Price", f"{latest_price_at(now_ts):,.2f}")
with hdr2:
    st.metric("Futures Price", f"{futures_price_at(now_ts):,.2f}")
with hdr3:
    st.metric("Lot Size", f"{lot}")
with hdr4:
    st.metric("IV", "--")

st.divider()

# --- Playback controls ---
play = st.checkbox("Play")
step_map = {"1 MIN": timedelta(minutes=1), "5 MIN": timedelta(minutes=5), "15 MIN": timedelta(minutes=15),
            "30 MIN": timedelta(minutes=30), "1 DAY": timedelta(days=1)}
step = step_map[tf]
speed_map = {"1x": 0.0, "2x": 0.0, "4x": 0.0}  # Streamlit can't truly animate without sleep; we step via button

# state for current ts
if "cursor" not in st.session_state:
    st.session_state["cursor"] = now_ts
cur = st.session_state["cursor"]

btns = st.columns([1,1,1,1,1,6])
with btns[0]:
    if st.button("<< 30 MIN"):
        st.session_state["cursor"] = cur - timedelta(minutes=30)
with btns[1]:
    if st.button("<< 5 MIN"):
        st.session_state["cursor"] = cur - timedelta(minutes=5)
with btns[2]:
    if st.button("1 MIN >>"):
        st.session_state["cursor"] = cur + timedelta(minutes=1)
with btns[3]:
    if st.button("5 MIN >>"):
        st.session_state["cursor"] = cur + timedelta(minutes=5)
with btns[4]:
    if st.button("1 DAY >>"):
        st.session_state["cursor"] = cur + timedelta(days=1)

cur = st.session_state["cursor"]
st.caption(f"{cur:%a %d-%b-%Y %H:%M}")

# Recompute tiles at cursor
with hdr1:
    st.metric("Spot Price", f"{latest_price_at(cur):,.2f}")
with hdr2:
    st.metric("Futures Price", f"{futures_price_at(cur):,.2f}")
with hdr3:
    st.metric("Lot Size", f"{lot}")
with hdr4:
    st.metric("IV", "--")

st.divider()

# --- Simplified trading blotter ---
st.subheader("Futures Paper Trades")
qty = st.number_input("Qty (in lots)", min_value=1, value=1)
side = st.radio("Side", ["BUY","SELL"], horizontal=True)

if "trades" not in st.session_state:
    st.session_state["trades"] = []

if st.button("Place Trade on Futures"):
    px = futures_price_at(cur)
    if np.isnan(px):
        st.error("No futures price at current time.")
    else:
        st.session_state["trades"].append({"ts": cur, "side": side, "qty": int(qty), "px": float(px), "lot": lot})

trades = pd.DataFrame(st.session_state["trades"])
if len(trades):
    # Mark-to-market PnL vs latest futures price
    last_px = futures_price_at(cur)
    def pnl_row(r):
        mult = r["qty"]*r["lot"]
        sign = 1 if r["side"]=="BUY" else -1
        return (last_px - r["px"]) * mult * sign
    trades["UPnL"] = trades.apply(pnl_row, axis=1)
    st.dataframe(trades)
    st.metric("Unrealized P&L", f"{trades['UPnL'].sum():,.0f}")
else:
    st.info("No trades yet.")
