# Options Simulator (Streamlit)

A lightweight, local options simulator inspired by Opstra-style playback.
Works with your historical **spot/strike-price** CSV and **F&O options/futures** CSVs.

## Features
- Upload or point to two CSVs:
  1) Spot/Strike (minute or day) with columns like: `Ticker,Datetime,Open,High,Low,Close`
  2) F&O file (bhavcopy-joined style) covering OPTIDX/OPTSTK/FUTIDX/FUTSTK with columns such as:
     `INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,OPEN_INT,CHG_IN_OI,Timestamp`
- Time scrubber with play speeds (1m/5m/15m/30m/1d)
- Shows Spot, Futures, Lot Size (auto), and simple IV (if provided) or greeks via Black–Scholes (if you enable it)
- Paper buy/sell of futures or options with running P&L
- Auto Lot-Size resolver using multiple strategies:
  - Historical mapping for common indices (NIFTY, BANKNIFTY, FINNIFTY) with date ranges (best-effort)
  - Inference from FO file if turnover-related columns exist
  - Manual override in the UI

## Quickstart (Local)
```bash
python -m venv .venv && source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```
Then upload your two CSV files from your Drive OR set their paths in the sidebar.

## Docker
```bash
docker build -t options-sim .
docker run -p 8501:8501 -v $PWD/data:/app/data options-sim
```
Put your CSVs in `./data` and choose them in the UI.

## CSV Expectations

### Spot/Strike file
- Columns: `Ticker,Datetime,Open,High,Low,Close`
- Datetime can be `%Y-%m-%d %H:%M:%S` or `%d/%m/%Y %H:%M:%S` or date-only.
- Example row:
```
NIFTY,2016-08-01 09:17:00,8691.55,8695.45,8689.6,8689.6
```

### F&O file
- Prefer NSE-style merged bhavcopy fields. Minimum useful fields:
  `INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,OPEN_INT,Timestamp`
- Datetime column may be named `Timestamp` or `DATE`.

## Notes on Lot Size
Lot size changed over the years. We do a best-effort guess. You can override anytime from the sidebar.

---

## Handling 15GB+ F&O data (fast)

For very large FO CSVs, convert once to **Parquet (partitioned)** or a **DuckDB** DB:

```bash
# Option 1: Parquet partitioned by year and symbol
python scripts/preprocess_fno.py --csv "/path/to/FNO_*.csv" --out /path/to/fo_parquet

# Option 2: Single DuckDB file
python scripts/preprocess_fno.py --csv "/path/to/FNO_*.csv" --duckdb /path/to/fo_store.duckdb
```

Then, in the app sidebar:
- Choose **Data Mode = Parquet/DuckDB**
- Enter the **Parquet directory** (e.g., `/path/to/fo_parquet`) or **DuckDB file** (e.g., `/path/to/fo_store.duckdb`).

The app will query only the subset needed for your selected **symbol**, **date window**, and **expiry**, making it snappy even on huge datasets.


## Pulling data directly from Google Drive

You can paste **Google Drive share links or file IDs** and let the app download + build the database for you.

### In the UI
- Open the **Google Drive ingest** expander in the sidebar.
- Paste your Spot CSV link/ID and FO CSV link/ID.
- Choose either **Parquet directory** or a **DuckDB file** target.
- Click **Ingest from Drive** (this can take a while for 15GB).

### From the CLI
```bash
# Parquet
python scripts/ingest_from_drive.py --spot "<spot_link_or_id>" --fo "<fo_link_or_id>" --parquet "/data/fo_parquet" --spot-out /data/spot.csv

# DuckDB
python scripts/ingest_from_drive.py --fo "<fo_link_or_id>" --duckdb "/data/fo_store.duckdb"
```


## Streamlit Community Cloud (persistent data to Google Drive)

1. **Prepare Google Cloud** (one-time):
   - Create a Google Cloud project → enable **Google Drive API**.
   - Create a **Service Account**. Download its JSON key.
   - In Google Drive, create or pick a destination folder. **Share** that folder with your service account’s email (Editor).
   - Copy the folder’s **ID** (the long string in the URL).

2. **Add Streamlit secrets** (on Streamlit Cloud → *App* → *Settings* → *Secrets*):
```toml
# .streamlit/secrets.toml (managed in the Cloud UI)
[drive]
service_account_json = 