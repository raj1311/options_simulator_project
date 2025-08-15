#!/usr/bin/env python3
"""
Preprocess big F&O CSV (≈15GB) into partitioned Parquet or a DuckDB database for fast queries.

Examples:
  python scripts/preprocess_fno.py --csv /data/NIFTY50_FNO_2010_2025.csv --out parquet_dir
  python scripts/preprocess_fno.py --csv /data/NIFTY50_FNO_2010_2025.csv --duckdb fo_store.duckdb
"""

import argparse, os, duckdb, pathlib

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to the giant F&O CSV (can be multiple via glob, e.g., '/data/FNO_*.csv')")
    ap.add_argument("--out", help="Directory to write Parquet partitions (recommended)")
    ap.add_argument("--duckdb", help="Write a DuckDB database file instead of Parquet")
    args = ap.parse_args()

    if not args.out and not args.duckdb:
        ap.error("Specify --out (parquet dir) or --duckdb (database file)")

    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(database=":memory:")

    # Create a relation scanning the CSV. AUTO_DETECT reads headers & types, SAMPLE_SIZE=-1 for full inspection.
    # Adjust DATE/Timestamp formats if needed.
    rel = con.sql(f"""
        SELECT
            *,
            COALESCE(Timestamp, TIMESTAMP) AS TS
        FROM read_csv_auto('{args.csv}', SAMPLE_SIZE=-1, AUTO_DETECT=TRUE)
    """)

    # Normalize expected column names
    con.sql("""
        CREATE OR REPLACE TABLE fo AS
        SELECT
            UPPER(COALESCE(SYMBOL, symbol)) AS SYMBOL,
            UPPER(COALESCE(INSTRUMENT, instrument)) AS INSTRUMENT,
            TRY_CAST(COALESCE(EXPIRY_DT, EXPIRY, "EXPIRY DATE") AS DATE) AS EXPIRY_DT,
            TRY_CAST(COALESCE(STRIKE_PR, STRIKE, "STRIKE PRICE") AS DOUBLE) AS STRIKE_PR,
            UPPER(COALESCE(OPTION_TYP, OPTION_TYPE)) AS OPTION_TYP,
            TRY_CAST(COALESCE(OPEN, OPEN_PRICE) AS DOUBLE) AS OPEN,
            TRY_CAST(COALESCE(HIGH, HIGH_PRICE) AS DOUBLE) AS HIGH,
            TRY_CAST(COALESCE(LOW, LOW_PRICE) AS DOUBLE) AS LOW,
            TRY_CAST(COALESCE(CLOSE, CLOSE_PRICE) AS DOUBLE) AS CLOSE,
            TRY_CAST(COALESCE(SETTLE_PR, SETTLEMENT_PRICE) AS DOUBLE) AS SETTLE_PR,
            TRY_CAST(COALESCE(OPEN_INT, OI, "OPEN INTEREST") AS BIGINT) AS OPEN_INT,
            TRY_CAST(COALESCE("CHG_IN_OI", CHG_OI, "CHANGE IN OI") AS BIGINT) AS CHG_IN_OI,
            TRY_CAST(COALESCE(TS, DATE, "TIMESTAMP", "TIMESTAMP_1") AS TIMESTAMP) AS Timestamp,
            *
        FROM rel
    """)

    if args.out:
        out_dir = pathlib.Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        # Partition by year and symbol for fast prunes
        con.sql(f"""
            COPY (
                SELECT *, YEAR(Timestamp) AS year
                FROM fo
                WHERE Timestamp IS NOT NULL
            )
            TO '{str(out_dir)}'
            (FORMAT PARQUET, PARTITION_BY (year, SYMBOL), OVERWRITE_OR_IGNORE TRUE);
        """)
        print(f"Parquet written under: {out_dir} (partitioned by year/SYMBOL)")

    if args.duckdb:
        db_path = pathlib.Path(args.duckdb)
        con2 = duckdb.connect(database=str(db_path))
        con2.execute("CREATE TABLE IF NOT EXISTS fo AS SELECT * FROM fo;")
        con2.close()
        print(f"DuckDB database stored at: {db_path}")

if __name__ == "__main__":
    main()
