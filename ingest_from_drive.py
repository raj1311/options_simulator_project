#!/usr/bin/env python3
"""
Download spot (100MB) and FO (huge) from Google Drive and build a local Parquet or DuckDB store.
Examples:
  python scripts/ingest_from_drive.py \
    --spot <spot_file_id_or_link> \
    --fo <fo_file_id_or_link> \
    --parquet /data/fo_parquet \
    --spot-out /data/spot.csv

  python scripts/ingest_from_drive.py \
    --fo <fo_file_id_or_link> \
    --duckdb /data/fo_store.duckdb
"""
import argparse, os, pathlib, subprocess, sys
import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import os
from utils.gdrive import download_file
from utils.drive_uploader import get_drive, upload_folder_recursive

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot", help="Drive link or ID for Strike/Spot CSV (≈100MB)")
    ap.add_argument("--fo", help="Drive link or ID for giant FO CSV (15GB+)")
    ap.add_argument("--spot-out", default="data/spot.csv", help="Where to save the spot CSV locally")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--parquet", help="Output Parquet directory")
    group.add_argument("--duckdb", help="Output DuckDB db file")
    ap.add_argument("--upload-to-drive", action="store_true", help="If set, upload the built Parquet/DuckDB to Google Drive")
    ap.add_argument("--drive-folder-id", help="Google Drive folder ID to upload into (recommended). If omitted, uploads to My Drive root.")
    args = ap.parse_args()

    os.makedirs("data", exist_ok=True)

    if args.spot:
        print("Downloading Spot/Strike CSV from Drive...")
        download_file(args.spot, args.spot_out)
        print("Spot saved to:", args.spot_out)

    if not args.fo:
        print("No FO link provided; done.")
        return

    # Large FO file
    fo_tmp = "data/fo_big.csv"
    print("Downloading FO CSV from Drive (this can take a while)...")
    download_file(args.fo, fo_tmp)
    print("FO CSV saved to:", fo_tmp)

    # Preprocess to Parquet or DuckDB
    cmd = [sys.executable, "scripts/preprocess_fno.py", "--csv", fo_tmp]
    if args.parquet:
        cmd += ["--out", args.parquet]
    else:
        cmd += ["--duckdb", args.duckdb]
    print("Running:", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        sys.exit(rc)
    print("Ingestion completed.")
    if args.upload_to_drive:
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sa_json:
            print("GOOGLE_SERVICE_ACCOUNT_JSON is not set; cannot upload to Drive.", flush=True)
        else:
            drive = get_drive(sa_json)
            if args.parquet:
                folder = args.parquet
            else:
                folder = os.path.dirname(args.duckdb) or "."
            print(f"Uploading {folder} to Drive...", flush=True)
            upload_folder_recursive(drive, folder, args.drive_folder_id or "root")
            print("Upload to Drive completed.", flush=True)

if __name__ == "__main__":
    main()
