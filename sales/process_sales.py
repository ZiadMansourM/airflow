#!/usr/bin/env python3
"""
process_sales.py
────────────────
• Cleans every CSV under s3://raw/
• Writes per-file cleaned + aggregated artefacts:
      processed/cleaned/<base>_cleaned.csv
      processed/aggregates/<base>_aggregated.csv
• INSIGHTS step pulls every *_aggregated.csv already in
      processed/aggregates/   →   computes KPIs, charts & JSON.
"""

import os, io, json, logging, datetime
import pandas as pd
import boto3

import matplotlib
matplotlib.use("Agg")           # headless backend
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

# ── CONFIG ──────────────────────────────────────────────────────────────
RAW_BUCKET        = "raw"
PROCESSED_BUCKET  = "processed"

CLEAN_PREFIX      = "cleaned/"             # processed/cleaned/…
CLEAN_SUFFIX      = "_cleaned.csv"

AGG_PREFIX        = "aggregates/"          # processed/aggregates/…
AGG_SUFFIX        = "_aggregated.csv"      # per-file aggregate files

INSIGHTS_PREFIX   = "insights/sales_insights_"   # + <UTC> suffix

REQUIRED_COLUMNS  = {"id", "name", "country", "amount"}
LOG_FILENAME      = "process_sales.log"

# S3 / MinIO creds (override via env)
S3_ENDPOINT_URL   = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
AWS_ACCESS_KEY    = os.getenv("MINIO_ROOT_USER",     "x62hAFEb4wkRNRaR")
AWS_SECRET_KEY    = os.getenv("MINIO_ROOT_PASSWORD", "IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW")

logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)

# ── CONNECT TO S3 / MINIO ───────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# ── HELPERS ─────────────────────────────────────────────────────────────
def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    raw_bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_csv(io.BytesIO(raw_bytes))

def write_string_to_s3(buf: str, bucket: str, key: str):
    s3.put_object(Bucket=bucket, Key=key, Body=buf)

def load_all_aggregates(prefix: str) -> pd.DataFrame:
    """Load every per-file *_aggregated.csv already in processed/aggregates/."""
    objs = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix=prefix).get("Contents", [])
    if not objs:
        return pd.DataFrame()
    frames = []
    for o in objs:
        raw = s3.get_object(Bucket=PROCESSED_BUCKET, Key=o["Key"])["Body"].read()
        frames.append(pd.read_csv(io.BytesIO(raw)))
    return pd.concat(frames, ignore_index=True)

# ── STEP 1 : LIST RAW FILES ─────────────────────────────────────────────
logging.info("Listing raw CSVs in s3://%s", RAW_BUCKET)
raw_objs = (s3.list_objects_v2(Bucket=RAW_BUCKET).get("Contents") or [])
raw_keys = [o["Key"] for o in raw_objs if o["Key"].lower().endswith(".csv")]

if not raw_keys:
    logging.warning("No CSV files found in raw bucket; nothing to do.")
    exit(0)

# ── STEP 2 : CLEAN & WRITE PER-FILE OUTPUTS ─────────────────────────────
for key in raw_keys:
    logging.info("Processing %s …", key)
    df = read_csv_from_s3(RAW_BUCKET, key)

    # schema validation
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        logging.error("❌  %s missing columns %s – skipped", key, missing)
        continue

    before = len(df)
    df = df.dropna(subset=["country", "amount"])
    df = df[df["amount"] >= 0]
    after  = len(df)
    logging.info("✔️  %s  %d → %d rows after cleaning", key, before, after)
    if after == 0:
        logging.warning("All rows dropped for %s – no clean file written", key)
        continue

    base      = os.path.basename(key)                          # e.g. sales_2025_05_01.csv
    clean_key = f"{CLEAN_PREFIX}{base.replace('.csv', CLEAN_SUFFIX)}"
    write_string_to_s3(df.to_csv(index=False), PROCESSED_BUCKET, clean_key)
    logging.info("   ↳ cleaned     →  s3://%s/%s", PROCESSED_BUCKET, clean_key)

    # per-file aggregation
    agg_df = (
        df.groupby("country", as_index=False)
          .agg(total_amount=("amount", "sum"),
               record_count=("amount", "count"))
          .sort_values("total_amount", ascending=False)
    )
    agg_key = f"{AGG_PREFIX}{base.replace('.csv', AGG_SUFFIX)}"
    write_string_to_s3(agg_df.to_csv(index=False), PROCESSED_BUCKET, agg_key)
    logging.info("   ↳ aggregated  →  s3://%s/%s", PROCESSED_BUCKET, agg_key)

# ── STEP 3 : LOAD **ALL** PER-FILE AGGREGATES FOR INSIGHTS ──────────────
agg_history = load_all_aggregates(f"{AGG_PREFIX}sales_")
if agg_history.empty:
    logging.error("No per-file aggregates found – insights aborted.")
    exit(1)

global_agg = (
    agg_history
      .groupby("country", as_index=False)
      .agg(total_amount=("total_amount", "sum"),
           record_count=("record_count", "sum"))
      .sort_values("total_amount", ascending=False)
)

# ── STEP 4 : BUSINESS INSIGHTS ─────────────────────────────────────────-
insights = {
    "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "kpis": {
        "total_revenue":      float(global_agg["total_amount"].sum()),
        "total_transactions": int(global_agg["record_count"].sum()),
        "unique_countries":   int(global_agg["country"].nunique()),
        "files_considered":   int(len(agg_history))  # optional sanity metric
    },
    "top_countries": (
        global_agg.head(5)
                  .assign(pct_total=lambda d:
                          (d.total_amount /
                           global_agg.total_amount.sum() * 100).round(2))
                  .to_dict(orient="records")
    )
}

# ── STEP 5 : CHARTS & KPI TABLE ────────────────────────────────────────
ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

try:
    # A. horizontal bar of top-5 countries
    fig, ax = plt.subplots(figsize=(8, 4))
    top5 = global_agg.head(5)
    ax.barh(top5["country"], top5["total_amount"])
    ax.invert_yaxis()
    ax.set_xlabel("Sales amount")
    ax.xaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    ax.set_title("Top 5 countries by total sales")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    chart_key = f"{INSIGHTS_PREFIX}{ts}.png"
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=chart_key, Body=buf.getvalue())
    logging.info("Chart written  s3://%s/%s", PROCESSED_BUCKET, chart_key)

    # B. KPI mini-table
    fig_tbl, ax_tbl = plt.subplots(figsize=(4, 1.6))
    ax_tbl.axis("off")
    kpi_row = [
        f"${insights['kpis']['total_revenue']:,.0f}",
        f"{insights['kpis']['total_transactions']:,}",
        f"{insights['kpis']['unique_countries']:,}",
    ]
    tbl = ax_tbl.table(
        cellText=[kpi_row],
        colLabels=["Revenue", "Txns", "Countries"],
        loc="center"
    )
    tbl.scale(1, 2)
    plt.tight_layout()

    buf_tbl = io.BytesIO()
    plt.savefig(buf_tbl, format="png", dpi=150)
    buf_tbl.seek(0)
    tbl_key = f"{INSIGHTS_PREFIX}{ts}_kpis.png"
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=tbl_key, Body=buf_tbl.getvalue())
    logging.info("KPI table written  s3://%s/%s", PROCESSED_BUCKET, tbl_key)

except Exception as e:
    logging.warning("Plot step failed: %s", e)

# ── STEP 6 : INSIGHTS JSON ─────────────────────────────────────────────
insights_key = f"{INSIGHTS_PREFIX}{ts}.json"
write_string_to_s3(json.dumps(insights, indent=2), PROCESSED_BUCKET, insights_key)
logging.info("Insights JSON written  s3://%s/%s", PROCESSED_BUCKET, insights_key)