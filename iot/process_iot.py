#!/usr/bin/env python3
"""
process_iot.py
──────────────
• Cleans every *.jsonl under s3://raw/
• Writes per-file artefacts:
      processed/cleaned_iot/<base>_cleaned.jsonl
      processed/aggregates/<base>_aggregated.csv
• INSIGHTS step loads every *_aggregated.csv already in
      processed/aggregates/   →   computes KPIs, charts & JSON.
"""

import os, io, json, logging, datetime
import pandas as pd
import boto3

import matplotlib
matplotlib.use("Agg")             # headless backend
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

# ── CONFIG ──────────────────────────────────────────────────────────────
RAW_BUCKET        = "raw"
PROCESSED_BUCKET  = "processed"

CLEAN_PREFIX      = "cleaned/"            # processed/cleaned/…
CLEAN_SUFFIX      = "_cleaned.jsonl"

AGG_PREFIX        = "aggregates/"             # processed/aggregates/…
AGG_SUFFIX        = "_aggregated.csv"         # per-file aggregate

INSIGHTS_PREFIX   = "insights/iot_insights_"  # + <UTC> timestamp

REQUIRED_COLS     = {"sensor_id", "ts", "temp", "humidity", "pressure"}

TEMP_RANGE        = (-30, 60)   # °C
HUM_RANGE         = (0, 100)    # %
PRES_RANGE        = (900, 1100) # hPa

LOG_FILENAME      = "process_iot.log"

# S3 / MinIO creds -------------------------------------------------------
S3_ENDPOINT_URL   = os.getenv("S3_ENDPOINT_URL",  "http://localhost:9000")
AWS_ACCESS_KEY    = os.getenv("MINIO_ROOT_USER",  "x62hAFEb4wkRNRaR")
AWS_SECRET_KEY    = os.getenv("MINIO_ROOT_PASSWORD",
                               "IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW")

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
def read_jsonl_from_s3(bucket: str, key: str) -> pd.DataFrame:
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read().splitlines()
    return pd.DataFrame(json.loads(line) for line in raw)

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
logging.info("Listing raw IoT files in s3://%s", RAW_BUCKET)
raw_objs = (s3.list_objects_v2(Bucket=RAW_BUCKET).get("Contents") or [])
raw_keys = [o["Key"] for o in raw_objs if o["Key"].lower().endswith(".jsonl")]

if not raw_keys:
    logging.warning("No JSONL files found in raw bucket; nothing to do.")
    exit(0)

# ── STEP 2 : CLEAN & WRITE PER-FILE OUTPUTS ─────────────────────────────
for key in raw_keys:
    logging.info("Processing %s …", key)
    df = read_jsonl_from_s3(RAW_BUCKET, key)

    # schema validation
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        logging.error("❌  %s missing columns %s - skipped", key, missing)
        continue

    # drop nulls + out-of-range
    before = len(df)
    df = (
        df.dropna(subset=["sensor_id", "temp", "humidity", "pressure"])
        .query(
            "(@TEMP_RANGE[0] <= temp <= @TEMP_RANGE[1])"
            " & (@HUM_RANGE[0]  <= humidity <= @HUM_RANGE[1])"
            " & (@PRES_RANGE[0] <= pressure <= @PRES_RANGE[1])"
        )
        .copy()                                # <<< here
    )
    after = len(df)
    logging.info("✔️  %s  %d → %d rows after cleaning", key, before, after)
    if after == 0:
        logging.warning("All rows dropped for %s - no clean file written", key)
        continue

    # write cleaned JSONL
    base       = os.path.basename(key)                         # iot_2025_05_01.jsonl
    clean_key  = f"{CLEAN_PREFIX}{base.replace('.jsonl', CLEAN_SUFFIX)}"
    buf = io.StringIO()
    df.to_json(buf, orient="records", lines=True)
    write_string_to_s3(buf.getvalue(), PROCESSED_BUCKET, clean_key)
    logging.info("   ↳ cleaned     →  s3://%s/%s", PROCESSED_BUCKET, clean_key)

    # per-file aggregation (1-min stats per sensor)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    agg_df = (
        df.set_index("ts")
          .groupby("sensor_id")
          .resample("1min", include_groups=False)
          .agg(temp_mean=("temp", "mean"),
               temp_std =("temp", "std"),
               hum_mean =("humidity", "mean"),
               hum_std  =("humidity", "std"),
               pres_mean=("pressure", "mean"),
               pres_std =("pressure", "std"),
               samples  =("temp", "count"))
          .dropna(subset=["samples"])
          .reset_index()
          .sort_values(["sensor_id", "ts"])
    )
    agg_key = f"{AGG_PREFIX}{base.replace('.jsonl', AGG_SUFFIX)}"
    write_string_to_s3(agg_df.to_csv(index=False), PROCESSED_BUCKET, agg_key)
    logging.info("   ↳ aggregated  →  s3://%s/%s", PROCESSED_BUCKET, agg_key)

# ── STEP 3 : LOAD **ALL** PER-FILE AGGREGATES FOR INSIGHTS ──────────────
agg_history = load_all_aggregates(f"{AGG_PREFIX}iot_")
if agg_history.empty:
    logging.error("No per-file aggregates found - insights aborted.")
    exit(1)

# Weighted KPIs (weight = samples)
total_samples   = agg_history["samples"].sum()
total_sensors   = agg_history["sensor_id"].nunique()

avg_temp        = (agg_history["temp_mean"] * agg_history["samples"]).sum() / total_samples
avg_humidity    = (agg_history["hum_mean"]  * agg_history["samples"]).sum() / total_samples
avg_pressure    = (agg_history["pres_mean"] * agg_history["samples"]).sum() / total_samples

# top 5 sensors by weighted average temperature
sensor_temp = (
    agg_history
      # 1) weight each per-minute mean by its sample count
      .assign(weighted_temp=lambda d: d.temp_mean * d.samples)
      # 2) sum weights & samples per sensor
      .groupby("sensor_id", as_index=False)
      .agg(total_weighted_temp=("weighted_temp", "sum"),
           total_samples=("samples", "sum"))
      # 3) final weighted average
      .assign(avg_temp=lambda d: d.total_weighted_temp / d.total_samples)
      .drop(columns=["total_weighted_temp", "total_samples"])
      .sort_values("avg_temp", ascending=False)
      .head(5)
)

# ── STEP 4 : BUSINESS INSIGHTS ─────────────────────────────────────────
insights = {
    "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "kpis": {
        "total_sensors":   int(total_sensors),
        "total_readings":  int(total_samples),
        "avg_temp":        round(avg_temp, 2),
        "avg_humidity":    round(avg_humidity, 2),
        "avg_pressure":    round(avg_pressure, 2),
        "files_considered": len(agg_history["sensor_id"].groupby(
                               agg_history["sensor_id"].index).count())
    },
    "top_sensors_by_temp": sensor_temp.to_dict(orient="records")
}

# ── STEP 5 : CHART & KPI PNGs ───────────────────────────────────────────
ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

try:
    # Horizontal bar of top-5 hottest sensors
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(sensor_temp["sensor_id"], sensor_temp["avg_temp"])
    ax.invert_yaxis()
    ax.set_xlabel("Average temperature (°C)")
    ax.xaxis.set_major_formatter(StrMethodFormatter('{x:.1f}'))
    ax.set_title("Top 5 sensors by average temperature")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    chart_key = f"{INSIGHTS_PREFIX}{ts}.png"
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=chart_key, Body=buf.getvalue())
    logging.info("Chart written  s3://%s/%s", PROCESSED_BUCKET, chart_key)

    # KPI mini-table
    fig_tbl, ax_tbl = plt.subplots(figsize=(4, 1.6))
    ax_tbl.axis("off")
    row = [
        f"{total_sensors:,}",
        f"{total_samples:,}",
        f"{avg_temp:.1f}°C",
        f"{avg_humidity:.1f}%",
        f"{avg_pressure:.0f} hPa",
    ]
    tbl = ax_tbl.table(
        cellText=[row],
        colLabels=["Sensors", "Readings", "Avg T", "Avg H", "Avg P"],
        loc="center"
    )
    tbl.scale(1, 2)
    plt.tight_layout()

    buf_tbl = io.BytesIO()
    plt.savefig(buf_tbl, format="png", dpi=150)
    buf_tbl.seek(0)
    kpi_key = f"{INSIGHTS_PREFIX}{ts}_kpis.png"
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=kpi_key, Body=buf_tbl.getvalue())
    logging.info("KPI table written  s3://%s/%s", PROCESSED_BUCKET, kpi_key)

except Exception as e:
    logging.warning("Plot step failed: %s", e)

# ── STEP 6 : INSIGHTS JSON ─────────────────────────────────────────────
insights_key = f"{INSIGHTS_PREFIX}{ts}.json"
write_string_to_s3(json.dumps(insights, indent=2), PROCESSED_BUCKET, insights_key)
logging.info("Insights JSON written  s3://%s/%s", PROCESSED_BUCKET, insights_key)
