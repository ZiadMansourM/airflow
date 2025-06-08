"""generate_iot_data.py
- Synthetic IoT telemetry with intentional bad records
- One JSON-lines file per day, uploaded to MinIO/S3 if desired
"""
import json, random, uuid, datetime as dt, logging, os, io, boto3
from faker import Faker
import numpy as np

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
DATE = "2025_05_04"
ROWS = 20_000
UPLOAD_TO_S3 = True

fake = Faker()
file_name   = f"iot_raw_data_{DATE}.jsonl"
log_name    = "iot_data.log"

logging.basicConfig(
    filename=log_name,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------------------------------------------------------------------
# Skip if file already exists
# ---------------------------------------------------------------------------
if os.path.exists(file_name):
    logging.info(f"{file_name} already exists. No file generated.")
    quit()

# ---------------------------------------------------------------------------
# Decide which rows will be corrupted
# ---------------------------------------------------------------------------
missing_pct  = random.randint(10, 30)
n_missing    = int(ROWS * missing_pct / 100)
missing_rows = set(random.sample(range(ROWS), n_missing))

# to make post-processing interesting we’ll mix:
#   • missing sensor_id
#   • impossible values (temp < -30 °C or > 60 °C, humidity > 100 %)
#   • NaNs
bad_types = ["sensor_id", "temp", "humidity", "pressure"]

# ---------------------------------------------------------------------------
# Generate rows
# ---------------------------------------------------------------------------
base_ts = dt.datetime.strptime(DATE, "%Y_%m_%d").replace(tzinfo=dt.timezone.utc)
one_day_seconds = 24 * 3600
sensors = [str(uuid.uuid4())[:8] for _ in range(50)]   # 50 sensors

def good_reading(ts):
    return {
        "sensor_id": random.choice(sensors),
        "ts": ts.isoformat(),                # ISO-8601 UTC
        "temp": round(np.random.normal(22, 4), 2),       # °C
        "humidity": round(np.random.normal(55, 10), 2),  # %
        "pressure": round(np.random.normal(1013, 12), 2) # hPa
    }

with open(file_name, "w", encoding="utf-8") as fh:
    for i in range(ROWS):
        # Spread timestamps uniformly through the day
        ts = base_ts + dt.timedelta(seconds=random.randint(0, one_day_seconds-1))
        row = good_reading(ts)

        if i in missing_rows:
            corrupt = random.choice(bad_types)
            if corrupt == "sensor_id":
                row["sensor_id"] = None
            elif corrupt == "temp":
                row["temp"] = random.choice([None, round(random.uniform(-50, 80), 2)])
            elif corrupt == "humidity":
                row["humidity"] = random.choice([None, round(random.uniform(120, 200), 2)])
            elif corrupt == "pressure":
                row["pressure"] = None

        fh.write(json.dumps(row) + "\n")

logging.info(f"Saved {file_name} with ~{missing_pct}% bad rows.")

# ---------------------------------------------------------------------------
# Optional upload to MinIO/S3
# ---------------------------------------------------------------------------
if UPLOAD_TO_S3:
    with open(file_name, "r", encoding="utf-8") as f:
        buf = io.StringIO(f.read())

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "x62hAFEb4wkRNRaR"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW"),
    )
    key = f"iot_{DATE}.jsonl"
    s3.put_object(Bucket="raw", Key=key, Body=buf.getvalue())
    logging.info(f"File uploaded to S3 as raw/{key}")