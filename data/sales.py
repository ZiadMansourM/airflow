from faker import Faker
import logging
import csv
import random
import os

log_filename = "sales_data.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global constant to control S3 upload
UPLOAD_TO_S3 = True
DATE = "2025_05_04"

# Initialize Faker
fake = Faker()

filename = f"sales_raw_data_{DATE}.csv"

# Check if file already exists
if not os.path.exists(filename):
    num_rows = 1_000
    missing_percentage = random.randint(10, 30)  # Between 10% and 30%
    num_missing = int(num_rows * missing_percentage / 100)
    missing_indices = random.sample(range(num_rows), num_missing)

    rows = []
    for i in range(num_rows):
        row = {
            "id": i,
            "name": fake.name(),
            "country": fake.country(),
            "amount": random.randint(5, 5000)
        }

        if i in missing_indices:
            if random.choice(["country", "amount"]) == "country":
                row["country"] = None
            else:
                row["amount"] = None

        rows.append(row)

    # Save to a CSV file
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logging.info(f"Saved as {filename} with ~{missing_percentage}% missing country or amount fields.")
else:
    logging.info(f"{filename} already exists. No file generated.")

# Upload to S3 if enabled
if UPLOAD_TO_S3:
    import boto3
    import io

    # Read the CSV file into memory
    with open(filename, "r", encoding="utf-8") as f:
        csv_buf = io.StringIO(f.read())

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "x62hAFEb4wkRNRaR"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW"),
    )
    s3.put_object(Bucket="raw", Key=f"sales_{DATE}.csv", Body=csv_buf.getvalue())

    logging.info("File uploaded to S3.")