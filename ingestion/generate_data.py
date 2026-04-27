"""
Data Generator — simulates e-commerce source data
Writes Parquet files to MinIO bronze layer.
"""

import io
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [generator] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",    "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",    "minioadmin")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET",   "bronze")
INTERVAL       = int(os.getenv("INTERVAL_SECONDS", "60"))

PRODUCTS = [
    (1,  "iPhone 15 Pro",    "Electronics",  1199.99),
    (2,  "Samsung S24",      "Electronics",   999.99),
    (3,  "Nike Air Max",     "Clothing",      129.99),
    (4,  "Python Cookbook",  "Books",          49.99),
    (5,  "Dyson V15",        "Home & Garden", 599.99),
    (6,  "Yoga Mat",         "Sports",         59.99),
    (7,  "Nespresso Vertuo", "Home & Garden", 149.99),
    (8,  "Levis 501",        "Clothing",       89.99),
    (9,  "Protein Powder",   "Food",           49.99),
    (10, "MacBook Air M3",   "Electronics",  1299.99),
    (11, "Sony WH-1000XM5", "Electronics",   349.99),
    (12, "Clean Code Book",  "Books",          39.99),
    (13, "Kettlebell 16kg",  "Sports",         45.99),
    (14, "Organic Coffee",   "Food",           18.99),
    (15, "Face Cream SPF50", "Beauty",         29.99),
]
COUNTRIES = ["FR","US","GB","DE","ES","IT","NL","BE","CH","CA","AU","JP"]
STATUSES  = ["completed","completed","completed","refunded","cancelled"]


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_buckets(s3):
    for bucket in [BRONZE_BUCKET, "silver", "gold", "checkpoints", "mlflow"]:
        try:
            s3.create_bucket(Bucket=bucket)
            log.info("Created bucket: %s", bucket)
        except Exception:
            pass


def write_parquet(s3, data, bucket, key):
    if not data:
        return
    table = pa.Table.from_pylist(data)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    body = buf.getvalue()
    s3.put_object(Bucket=bucket, Key=key, Body=body,
                  ContentType="application/octet-stream")
    log.info("Wrote s3://%s/%s (%d rows, %.1f KB)", bucket, key, len(data), len(body)/1024)


def generate_orders(n=100, date=None):
    if date is None:
        date = datetime.now(timezone.utc)
    orders = []
    for _ in range(n):
        p     = random.choice(PRODUCTS)
        qty   = random.randint(1, 5)
        disc  = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
        total = round(p[3] * qty * (1 - disc), 2)
        dt    = date - timedelta(seconds=random.randint(0, 86400))
        orders.append({
            "order_id":       str(uuid.uuid4()),
            "customer_id":    random.randint(1, 500),
            "product_id":     p[0],
            "product_name":   p[1],
            "category":       p[2],
            "unit_price":     p[3],
            "quantity":       qty,
            "discount":       disc,
            "total_amount":   total,
            "country":        random.choice(COUNTRIES),
            "status":         random.choice(STATUSES),
            "payment_method": random.choice(["card","paypal","apple_pay","bank_transfer"]),
            "created_at":     dt.isoformat(),
        })
    return orders


def generate_customers(n=500):
    customers = []
    for i in range(1, n + 1):
        joined = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 730))
        customers.append({
            "customer_id": i,
            "name":        f"Customer {i:04d}",
            "email":       f"customer{i}@example.com",
            "country":     random.choice(COUNTRIES),
            "segment":     random.choice(["standard","standard","premium","vip"]),
            "joined_at":   joined.isoformat(),
        })
    return customers


def generate_products():
    return [
        {
            "product_id":   p[0],
            "name":         p[1],
            "category":     p[2],
            "base_price":   p[3],
            "stock":        random.randint(0, 500),
            "rating":       round(random.uniform(3.5, 5.0), 1),
            "review_count": random.randint(10, 5000),
        }
        for p in PRODUCTS
    ]


def main():
    s3 = get_s3()
    ensure_buckets(s3)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    write_parquet(s3, generate_customers(), BRONZE_BUCKET, f"customers/snapshot_{ts}.parquet")
    write_parquet(s3, generate_products(),  BRONZE_BUCKET, f"products/snapshot_{ts}.parquet")
    log.info("Static tables written — starting order loop (interval=%ds)", INTERVAL)

    batch = 0
    while True:
        batch += 1
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y%m%d_%H%M%S")
        n   = random.randint(80, 200)
        key = f"orders/year={now.year}/month={now.month}/day={now.day}/orders_{ts}.parquet"
        write_parquet(s3, generate_orders(n, now), BRONZE_BUCKET, key)
        log.info("Batch %d: %d orders written", batch, n)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
