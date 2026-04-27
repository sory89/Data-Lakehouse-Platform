"""
Bronze → Silver Pipeline — Apache Iceberg
Reads Parquet from MinIO bronze, applies cleaning & DQ,
writes to silver layer as Iceberg tables (ACID, schema evolution, time travel).
"""

import logging
import os
import time
from datetime import datetime, timezone

import boto3
import psycopg2
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [bronze→silver] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
DB_DSN         = os.getenv("DB_DSN", "host=postgres port=5432 dbname=lakehouse user=lakehouse password=lakehouse")
DB_URL         = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/lakehouse")


def get_spark():
    return (
        SparkSession.builder
        .appName("BronzeToSilver-Iceberg")
        # S3A config for MinIO
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Iceberg config
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",            "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",        "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse",   "s3a://silver/iceberg")
        # Performance
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )


def wait_for_data(bucket, prefix, max_wait=300):
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET,
                      config=Config(signature_version="s3v4"), region_name="us-east-1")
    waited = 0
    while waited < max_wait:
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            if resp.get("KeyCount", 0) > 0:
                log.info("Data found in s3://%s/%s", bucket, prefix)
                return True
        except Exception:
            pass
        log.info("Waiting for s3://%s/%s (%ds)...", bucket, prefix, waited)
        time.sleep(10)
        waited += 10
    return False


def log_run(conn, name, layer, rows_r, rows_w, ms, error=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs
              (pipeline_name, layer, status, rows_read, rows_written, duration_ms, error_msg, finished_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,now())
        """, (name, layer, "failed" if error else "success", rows_r, rows_w, ms, error))


def update_catalog(conn, table, layer, location, rows):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO table_catalog (table_name, layer, location, format, row_count, last_updated) "
            "VALUES (%s,%s,%s,%s,%s,now()) "
            "ON CONFLICT (table_name, layer) DO UPDATE SET "
            "row_count=EXCLUDED.row_count, last_updated=now(), format=EXCLUDED.format",
            (table, layer, location, "iceberg", rows)
        )


def run_dq(conn, table, layer, df, checks):
    total = df.count()
    for check_name, condition in checks.items():
        failed = df.filter(~condition).count()
        status = "pass" if failed == 0 else ("warn" if failed < total * 0.01 else "fail")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dq_results
                  (table_name, layer, check_name, status, rows_tested, rows_failed)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (table, layer, check_name, status, total, failed))
        log.info("DQ [%s] %s → %s (%d/%d failed)", table, check_name, status, failed, total)


def ensure_namespaces(spark):
    """Create Iceberg namespaces (databases) if not exist."""
    for ns in ["silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
        log.info("Namespace lakehouse.%s ready", ns)


def process_orders(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Orders: bronze → silver (Iceberg)")
    try:
        df_raw = (
            spark.read
            .option("mergeSchema", "true")
            .parquet("s3a://bronze/orders/year=*/month=*/day=*/*.parquet")
        )
        rows_read = df_raw.count()
        log.info("Read %d raw orders", rows_read)

        df = (
            df_raw
            .withColumn("created_at", F.to_timestamp("created_at"))
            .dropDuplicates(["order_id"])
            .dropna(subset=["order_id", "customer_id", "product_id"])
            .filter(F.col("unit_price") > 0)
            .filter(F.col("quantity") > 0)
            .filter(F.col("total_amount") > 0)
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .withColumn("revenue_net", F.round(F.col("total_amount") * (1 - F.col("discount")), 2))
            .withColumn("is_refunded", F.col("status") == "refunded")
            .withColumn("order_date", F.to_date("created_at"))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        run_dq(conn, "orders", "silver", df, {
            "no_null_order_id":      F.col("order_id").isNotNull(),
            "positive_amount":       F.col("total_amount") > 0,
            "valid_status":          F.col("status").isin(["completed","refunded","cancelled"]),
            "valid_quantity":        F.col("quantity").between(1, 100),
            "non_negative_discount": F.col("discount").between(0, 1),
        })

        # Write as Iceberg table with hidden partitioning by month
        spark.sql("DROP TABLE IF EXISTS lakehouse.silver.orders")
        df.writeTo("lakehouse.silver.orders") \
          .partitionedBy(F.months("created_at")) \
          .tableProperty("format-version", "2") \
          .tableProperty("write.parquet.compression-codec", "snappy") \
          .createOrReplace()

        rows_written = df.count()
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_orders", "silver", rows_read, rows_written, ms)
        update_catalog(conn, "orders", "silver", "s3a://silver/iceberg/silver/orders", rows_written)
        log.info("Orders: %d → %d rows in %dms (Iceberg)", rows_read, rows_written, ms)

        # Show snapshot info
        spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.silver.orders.snapshots").show(5, False)
        return rows_written

    except Exception as e:
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_orders", "silver", 0, 0, ms, str(e))
        log.error("Orders failed: %s", e)
        raise


def process_customers(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Customers: bronze → silver (Iceberg)")
    try:
        df_raw = (
            spark.read
            .option("mergeSchema", "true")
            .parquet("s3a://bronze/customers/*.parquet")
        )
        rows_read = df_raw.count()

        df = (
            df_raw
            .withColumn("joined_at", F.to_timestamp("joined_at"))
            .dropDuplicates(["customer_id"])
            .dropna(subset=["customer_id", "email"])
            .withColumn("email",   F.lower(F.trim(F.col("email"))))
            .withColumn("segment", F.lower(F.trim(F.col("segment"))))
            .withColumn("days_since_joined",
                F.datediff(F.current_date(), F.col("joined_at").cast("date")))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        run_dq(conn, "customers", "silver", df, {
            "no_null_customer_id": F.col("customer_id").isNotNull(),
            "valid_email":         F.col("email").contains("@"),
            "valid_segment":       F.col("segment").isin(["standard","premium","vip"]),
        })

        spark.sql("DROP TABLE IF EXISTS lakehouse.silver.customers")
        df.writeTo("lakehouse.silver.customers") \
          .tableProperty("format-version", "2") \
          .tableProperty("write.parquet.compression-codec", "snappy") \
          .createOrReplace()

        rows_written = df.count()
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_customers", "silver", rows_read, rows_written, ms)
        update_catalog(conn, "customers", "silver", "s3a://silver/iceberg/silver/customers", rows_written)
        log.info("Customers: %d → %d rows in %dms (Iceberg)", rows_read, rows_written, ms)
        return rows_written

    except Exception as e:
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_customers", "silver", 0, 0, ms, str(e))
        raise


def process_products(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Products: bronze → silver (Iceberg)")
    try:
        df_raw = (
            spark.read
            .option("mergeSchema", "true")
            .parquet("s3a://bronze/products/*.parquet")
        )
        rows_read = df_raw.count()

        df = (
            df_raw
            .dropDuplicates(["product_id"])
            .dropna(subset=["product_id", "name"])
            .filter(F.col("base_price") > 0)
            .withColumn("price_tier",
                F.when(F.col("base_price") < 50,   "budget")
                 .when(F.col("base_price") < 200,  "mid")
                 .when(F.col("base_price") < 500,  "premium")
                 .otherwise("luxury"))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        run_dq(conn, "products", "silver", df, {
            "no_null_product_id": F.col("product_id").isNotNull(),
            "positive_price":     F.col("base_price") > 0,
            "valid_rating":       F.col("rating").between(0, 5),
        })

        spark.sql("DROP TABLE IF EXISTS lakehouse.silver.products")
        df.writeTo("lakehouse.silver.products") \
          .tableProperty("format-version", "2") \
          .tableProperty("write.parquet.compression-codec", "snappy") \
          .createOrReplace()

        rows_written = df.count()
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_products", "silver", rows_read, rows_written, ms)
        update_catalog(conn, "products", "silver", "s3a://silver/iceberg/silver/products", rows_written)
        log.info("Products: %d → %d rows in %dms (Iceberg)", rows_read, rows_written, ms)
        return rows_written

    except Exception as e:
        ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        log_run(conn, "bronze_to_silver_products", "silver", 0, 0, ms, str(e))
        raise


def main():
    wait_for_data("bronze", "orders/")

    spark = get_spark()
    conn  = psycopg2.connect(DB_DSN)
    conn.autocommit = True

    try:
        ensure_namespaces(spark)
        n1 = process_orders(spark, conn)
        n2 = process_customers(spark, conn)
        n3 = process_products(spark, conn)
        log.info("Bronze→Silver (Iceberg) complete ✅  orders=%d customers=%d products=%d", n1, n2, n3)

        # Demo: time travel query
        log.info("=== Iceberg Features Demo ===")
        spark.sql("SELECT COUNT(*) as total FROM lakehouse.silver.orders").show()
        spark.sql("SELECT snapshot_id, committed_at, operation, summary FROM lakehouse.silver.orders.snapshots").show(5, False)
        spark.sql("SELECT * FROM lakehouse.silver.orders.history").show(5, False)

    finally:
        spark.stop()
        conn.close()


if __name__ == "__main__":
    main()
