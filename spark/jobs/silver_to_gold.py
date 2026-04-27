"""
Silver → Gold Pipeline — Apache Iceberg
Reads from Iceberg silver tables, builds analytical aggregates,
writes to Iceberg gold tables + PostgreSQL for API serving.
"""

import logging
import os
import time
from datetime import datetime, timezone

import boto3
import psycopg2
from botocore.client import Config
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [silver→gold] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
DB_DSN         = os.getenv("DB_DSN", "host=postgres port=5432 dbname=lakehouse user=lakehouse password=lakehouse")
DB_URL         = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/lakehouse")


def get_spark():
    return (
        SparkSession.builder
        .appName("SilverToGold-Iceberg")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",            "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",        "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse",   "s3a://silver/iceberg")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def wait_for_silver(max_wait=600):
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET,
                      config=Config(signature_version="s3v4"), region_name="us-east-1")
    waited = 0
    required = ["iceberg/silver/orders/", "iceberg/silver/customers/", "iceberg/silver/products/"]
    while waited < max_wait:
        try:
            ready = []
            for prefix in required:
                resp = s3.list_objects_v2(Bucket="silver", Prefix=prefix, MaxKeys=1)
                if resp.get("KeyCount", 0) > 0:
                    ready.append(prefix)
            if len(ready) == len(required):
                log.info("Silver Iceberg data ready! (%s)", ready)
                return True
            log.info("Waiting for silver tables (%ds)... ready=%d/%d", waited, len(ready), len(required))
        except Exception as e:
            log.info("Waiting for silver (%ds)... %s", waited, e)
        time.sleep(15)
        waited += 15
    return False


def log_run(conn, name, layer, rows_r, rows_w, ms, error=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs
              (pipeline_name, layer, status, rows_read, rows_written, duration_ms, error_msg, finished_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,now())
        """, (name, layer, "failed" if error else "success", rows_r, rows_w, ms, error))


def build_daily_sales(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Building gold.daily_sales (Iceberg)")

    df_orders = spark.table("lakehouse.silver.orders").filter(F.col("status") == "completed")

    df_daily = (
        df_orders
        .groupBy(F.to_date("created_at").alias("date"))
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("date")
    )

    # Top category per day
    df_cat = (
        df_orders
        .groupBy(F.to_date("created_at").alias("date"), "category")
        .agg(F.sum("total_amount").alias("cat_revenue"))
    )
    w = Window.partitionBy("date").orderBy(F.col("cat_revenue").desc())
    df_top_cat = (
        df_cat
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("date", F.col("category").alias("top_category"))
    )

    df_gold = df_daily.join(df_top_cat, "date", "left")

    # Write to Iceberg gold
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.daily_sales")
    df_gold.writeTo("lakehouse.gold.daily_sales") \
           .tableProperty("format-version", "2") \
           .createOrReplace()

    # Sync to PostgreSQL for Go API
    rows = df_gold.collect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE gold.daily_sales")
        for r in rows:
            cur.execute("""
                INSERT INTO gold.daily_sales
                  (date, total_orders, total_revenue, avg_order, unique_customers, top_category)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date) DO UPDATE SET
                  total_orders=EXCLUDED.total_orders,
                  total_revenue=EXCLUDED.total_revenue,
                  avg_order=EXCLUDED.avg_order,
                  unique_customers=EXCLUDED.unique_customers,
                  top_category=EXCLUDED.top_category,
                  updated_at=now()
            """, (r["date"], r["total_orders"], float(r["total_revenue"]),
                  float(r["avg_order"]), r["unique_customers"], r["top_category"]))

    ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    log_run(conn, "silver_to_gold_daily_sales", "gold", len(rows), len(rows), ms)
    log.info("daily_sales: %d rows in %dms (Iceberg)", len(rows), ms)


def build_product_performance(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Building gold.product_performance (Iceberg)")

    df_orders   = spark.table("lakehouse.silver.orders")
    df_products = spark.table("lakehouse.silver.products")

    df_perf = (
        df_orders
        .withColumn("period", F.date_format("created_at", "yyyy-MM"))
        .groupBy("product_id", "period")
        .agg(
            F.sum("quantity").cast("int").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_price"),
            F.round(
                F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)) /
                F.count("order_id"), 4
            ).alias("return_rate"),
        )
    )

    df_gold = df_perf.join(
        df_products.select("product_id", F.col("name").alias("product_name"), "category"),
        "product_id", "left"
    )

    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.product_performance")
    df_gold.writeTo("lakehouse.gold.product_performance") \
           .partitionedBy("period") \
           .tableProperty("format-version", "2") \
           .createOrReplace()

    rows = df_gold.collect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE gold.product_performance")
        for r in rows:
            cur.execute("""
                INSERT INTO gold.product_performance
                  (product_id, product_name, category, period, units_sold, revenue, avg_price, return_rate)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (product_id, period) DO UPDATE SET
                  units_sold=EXCLUDED.units_sold, revenue=EXCLUDED.revenue,
                  avg_price=EXCLUDED.avg_price, return_rate=EXCLUDED.return_rate
            """, (r["product_id"], r["product_name"], r["category"], r["period"],
                  r["units_sold"], float(r["revenue"]), float(r["avg_price"]),
                  float(r["return_rate"] or 0)))

    ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    log_run(conn, "silver_to_gold_product_perf", "gold", len(rows), len(rows), ms)
    log.info("product_performance: %d rows in %dms (Iceberg)", len(rows), ms)


def build_customer_segments(spark, conn):
    t0 = datetime.now(timezone.utc)
    log.info("Building gold.customer_segments (Iceberg)")

    df_orders    = spark.table("lakehouse.silver.orders")
    df_customers = spark.table("lakehouse.silver.customers")

    df_agg = (
        df_orders
        .filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.count("order_id").cast("int").alias("order_count"),
            F.max("created_at").alias("last_order_at"),
        )
    )

    df_seg = (
        df_agg
        .withColumn("days_since_last",
            F.datediff(F.current_date(), F.col("last_order_at").cast("date")))
        .withColumn("clv_score",
            F.round(
                F.least(
                    F.col("total_spent") * F.col("order_count") /
                    (F.col("days_since_last") + 1),
                    F.lit(999999.9999)
                ), 4
            )
        )
        .withColumn("segment",
            F.when(F.col("total_spent") > 3000,   "vip")
             .when(F.col("total_spent") > 1000,   "premium")
             .when(F.col("days_since_last") > 180, "at_risk")
             .when(F.col("order_count") <= 1,      "new")
             .otherwise("regular")
        )
        .join(df_customers.select("customer_id"), "customer_id", "right")
        .fillna({"total_spent": 0, "order_count": 0, "clv_score": 0, "segment": "new"})
    )

    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.customer_segments")
    df_seg.writeTo("lakehouse.gold.customer_segments") \
          .tableProperty("format-version", "2") \
          .createOrReplace()

    rows = df_seg.collect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE gold.customer_segments")
        for r in rows:
            cur.execute("""
                INSERT INTO gold.customer_segments
                  (customer_id, segment, total_spent, order_count, last_order_at, clv_score)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (customer_id) DO UPDATE SET
                  segment=EXCLUDED.segment, total_spent=EXCLUDED.total_spent,
                  order_count=EXCLUDED.order_count, last_order_at=EXCLUDED.last_order_at,
                  clv_score=EXCLUDED.clv_score, updated_at=now()
            """, (r["customer_id"], r["segment"], float(r["total_spent"] or 0),
                  int(r["order_count"] or 0), r["last_order_at"],
                  min(float(r["clv_score"] or 0), 999999.9999)))

    ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    log_run(conn, "silver_to_gold_customer_seg", "gold", len(rows), len(rows), ms)
    log.info("customer_segments: %d rows in %dms (Iceberg)", len(rows), ms)


def main():
    wait_for_silver()

    spark = get_spark()
    conn  = psycopg2.connect(DB_DSN)
    conn.autocommit = True

    try:
        # Ensure gold namespace exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

        build_daily_sales(spark, conn)
        build_product_performance(spark, conn)
        build_customer_segments(spark, conn)

        log.info("Silver→Gold (Iceberg) complete ✅")

        # Demo Iceberg features
        log.info("=== Iceberg Gold Tables ===")
        for tbl in ["daily_sales", "product_performance", "customer_segments"]:
            cnt = spark.table(f"lakehouse.gold.{tbl}").count()
            log.info("  lakehouse.gold.%s: %d rows", tbl, cnt)

    finally:
        spark.stop()
        conn.close()


if __name__ == "__main__":
    main()
