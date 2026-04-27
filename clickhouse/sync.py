"""
ClickHouse Sync — copie les données Gold depuis PostgreSQL vers ClickHouse
"""
import logging
import os
from datetime import datetime
import psycopg2
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [clickhouse-sync] %(message)s")
log = logging.getLogger(__name__)

PG_DSN = os.getenv("DB_DSN", "host=postgres port=5432 dbname=lakehouse user=lakehouse password=lakehouse")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "lakehouse")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "lakehouse")
CH_DB   = os.getenv("CLICKHOUSE_DB", "lakehouse")


def get_pg():
    return psycopg2.connect(PG_DSN)


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS,
        database=CH_DB
    )


def sync_daily_sales(pg, ch):
    log.info("Syncing daily_sales...")
    with pg.cursor() as cur:
        cur.execute("""
            SELECT date::text, total_orders, total_revenue, avg_order,
                   unique_customers, top_category
            FROM gold.daily_sales
        """)
        rows = cur.fetchall()
    if not rows:
        log.info("daily_sales: 0 rows (skipped)")
        return
    # Convert: date string → date, ensure types
    from datetime import date
    converted = []
    for r in rows:
        converted.append((
            date.fromisoformat(str(r[0])),  # Date
            int(r[1]),                        # UInt32
            float(r[2]),                      # Float64
            float(r[3]),                      # Float64
            int(r[4]),                        # UInt32
            str(r[5]) if r[5] else "",        # String
        ))
    ch.insert("daily_sales", converted,
              column_names=["date","total_orders","total_revenue",
                            "avg_order","unique_customers","top_category"])
    log.info("daily_sales: %d rows synced", len(converted))


def sync_product_performance(pg, ch):
    log.info("Syncing product_performance...")
    with pg.cursor() as cur:
        cur.execute("""
            SELECT period, product_id::text, product_name, category,
                   revenue, units_sold, avg_price
            FROM gold.product_performance
        """)
        rows = cur.fetchall()
    if not rows:
        log.info("product_performance: 0 rows (skipped)")
        return
    converted = []
    for r in rows:
        converted.append((
            str(r[0]) if r[0] else "",  # String period
            str(r[1]) if r[1] else "",  # String product_id
            str(r[2]) if r[2] else "",  # String product_name
            str(r[3]) if r[3] else "",  # String category
            float(r[4]) if r[4] else 0.0,  # Float64 revenue
            int(r[5]) if r[5] else 0,       # UInt32 units_sold
            float(r[6]) if r[6] else 0.0,  # Float32 avg_price
        ))
    ch.insert("product_performance", converted,
              column_names=["period","product_id","product_name","category",
                            "total_revenue","units_sold","avg_rating"])
    log.info("product_performance: %d rows synced", len(converted))


def sync_customer_segments(pg, ch):
    log.info("Syncing customer_segments...")
    with pg.cursor() as cur:
        cur.execute("""
            SELECT cs.customer_id::text, cs.segment, cs.total_spent,
                   cs.order_count, cs.total_spent,
                   COALESCE(mf.churn_probability, 0),
                   cs.last_order_at
            FROM gold.customer_segments cs
            LEFT JOIN ml.customer_features mf
              ON cs.customer_id::text = mf.customer_id
        """)
        rows = cur.fetchall()
    if not rows:
        log.info("customer_segments: 0 rows (skipped)")
        return
    converted = []
    for r in rows:
        converted.append((
            str(r[0]) if r[0] else "",          # String customer_id
            str(r[1]) if r[1] else "unknown",   # String segment
            float(r[2]) if r[2] else 0.0,       # Float64 total_spent
            int(r[3]) if r[3] else 0,            # UInt32 order_count
            float(r[4]) if r[4] else 0.0,       # Float64 clv_score
            float(r[5]) if r[5] else 0.0,       # Float32 churn_probability
            r[6] if r[6] else datetime(2000, 1, 1),  # DateTime last_order_at
        ))
    ch.insert("customer_segments", converted,
              column_names=["customer_id","segment","total_spent",
                            "order_count","clv_score","churn_probability",
                            "last_order_at"])
    log.info("customer_segments: %d rows synced", len(converted))


def main():
    pg = get_pg()
    ch = get_ch()
    try:
        sync_daily_sales(pg, ch)
        sync_product_performance(pg, ch)
        sync_customer_segments(pg, ch)
        log.info("ClickHouse sync complete ✅")
    finally:
        pg.close()
        ch.close()


if __name__ == "__main__":
    main()
