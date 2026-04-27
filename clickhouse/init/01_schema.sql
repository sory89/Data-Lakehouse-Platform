-- ClickHouse Analytics Schema
-- Optimisé pour les requêtes analytiques (OLAP)

CREATE DATABASE IF NOT EXISTS lakehouse;

-- Daily sales analytics — MergeTree pour les agrégations rapides
CREATE TABLE IF NOT EXISTS lakehouse.daily_sales (
    date          Date,
    total_orders  UInt32,
    total_revenue Float64,
    avg_order     Float64,
    unique_customers UInt32,
    top_category  String,
    inserted_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY date;

-- Product performance analytics
CREATE TABLE IF NOT EXISTS lakehouse.product_performance (
    period        String,
    product_id    String,
    product_name  String,
    category      String,
    total_revenue Float64,
    units_sold    UInt32,
    avg_rating    Float32,
    inserted_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (period, product_id);

-- Customer segments analytics
CREATE TABLE IF NOT EXISTS lakehouse.customer_segments (
    customer_id   String,
    segment       String,
    total_spent   Float64,
    order_count   UInt32,
    clv_score     Float64,
    churn_probability Float32,
    last_order_at DateTime,
    inserted_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY customer_id;

-- Revenue trends
CREATE TABLE IF NOT EXISTS lakehouse.revenue_trends (
    date          Date,
    period        String,
    total_revenue Float64,
    total_orders  UInt32,
    avg_order     Float64,
    inserted_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (period, date);
