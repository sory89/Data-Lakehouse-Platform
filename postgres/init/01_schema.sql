-- ── Lakehouse Metastore ───────────────────────────────────────────────────────

-- Pipeline run tracking
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            SERIAL PRIMARY KEY,
    pipeline_name TEXT        NOT NULL,
    layer         TEXT        NOT NULL, -- bronze | silver | gold
    status        TEXT        NOT NULL DEFAULT 'running', -- running | success | failed
    rows_read     BIGINT      DEFAULT 0,
    rows_written  BIGINT      DEFAULT 0,
    duration_ms   INT,
    error_msg     TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

-- Table catalog (what's in the lakehouse)
CREATE TABLE IF NOT EXISTS table_catalog (
    id            SERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    layer         TEXT        NOT NULL,
    location      TEXT        NOT NULL, -- s3a://bucket/path
    format        TEXT        NOT NULL DEFAULT 'parquet',
    row_count     BIGINT      DEFAULT 0,
    size_bytes    BIGINT      DEFAULT 0,
    last_updated  TIMESTAMPTZ DEFAULT now(),
    schema_json   JSONB,
    UNIQUE (table_name, layer)
);

-- Data quality checks
CREATE TABLE IF NOT EXISTS dq_results (
    id            SERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    layer         TEXT        NOT NULL,
    check_name    TEXT        NOT NULL,
    status        TEXT        NOT NULL, -- pass | fail | warn
    rows_tested   BIGINT      DEFAULT 0,
    rows_failed   BIGINT      DEFAULT 0,
    details       TEXT,
    run_at        TIMESTAMPTZ DEFAULT now()
);

-- ── dbt Gold target schemas ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Gold: daily sales summary
CREATE TABLE IF NOT EXISTS gold.daily_sales (
    date          DATE        PRIMARY KEY,
    total_orders  INT,
    total_revenue NUMERIC(14,2),
    avg_order     NUMERIC(10,2),
    unique_customers INT,
    top_category  TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Gold: product performance
CREATE TABLE IF NOT EXISTS gold.product_performance (
    product_id    INT,
    product_name  TEXT,
    category      TEXT,
    period        TEXT,       -- YYYY-MM
    units_sold    INT,
    revenue       NUMERIC(14,2),
    avg_price     NUMERIC(10,2),
    return_rate   NUMERIC(5,4),
    PRIMARY KEY (product_id, period)
);

-- Gold: customer segments
CREATE TABLE IF NOT EXISTS gold.customer_segments (
    customer_id   INT         PRIMARY KEY,
    segment       TEXT,       -- vip | regular | at_risk | new
    total_spent   NUMERIC(14,2),
    order_count   INT,
    last_order_at TIMESTAMPTZ,
    clv_score     NUMERIC(16,4), -- customer lifetime value score
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_runs_pipeline   ON pipeline_runs(pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status     ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_dq_table        ON dq_results(table_name, run_at DESC);

-- ── Iceberg JDBC Catalog ──────────────────────────────────────────────────────
-- Iceberg uses these tables to track table metadata, snapshots, manifests
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name   VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name     VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name   VARCHAR(255) NOT NULL,
    namespace      VARCHAR(255) NOT NULL,
    property_key   VARCHAR(255),
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

-- ── ML Schema ─────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.customer_features (
    customer_id         VARCHAR(255) PRIMARY KEY,
    order_count_7d      INT          DEFAULT 0,
    order_count_30d     INT          DEFAULT 0,
    order_count_90d     INT          DEFAULT 0,
    total_spent_30d     NUMERIC(12,2) DEFAULT 0,
    avg_basket_size     NUMERIC(10,2) DEFAULT 0,
    return_rate         NUMERIC(6,4)  DEFAULT 0,
    days_since_last     INT          DEFAULT 999,
    clv_score           NUMERIC(16,4) DEFAULT 0,
    churn_probability   NUMERIC(6,4)  DEFAULT 0,
    preferred_category  VARCHAR(100),
    computed_at         TIMESTAMPTZ  DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ml.model_metrics (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(100),
    model_version   VARCHAR(50),
    metric_name     VARCHAR(100),
    metric_value    NUMERIC(10,6),
    run_id          VARCHAR(255),
    logged_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_features_churn
    ON ml.customer_features(churn_probability DESC);
CREATE INDEX IF NOT EXISTS idx_customer_features_clv
    ON ml.customer_features(clv_score DESC);
