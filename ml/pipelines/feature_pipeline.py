"""
Feature Engineering Pipeline — Netflix-style
Reads from Iceberg Silver, computes RFM features, trains ML models.
MLflow tracking is optional — features are always saved to PostgreSQL.
"""

import contextlib
import logging
import os

import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import GradientBoostingClassifier, RandomForestRegressor
from sklearn.metrics import roc_auc_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [feature-pipeline] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",   "minioadmin")
DB_DSN          = os.getenv("DB_DSN", "host=postgres port=5432 dbname=lakehouse user=lakehouse password=lakehouse")
MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")

# MLflow connectivity will be tested at runtime
MLFLOW_OK = False  # will be set in main()


def get_spark():
    return (
        SparkSession.builder.appName("FeaturePipeline")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",      "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/iceberg")
        .config("spark.sql.shuffle.partitions",          "4")
        .getOrCreate()
    )


def compute_features(spark):
    log.info("Computing customer features from Iceberg silver...")
    df = spark.table("lakehouse.silver.orders").filter(F.col("status") != "cancelled")

    pdf = (
        df.groupBy("customer_id").agg(
            F.datediff(F.current_date(), F.max("order_date").cast("date")).alias("days_since_last"),
            F.sum(F.when(F.datediff(F.current_date(), F.col("order_date").cast("date")) <= 7,  1).otherwise(0)).alias("order_count_7d"),
            F.sum(F.when(F.datediff(F.current_date(), F.col("order_date").cast("date")) <= 30, 1).otherwise(0)).alias("order_count_30d"),
            F.sum(F.when(F.datediff(F.current_date(), F.col("order_date").cast("date")) <= 90, 1).otherwise(0)).alias("order_count_90d"),
            F.round(F.sum(F.when(F.datediff(F.current_date(), F.col("order_date").cast("date")) <= 30, F.col("total_amount")).otherwise(0)), 2).alias("total_spent_30d"),
            F.round(F.avg("total_amount"), 2).alias("avg_basket_size"),
            F.round(F.sum("total_amount"), 2).alias("total_spent_all"),
            F.round(F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)) / F.count("order_id"), 4).alias("return_rate"),
            F.first("category").alias("preferred_category"),
        )
        .withColumn("clv_score",
            F.round(F.least(
                F.col("total_spent_all") * F.col("order_count_30d") / (F.col("days_since_last") + 1),
                F.lit(999999.0)
            ), 2)
        )
        .toPandas()
    )
    log.info("Features computed: %d customers", len(pdf))
    return pdf


def train_churn_model(pdf):
    log.info("Training churn model...")
    # Use median to ensure both classes exist
    median_days = pdf["days_since_last"].median()
    threshold = max(1, int(median_days))
    pdf["churned"] = (pdf["days_since_last"] > threshold).astype(int)
    if pdf["churned"].nunique() < 2:
        pdf["churned"] = (pdf.index % 2).astype(int)

    FEATS = ["order_count_7d","order_count_30d","order_count_90d",
             "total_spent_30d","avg_basket_size","return_rate","days_since_last","clv_score"]
    X = pdf[FEATS].fillna(0)
    y = pdf["churned"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    sc = StandardScaler()
    X_tr_s = sc.fit_transform(X_tr)
    X_te_s  = sc.transform(X_te)

    model = GradientBoostingClassifier(n_estimators=100, max_depth=4, random_state=42)
    model.fit(X_tr_s, y_tr)
    auc = roc_auc_score(y_te, model.predict_proba(X_te_s)[:,1])

    if MLFLOW_OK:
        with mlflow.start_run(run_name="churn_v1"):
            mlflow.log_params({"model": "GradientBoosting", "features": len(FEATS), "train_size": len(X_tr)})
            mlflow.log_metrics({"auc_roc": round(auc, 4), "churn_rate": float(y.mean())})
            mlflow.log_dict(dict(zip(FEATS, model.feature_importances_.tolist())), "importance.json")
            mlflow.sklearn.log_model(model, "model", registered_model_name="churn_model")

    log.info("Churn AUC-ROC: %.4f (threshold=%d days)", auc, threshold)
    pdf["churn_probability"] = model.predict_proba(sc.transform(X.fillna(0)))[:,1].round(4)
    return pdf, auc


def train_ltv_model(pdf):
    log.info("Training LTV model...")
    FEATS = ["order_count_30d","order_count_90d","avg_basket_size","return_rate","days_since_last"]
    X = pdf[FEATS].fillna(0)
    y = pdf["total_spent_all"].fillna(0)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_tr, y_tr)
    mae = mean_absolute_error(y_te, model.predict(X_te))

    if MLFLOW_OK:
        with mlflow.start_run(run_name="ltv_v1"):
            mlflow.log_params({"model": "RandomForest", "features": len(FEATS)})
            mlflow.log_metric("mae", round(mae, 2))
            mlflow.sklearn.log_model(model, "model", registered_model_name="ltv_model")

    log.info("LTV MAE: %.2f", mae)


def ensure_ml_schema(conn):
    """Create ml schema and tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute("""
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
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cf_churn
            ON ml.customer_features(churn_probability DESC)
        """)
    conn.commit()
    log.info("ML schema ready ✅")


def save_features(pdf, conn):
    log.info("Saving features to PostgreSQL ml schema...")
    ensure_ml_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE ml.customer_features")
        for _, r in pdf.iterrows():
            cur.execute("""
                INSERT INTO ml.customer_features
                  (customer_id,order_count_7d,order_count_30d,order_count_90d,
                   total_spent_30d,avg_basket_size,return_rate,days_since_last,
                   clv_score,churn_probability,preferred_category,computed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (customer_id) DO UPDATE SET
                  churn_probability=EXCLUDED.churn_probability,
                  clv_score=EXCLUDED.clv_score,
                  computed_at=now()
            """, (
                str(r["customer_id"]),
                int(r.get("order_count_7d") or 0),
                int(r.get("order_count_30d") or 0),
                int(r.get("order_count_90d") or 0),
                float(r.get("total_spent_30d") or 0),
                float(r.get("avg_basket_size") or 0),
                float(r.get("return_rate") or 0),
                int(r.get("days_since_last") or 999),
                float(r.get("clv_score") or 0),
                float(r.get("churn_probability") or 0),
                str(r.get("preferred_category") or "unknown"),
            ))
    conn.commit()
    log.info("Saved %d customer features", len(pdf))


def main():
    global MLFLOW_OK
    # Test MLflow connectivity (without affecting socket timeout for Spark)
    try:
        import requests as _req
        _req.get(f"{MLFLOW_URI}/health", timeout=3)
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("lakehouse_ml")
        MLFLOW_OK = True
        log.info("MLflow connected at %s", MLFLOW_URI)
    except Exception as e:
        log.warning("MLflow not reachable (%s) — features will be saved to PostgreSQL only", type(e).__name__)

    spark = get_spark()
    conn  = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    try:
        pdf          = compute_features(spark)
        pdf, auc     = train_churn_model(pdf)
        train_ltv_model(pdf)
        save_features(pdf, conn)
        log.info("Pipeline ML complete ✅  customers=%d  churn_auc=%.4f  high_risk=%d",
                 len(pdf), auc, (pdf["churn_probability"] > 0.7).sum())
    finally:
        spark.stop()
        conn.close()


if __name__ == "__main__":
    main()
