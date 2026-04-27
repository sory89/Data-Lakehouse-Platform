"""
🤖 ML Pipeline DAG — Netflix-style
Schedule: Every 6 hours
- Compute customer features from Iceberg Silver
- Train churn prediction model (GradientBoosting)
- Train LTV regression model (RandomForest)
- Log experiments to MLflow
- Save features to PostgreSQL for API serving
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner":            "ml-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

SPARK_IMAGE   = "lakehouse-ml-pipeline:latest"
LAKEHOUSE_NET = "lakehouse_default"

ICEBERG_CONF = (
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.lakehouse.type=hadoop "
    "--conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/iceberg"
)

ML_ENV = {
    "MINIO_ENDPOINT":       "http://minio:9000",
    "MINIO_ACCESS":          "minioadmin",
    "MINIO_SECRET":          "minioadmin",
    "DB_DSN":               "host=postgres port=5432 dbname=lakehouse user=lakehouse password=lakehouse",
    "MLFLOW_TRACKING_URI":  "http://mlflow:5001",
    "MLFLOW_S3_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID":     "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
}


def check_silver_ready(**context):
    """Verify Iceberg silver tables exist before running ML."""
    import boto3
    from botocore.client import Config
    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                      config=Config(signature_version="s3v4"), region_name="us-east-1")
    for prefix in ["iceberg/silver/orders/", "iceberg/silver/customers/"]:
        resp = s3.list_objects_v2(Bucket="silver", Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            raise ValueError(f"Silver table not ready: {prefix}")
    return True


def log_ml_summary(**context):
    """Log ML run summary."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), AVG(churn_probability), AVG(clv_score) FROM ml.customer_features")
        count, avg_churn, avg_clv = cur.fetchone()
    context["ti"].xcom_push(key="ml_summary", value={
        "customers": count,
        "avg_churn_probability": float(avg_churn or 0),
        "avg_clv_score": float(avg_clv or 0),
    })
    return True


with DAG(
    dag_id="ml_pipeline",
    description="🤖 Feature engineering + ML training (churn, LTV)",
    default_args=default_args,
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "features", "mlflow", "iceberg"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_silver = PythonOperator(
        task_id="check_silver_ready",
        python_callable=check_silver_ready,
    )

    feature_pipeline = DockerOperator(
        task_id="feature_pipeline",
        image=SPARK_IMAGE,
        command=f"/opt/spark/bin/spark-submit --master local[2] {ICEBERG_CONF} /opt/spark-jobs/feature_pipeline.py",
        network_mode=LAKEHOUSE_NET,
        environment=ML_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=45),
        doc_md="Computes RFM features + trains churn/LTV models + logs to MLflow",
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_ml_summary,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> check_silver >> feature_pipeline >> log_summary >> end
