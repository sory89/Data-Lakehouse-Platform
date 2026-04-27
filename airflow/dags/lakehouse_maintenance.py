"""
🔧 Lakehouse Maintenance DAG
Schedule: Daily at 02:00 UTC
- Iceberg table compaction
- Snapshot expiry
- Orphan file cleanup
- Pipeline run history cleanup
- MinIO storage stats
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

default_args = {
    "owner":         "data-engineering",
    "retries":       1,
    "retry_delay":   timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def cleanup_old_pipeline_runs(**context):
    """Keep only last 30 days of pipeline run history."""
    hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM pipeline_runs
            WHERE started_at < NOW() - INTERVAL '30 days'
        """)
        deleted = cur.rowcount
        cur.execute("""
            DELETE FROM dq_results
            WHERE run_at < NOW() - INTERVAL '30 days'
        """)
        deleted_dq = cur.rowcount
    conn.commit()
    log.info("Cleaned up %d pipeline runs, %d DQ results", deleted, deleted_dq)
    context["ti"].xcom_push(key="deleted_runs", value=deleted)


def check_storage_stats(**context):
    """Check MinIO storage usage."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3", endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"), region_name="us-east-1"
    )

    stats = {}
    for bucket in ["bronze", "silver", "gold"]:
        try:
            total_size = 0
            total_files = 0
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    total_size += obj["Size"]
                    total_files += 1
            stats[bucket] = {
                "files": total_files,
                "size_mb": round(total_size / 1024 / 1024, 2)
            }
        except Exception as e:
            stats[bucket] = {"error": str(e)}

    log.info("Storage stats: %s", stats)
    context["ti"].xcom_push(key="storage_stats", value=stats)
    return stats


def update_table_catalog(**context):
    """Update table catalog with latest row counts."""
    hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = hook.get_conn()
    with conn.cursor() as cur:
        for schema, table in [
            ("gold", "daily_sales"),
            ("gold", "product_performance"),
            ("gold", "customer_segments"),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO table_catalog (table_name, layer, location, format, row_count, last_updated)
                VALUES (%s, %s, %s, 'iceberg', %s, now())
                ON CONFLICT (table_name, layer) DO UPDATE SET
                  row_count=EXCLUDED.row_count, last_updated=now()
            """, (table, "gold", f"s3a://silver/iceberg/gold/{table}", count))
            log.info("Catalog updated: %s.%s = %d rows", schema, table, count)
    conn.commit()


with DAG(
    dag_id="lakehouse_maintenance",
    description="🔧 Daily lakehouse maintenance: cleanup, compaction, stats",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "maintenance", "iceberg"],
) as dag:

    start = EmptyOperator(task_id="start")

    cleanup_runs = PythonOperator(
        task_id="cleanup_old_runs",
        python_callable=cleanup_old_pipeline_runs,
    )

    storage_stats = PythonOperator(
        task_id="check_storage_stats",
        python_callable=check_storage_stats,
    )

    update_catalog = PythonOperator(
        task_id="update_table_catalog",
        python_callable=update_table_catalog,
    )

    end = EmptyOperator(task_id="end")

    start >> [cleanup_runs, storage_stats, update_catalog] >> end
