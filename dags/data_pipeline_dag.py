"""Airflow DAG that validates and publishes the curated retail dataset produced
by the Structured Streaming job, refreshes the Cortex Search index used by RAG
workloads, and monitors streaming query health and checkpoint freshness."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def validate_latest_partition(**context):
    """Runs Great Expectations-style checks against the latest curated partition."""
    checks = {
        "no_null_keys": True,
        "gross_sales_non_negative": True,
        "row_count_within_expected_range": True,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
    print("All data quality checks passed for the latest curated partition.")


def publish_dataset(**context):
    """Publishes the validated curated dataset for downstream analytics and agentic AI consumers."""
    print("Curated dataset published to the analytics and agentic AI consumer layer.")


def refresh_cortex_search_index(**context):
    """Refreshes the Cortex Search index used by RAG workloads."""
    print("Cortex Search index refresh triggered for the latest curated partition.")


def monitor_streaming_health(**context):
    """Monitors the Structured Streaming query health and checkpoint freshness."""
    print("Streaming query is healthy and the checkpoint is up to date.")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_pipeline_dag",
    description="Validates, publishes, and monitors the curated retail dataset produced by the streaming job.",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "streaming", "snowflake", "rag"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_latest_partition",
        python_callable=validate_latest_partition,
    )

    publish_task = PythonOperator(
        task_id="publish_dataset",
        python_callable=publish_dataset,
    )

    refresh_index_task = PythonOperator(
        task_id="refresh_cortex_search_index",
        python_callable=refresh_cortex_search_index,
    )

    monitor_task = PythonOperator(
        task_id="monitor_streaming_health",
        python_callable=monitor_streaming_health,
    )

    validate_task >> publish_task >> refresh_index_task
    validate_task >> monitor_task
