from datetime import datetime, timedelta
from airflow.sdk import DAG # type: ignore
from airflow.sdk.definitions.param import Param # type: ignore
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator # type: ignore

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='load_facts_to_silver',
    default_args=default_args,
    description='Load facts data to Silver layer using Spark on Kubernetes (triggered by ingestion DAG)',
    schedule=None,  # Triggered by github_to_s3_initial_load_dag, not scheduled
    start_date=datetime(2026, 3, 1),  # Start from March 1, 2026
    catchup=False,  # Don't backfill
    max_active_runs=1,  # Only one run at a time
    tags=['spark', 'kubernetes', 'silver', 'facts', 'triggered'],
    params={
        "start_date": Param(
            default="",
            type="string",
            description="Start date for fact trip table (YYYY-MM-DD). Leave empty to use execution date - 1"
        ),
        "end_date": Param(
            default="",
            type="string",
            description="End date for fact trip table (YYYY-MM-DD). Leave empty to use execution date - 1"
        ),
        "data_year_offset": Param(
            default="",
            type="string",
            description="Years of offset back to 2026 -> 2024"
        ),
        "fact_trip_overwrite_mode": Param(
            default="append",
            type="string",
            enum=["append", "overwrite"],
            description="append or overwrite table"
        )
    }
) as dag:
    
    ingest_task = SparkKubernetesOperator(
        task_id="load-facts",
        namespace="default",
        name="load-facts",  # DNS-1123 subdomain format
        application_file="spark_applications/write_fact_trip_sparkApplication.yaml",
        get_logs=True,
        do_xcom_push=False,
        success_run_history_limit=1,
        startup_timeout_seconds=600,
        log_events_on_failure=True,
        reattach_on_restart=True,
        delete_on_termination=True,
        kubernetes_conn_id="kubernetes_default",
        random_name_suffix=True
    )