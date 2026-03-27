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
    dag_id='load_dimensions_to_silver',
    default_args=default_args,
    description='Load dimensions data to Silver layer using Spark on Kubernetes',
    schedule=None,  # Triggered by github_to_s3_initial_load_dag, not scheduled
    start_date=datetime(2026, 3, 1),  # Start from March 1, 2026
    catchup=False,  # Don't backfill
    max_active_runs=1,  # Only one run at a time
    tags=['spark', 'kubernetes', 'silver', 'dimensions'],
    params={
        "dimtime_start_date": Param(
            default="",
            type="string",
            description="Start date for dimension time table (YYYY-MM-DD). Leave empty to use execution date - 1"
        ),
        "dimtime_end_date": Param(
            default="",
            type="string",
            description="End date for dimension time table (YYYY-MM-DD). Leave empty to use execution date - 1"
        ),
        "data_year_offset": Param(
            default="",
            type="string",
            description="Years of offset back to 2026 -> 2024"
        )
    }
) as dag:
    
    ingest_task = SparkKubernetesOperator(
        task_id="load-dimensions",
        namespace="default",
        name="load-dimensions",  # DNS-1123 subdomain format
        application_file="spark_applications/write_dimensions_sparkApplication.yaml",
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