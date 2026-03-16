from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.pod  import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dag(
    schedule="@daily",
    start_date=datetime(2024, 3, 1), 
    catchup=False,
    tags=["ingestion", "github", "s3"],
    description="From Github parquet file to S3",
    doc_md="""
    ## GitHub to S3 Data Ingestion DAG
    """
)


def data_ingestion_github_to_s3_daily_dag():

    pod_arguments = [
        "--token", "{{ var.value.GITHUB_TOKEN }}",
        "--date", "{{ macros.ds_add(ds, -731) }}",  # Yesterday minus 2 years (-1 day - 730 days)
        "--owner", "{{ var.value.REPO_OWNER }}",
        "--repo", "{{ var.value.REPO_NAME }}",
        "--path-prefix", "{{ var.value.PATH_PREFIX }}",
        "--bucket", "{{ var.value.BRONZE_BUCKET }}",
        "--s3-prefix", "{{ var.value.BRONZE_BUCKET_S3_PREFIX }}",
        "--endpoint", "{{ var.value.S3_ENDPOINT }}"
    ]

    ingest_task = KubernetesPodOperator(
        task_id="run_ingestion_container",
        name="github-to-s3-ingester",
        namespace="airflow",
        image="github_to_s3_ingest:1.0",
        image_pull_policy="IfNotPresent",  # Check local images first
        cmds=["python", "/app/01_github_to_s3.py"],
        arguments=pod_arguments,
        get_logs=True,
        env_vars={
        },
        # Resource limits for production
        container_resources={
            "requests": {
                "cpu": "200m",
                "memory": "512Mi"
            },
            "limits": {
                "cpu": "500m",
                "memory": "1Gi"
            }
        },
        # Pod configuration
        is_delete_operator_pod=True,
        in_cluster=True,
        # Retry configuration
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        # Timeout configuration
        startup_timeout_seconds=300,
        # Security context
        security_context={
            "run_as_user": 1000,
            "run_as_group": 1000,
            "fs_group": 1000
        },
        doc="""
        ## Data Ingestion Task
        """
    )
    
    # Trigger the Spark job after successful data ingestion
    trigger_spark_job = TriggerDagRunOperator(
        task_id="trigger_spark_processing",
        trigger_dag_id="load_facts_to_silver",
        wait_for_completion=False,
        reset_dag_run=True,
        doc="""
        ## Trigger Spark Processing
        Triggers the load_facts_to_silver DAG after successful daily data ingestion
        """
    )
    
    # Set task dependencies
    ingest_task >> trigger_spark_job

    return [ingest_task, trigger_spark_job]

# Create DAG instance
dag_instance = data_ingestion_github_to_s3_daily_dag()