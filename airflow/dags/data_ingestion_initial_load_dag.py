from airflow.sdk import dag # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod  import KubernetesPodOperator # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "github", "s3", "initial load"],
    description="From Github parquet file to S3 between two date",
    doc_md="""
    ## GitHub to S3 Data Ingestion Initial Load DAG between two dates
    """
)


def github_to_s3_initial_load_dag():

    pod_arguments = [
        "--token", "{{ var.value.GITHUB_TOKEN }}",
        "--start-date", "{{ var.value.INITIAL_LOAD_START_DATE }}",
        "--end-date", "{{ var.value.INITIAL_LOAD_END_DATE }}", 
        "--owner", "{{ var.value.REPO_OWNER }}",
        "--repo", "{{ var.value.REPO_NAME }}",
        "--path-prefix", "{{ var.value.PATH_PREFIX }}",
        "--bucket", "{{ var.value.BRONZE_BUCKET }}",
        "--s3-prefix", "{{ var.value.BRONZE_BUCKET_S3_PREFIX }}",
        "--endpoint", "{{ var.value.S3_ENDPOINT }}"
    ]

    ingest_task = KubernetesPodOperator(
        task_id="run_initial_load_container",
        name="github-to-s3-initial-load",
        namespace="airflow",
        image="github_to_s3_ingest:1.0",
        image_pull_policy="IfNotPresent",  # Check local images first
        cmds=["python", "/app/02_github_to_s3_initial_load.py"],
        arguments=pod_arguments,
        get_logs=True,
        env_vars={
        },
        # Resource limits for production
        container_resources={
            "requests": {
                "cpu": "500m",
                "memory": "1Gi"
            },
            "limits": {
                "cpu": "1000m",
                "memory": "2Gi"
            }
        },
        # Pod configuration
        is_delete_operator_pod=True,
        in_cluster=True,
        # Retry configuration
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        # Timeout configuration
        startup_timeout_seconds=600,
        # Execution timeout for long-running tasks
        execution_timeout=timedelta(hours=2),
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


    return ingest_task

# Create DAG instance
dag_instance = github_to_s3_initial_load_dag()