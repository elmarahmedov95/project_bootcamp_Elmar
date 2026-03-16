from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod  import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "github", "s3"],
    description="From postgresql table to S3",
    doc_md="""
    ## GitHub to S3 Data Ingestion DAG
    """
)
def db_to_s3_dag():

    ingest_task = KubernetesPodOperator(
        task_id="run_ingestion_container",
        name="db-to-s3-ingester",
        namespace="airflow",
        image="pandas_db_to_s3:1.0",
        image_pull_policy="IfNotPresent",  # Check local images first
        cmds=["python", "/app/pandas_db_to_s3.py"],
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


    return ingest_task

# Create DAG instance
dag_instance = db_to_s3_dag()