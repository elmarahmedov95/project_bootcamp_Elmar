from airflow.sdk import dag # type: ignore
from airflow.sdk.definitions.param import Param # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator # type: ignore
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dag(
    schedule="@daily",  # Daily schedule for incremental loads
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["ingestion", "github", "s3", "unified"],
    description="Unified GitHub to S3 data ingestion - supports both daily and batch loads",
    doc_md="""
    ## Unified GitHub to S3 Data Ingestion DAG
    
    This DAG handles both use cases:
    1. **Daily incremental load** (scheduled): Automatically runs daily to fetch yesterday's data
    2. **Batch/Initial load** (manual): Can be triggered manually with custom date ranges
    
    ### Parameters (for manual runs):
    - **mode**: "daily" or "batch" (default: "daily")
    - **start_date**: For batch mode - start date (YYYY-MM-DD)
    - **end_date**: For batch mode - end date (YYYY-MM-DD)
    - **data_year_offset**: Years to offset (e.g., 2 means 2026->2024)
    - **concurrency**: For batch mode - number of concurrent connections (default: 10)
    
    ### Daily Mode:
    - Fetches data for (execution_date - 1 day - data_year_offset)
    - Uses single-day script for efficiency
    
    ### Batch Mode:
    - Processes date ranges using async script
    - Configurable concurrency for faster processing
    """,
    params={
        "mode": Param(
            default="daily",
            type="string",
            enum=["daily", "batch"],
            description="Execution mode: 'daily' for single day, 'batch' for date range"
        ),
        "start_date": Param(
            default="",
            type="string",
            description="For batch mode: Start date (YYYY-MM-DD). Leave empty for daily mode"
        ),
        "end_date": Param(
            default="",
            type="string",
            description="For batch mode: End date (YYYY-MM-DD). Leave empty for daily mode"
        ),
        "data_year_offset": Param(
            default="",
            type="string",
            description="Years of offset back (e.g., 2 means 2026->2024). Leave empty to use variable or default"
        ),
        "concurrency": Param(
            default="10",
            type="string",
            description="For batch mode: Number of concurrent connections (default: 10)"
        )
    }
)
def github_to_s3_unified_dag():
    
    # Daily incremental load task
    daily_pod_arguments = [
        "--token", "{{ var.value.GITHUB_TOKEN }}",
        "--date", "{{ macros.ds_add(dag_run.logical_date.strftime('%Y-%m-%d') if dag_run.logical_date else macros.datetime.now().strftime('%Y-%m-%d'), -731) }}",  # Yesterday minus 2 years
        "--owner", "{{ var.value.REPO_OWNER }}",
        "--repo", "{{ var.value.REPO_NAME }}",
        "--path-prefix", "{{ var.value.PATH_PREFIX }}",
        "--bucket", "{{ var.value.BRONZE_BUCKET }}",
        "--s3-prefix", "{{ var.value.BRONZE_BUCKET_S3_PREFIX }}",
        "--endpoint", "{{ var.value.S3_ENDPOINT }}"
    ]
    
    daily_ingest_task = KubernetesPodOperator(
        task_id="run_daily_ingestion",
        name="github-to-s3-daily",
        namespace="airflow",
        image="github_to_s3_ingest:1.0",
        image_pull_policy="IfNotPresent",
        cmds=["python", "/app/01_github_to_s3.py"],
        arguments=daily_pod_arguments,
        get_logs=True,
        env_vars={
            "AWS_ACCESS_KEY": "rustfsadmin",
            "AWS_SECRET_KEY": "rustfsadmin",
            "GITHUB_TOKEN": "{{ var.value.GITHUB_TOKEN }}"
        },
        container_resources={
            "requests": {"cpu": "200m", "memory": "512Mi"},
            "limits": {"cpu": "500m", "memory": "1Gi"}
        },
        is_delete_operator_pod=True,
        in_cluster=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        startup_timeout_seconds=600,
        execution_timeout=timedelta(minutes=30),
        trigger_rule="all_success",
        doc="Daily incremental data ingestion for yesterday's data"
    )
    
    # Batch/Initial load task
    batch_pod_arguments = [
        "--token", "{{ var.value.GITHUB_TOKEN }}",
        "--start_date", "{{ params.start_date if params.start_date else var.value.INITIAL_LOAD_START_DATE }}",
        "--end_date", "{{ params.end_date if params.end_date else var.value.INITIAL_LOAD_END_DATE }}",
        "--data_year_offset", "{{ params.data_year_offset if params.data_year_offset else var.value.DATA_YEAR_OFFSET | default('2') }}",
        "--owner", "{{ var.value.REPO_OWNER }}",
        "--repo", "{{ var.value.REPO_NAME }}",
        "--path_prefix", "{{ var.value.PATH_PREFIX }}",
        "--bucket", "{{ var.value.BRONZE_BUCKET }}",
        "--s3_prefix", "{{ var.value.BRONZE_BUCKET_S3_PREFIX }}",
        "--endpoint", "{{ var.value.S3_ENDPOINT }}",
        "--concurrency", "{{ params.concurrency }}"
    ]
    
    batch_ingest_task = KubernetesPodOperator(
        task_id="run_batch_ingestion",
        name="github-to-s3-batch",
        namespace="airflow",
        image="github_to_s3_ingest:1.0",
        image_pull_policy="IfNotPresent",
        cmds=["python", "/app/02_github_to_s3_initial_load_async.py"],
        arguments=batch_pod_arguments,
        get_logs=True,
        env_vars={
            "AWS_ACCESS_KEY": "rustfsadmin",
            "AWS_SECRET_KEY": "rustfsadmin",
            "GITHUB_TOKEN": "{{ var.value.GITHUB_TOKEN }}"
        },
        container_resources={
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1000m", "memory": "2Gi"}
        },
        is_delete_operator_pod=False,  # Keep pod for debugging
        in_cluster=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        startup_timeout_seconds=600,
        execution_timeout=timedelta(hours=6),  # Longer timeout for batch processing
        trigger_rule="all_success",
        doc="Batch data ingestion for date ranges"
    )
    
    # Trigger downstream transformation DAGs after successful ingestion
    trigger_dimensions = TriggerDagRunOperator(
        task_id="trigger_load_dimensions",
        trigger_dag_id="load_dimensions_to_silver",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",  # Trigger if at least one ingestion succeeded
        doc="Trigger dimension loading to silver layer"
    )
    
    trigger_facts = TriggerDagRunOperator(
        task_id="trigger_load_facts",
        trigger_dag_id="load_facts_to_silver",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
        doc="Trigger fact loading to silver layer"
    )
    
    # Branching logic based on mode parameter
    from airflow.providers.standard.operators.python import BranchPythonOperator
    
    def choose_execution_mode(**context):
        """Determine which task to run based on mode parameter."""
        mode = context['params'].get('mode', 'daily')
        if mode == 'batch':
            return 'run_batch_ingestion'
        return 'run_daily_ingestion'
    
    branch_task = BranchPythonOperator(
        task_id='choose_mode',
        python_callable=choose_execution_mode
    )
    
    # Task dependencies
    # Branch to appropriate ingestion task
    branch_task >> [daily_ingest_task, batch_ingest_task]
    
    # Only daily mode triggers downstream DAGs
    daily_ingest_task >> trigger_dimensions >> trigger_facts
    
    # Batch mode ends without triggering downstream
    # (batch loads are typically for historical data that's already processed)

# Instantiate the DAG
dag = github_to_s3_unified_dag()