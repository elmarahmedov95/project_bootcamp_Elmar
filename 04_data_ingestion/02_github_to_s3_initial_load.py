import os
import sys
import requests
import boto3
import argparse
import logging
from botocore.config import Config
from datetime import datetime, timedelta

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("initial_load.log")]
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Migrate Parquet files from GitHub to S3/MinIO for a date range.")
    
    # Arguments
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub PAT")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--owner", default="erkansirin78", help="GitHub Repo Owner")
    parser.add_argument("--repo", default="datasets", help="GitHub Repo Name")
    parser.add_argument("--path-prefix", default="yellow_tripdata_partitioned_by_day", help="GitHub path prefix")
    parser.add_argument("--bucket", default="bronze", help="Target S3/MinIO Bucket")
    parser.add_argument("--s3-prefix", default="nyc-taxi-data", help="S3 Folder Prefix")
    parser.add_argument("--endpoint", default="http://localhost:30902", help="S3 Endpoint URL")
    
    return parser.parse_args()

def process_single_day(s3_client, headers, args, current_date):
    """Handles the migration logic for one specific day."""
    year, month, day = current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d")
    
    # GitHub uses month=3, day=7 (no leading zeros in path)
    github_path = f"{args.path_prefix}/year={year}/month={int(month)}/day={int(day)}"
    # S3 usually keeps leading zeros for partition naming consistency
    s3_dest_prefix = f"{args.s3_prefix}/{year}/{month}/{day}/"

    api_url = f"https://api.github.com/repos/{args.owner}/{args.repo}/contents/{github_path}"
    
    logger.info(f"--- Processing Date: {current_date.date()} ---")
    response = requests.get(api_url, headers=headers)
    
    if response.status_code != 200:
        logger.warning(f"Skipping {current_date.date()}: {response.status_code} - {response.json().get('message')}")
        return

    files = response.json()
    for file_info in files:
        if file_info['name'].endswith('.parquet'):
            file_name = file_info['name']
            s3_key = f"{s3_dest_prefix}{file_name}"

            try:
                with requests.get(file_info['download_url'], headers=headers, stream=True) as r:
                    r.raise_for_status()
                    s3_client.upload_fileobj(r.raw, args.bucket, s3_key)
                    logger.info(f"✅ Uploaded: {s3_key}")
            except Exception as e:
                logger.error(f"❌ Error uploading {file_name}: {e}")

def migrate_range(args):
    # Convert strings to datetime objects
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    
    if start > end:
        logger.error("Start date must be before end date.")
        return

    # 1. Initialize S3 Client once
    s3_client = boto3.client(
        's3',
        endpoint_url=args.endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY", "rustfsadmin"),
        config=Config(signature_version='s3v4')
    )

    headers = {"Authorization": f"token {args.token}", "Accept": "application/vnd.github.v3+json"}

    # 2. Iterate through date range
    current_date = start
    while current_date <= end:
        process_single_day(s3_client, headers, args, current_date)
        current_date += timedelta(days=1)

    logger.info("🎉 Initial Load Complete!")

if __name__ == "__main__":
    args = parse_args()
    if not args.token:
        logger.warning("GITHUB_TOKEN missing. Rate limits will be strictly enforced.")
    migrate_range(args)