"""
For local test:
python 01_github_to_s3.py \
    --token "ghp_your_github_personal_access_token_here" \
    --date "2024-03-07" \
    --owner "erkansirin78" \
    --repo "datasets" \
    --path-prefix "yellow_tripdata_partitioned_by_day" \
    --bucket "bronze" \
    --s3-prefix "nyc-taxi-data" \
    --endpoint "http://localhost:30902" 
"""
import os
import sys
import requests
import boto3
import argparse
import logging
from botocore.config import Config
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("migration.log")]
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Migrate Parquet files from GitHub to S3/MinIO.")
    
    # Arguments
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub PAT")
    parser.add_argument("--date", help="Target date in YYYY-MM-DD format (Default: 2 years ago today)")
    parser.add_argument("--owner", default="erkansirin78", help="GitHub Repo Owner")
    parser.add_argument("--repo", default="datasets", help="GitHub Repo Name")
    parser.add_argument("--path-prefix", default="yellow_tripdata_partitioned_by_day", help="GitHub path prefix")
    parser.add_argument("--bucket", default="bronze", help="Target S3/MinIO Bucket")
    parser.add_argument("--s3-prefix", default="nyc-taxi-data", help="S3 Folder Prefix")
    parser.add_argument("--endpoint", default="http://localhost:30902", help="S3 Endpoint URL")
    
    return parser.parse_args()

def migrate_github_to_s3(args):
    # 1. Handle Date Logic
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        logger.error("No date provided")

    year, month, day = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
    
    github_path = f"{args.path_prefix}/year={year}/month={int(month)}/day={int(day)}"
    s3_dest_prefix = f"{args.s3_prefix}/{year}/{month}/{day}/"

    logger.info(f"Target Date: {target_date.date()} | GitHub Path: {github_path}")

    # 2. Initialize S3 Client
    s3_client = boto3.client(
        's3',
        endpoint_url=args.endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY", "rustfsadmin"),
        config=Config(signature_version='s3v4')
    )

    # 3. Get file list from GitHub
    headers = {"Authorization": f"token {args.token}", "Accept": "application/vnd.github.v3+json"}
    api_url = f"https://api.github.com/repos/{args.owner}/{args.repo}/contents/{github_path}"
    
    response = requests.get(api_url, headers=headers)
    
    if response.status_code != 200:
        logger.error(f"GitHub API Error: {response.status_code} - {response.json().get('message')}")
        return

    files = response.json()

    for file_info in files:
        if file_info['name'].endswith('.parquet'):
            file_name = file_info['name']
            download_url = file_info['download_url']
            s3_key = f"{s3_dest_prefix}{file_name}"

            logger.info(f"Streaming {file_name} to S3 bucket '{args.bucket}'...")

            try:
                with requests.get(download_url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    s3_client.upload_fileobj(r.raw, args.bucket, s3_key)
                    logger.info(f"✅ Successfully uploaded: {s3_key}")
            except Exception as e:
                logger.error(f"❌ Failed to upload {file_name}: {e}")

if __name__ == "__main__":
    args = parse_args()
    
    if not args.token:
        logger.warning("GITHUB_TOKEN not provided. API rate limits will be restricted.")
    
    migrate_github_to_s3(args)