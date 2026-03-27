import os
import sys
import asyncio
import aiohttp
import aioboto3
import argparse
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("initial_load_async.log")]
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Migrate Parquet files from GitHub to S3/MinIO for a date range (Async version).")
    
    # Arguments
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub PAT")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--data_year_offset", type=int, default=0, help="Year offset to apply (e.g., 2 means 2026->2024)")
    parser.add_argument("--owner", default="erkansirin78", help="GitHub Repo Owner")
    parser.add_argument("--repo", default="datasets", help="GitHub Repo Name")
    parser.add_argument("--path_prefix", default="yellow_tripdata_partitioned_by_day", help="GitHub path prefix")
    parser.add_argument("--bucket", default="bronze", help="Target S3/MinIO Bucket")
    parser.add_argument("--s3_prefix", default="nyc-taxi-data", help="S3 Folder Prefix")
    parser.add_argument("--endpoint", default="http://localhost:30902", help="S3 Endpoint URL")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent downloads")
    
    return parser.parse_args()

async def download_and_upload_file(session, s3_client, file_info, s3_key, bucket, headers):
    """Download a file from GitHub and upload to S3 asynchronously."""
    file_name = file_info['name']
    try:
        async with session.get(file_info['download_url'], headers=headers, timeout=30) as response:
            if response.status == 200:
                # Read file content
                content = await response.read()
                
                # Upload to S3
                await s3_client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=content
                )
                logger.info(f"✅ Uploaded: {s3_key}")
                return True
            else:
                logger.error(f"❌ Failed to download {file_name}: HTTP {response.status}")
                return False
                
    except asyncio.TimeoutError:
        logger.error(f"⏱️ Timeout downloading {file_name}, skipping...")
        return False
    except Exception as e:
        logger.error(f"❌ Error processing {file_name}: {e}")
        return False

async def process_single_day(session, s3_client, args, current_date, github_date, semaphore, headers):
    """Handles the migration logic for one specific day asynchronously."""
    
    async with semaphore:  # Limit concurrent operations
        # Use github_date for fetching from GitHub
        github_year, github_month, github_day = github_date.strftime("%Y"), github_date.strftime("%m"), github_date.strftime("%d")
        
        # Use current_date for S3 storage path (preserves the requested date)
        s3_year, s3_month, s3_day = current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d")
        
        # GitHub uses month=3, day=7 (no leading zeros in path)
        github_path = f"{args.path_prefix}/year={github_year}/month={int(github_month)}/day={int(github_day)}"
        # S3 stores with the requested date structure
        s3_dest_prefix = f"{args.s3_prefix}/{s3_year}/{s3_month}/{s3_day}/"
        
        api_url = f"https://api.github.com/repos/{args.owner}/{args.repo}/contents/{github_path}"
        
        logger.info(f"--- Processing Date: {current_date.date()} (GitHub: {github_date.date()}) ---")
        
        try:
            async with session.get(api_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.warning(f"Skipping {current_date.date()}: {response.status}")
                    return 0
                
                files = await response.json()
                
                # Process all parquet files for this day concurrently
                tasks = []
                for file_info in files:
                    if file_info['name'].endswith('.parquet'):
                        file_name = file_info['name']
                        s3_key = f"{s3_dest_prefix}{file_name}"
                        
                        task = download_and_upload_file(
                            session, s3_client, file_info, s3_key, args.bucket, headers
                        )
                        tasks.append(task)
                
                # Wait for all files of this day to complete
                if tasks:
                    results = await asyncio.gather(*tasks)
                    successful = sum(1 for r in results if r)
                    return successful
                return 0
                
        except asyncio.TimeoutError:
            logger.error(f"⏱️ Timeout fetching file list for {current_date.date()}")
            return 0
        except Exception as e:
            logger.error(f"❌ Error processing {current_date.date()}: {e}")
            return 0

async def migrate_range_async(args):
    """Main async function to migrate date range."""
    # Convert strings to datetime objects
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    
    if start > end:
        logger.error("Start date must be before end date.")
        return
    
    # Apply year offset to get GitHub dates
    if args.data_year_offset:
        logger.info(f"Applying year offset of {args.data_year_offset} years")
        github_start = start - relativedelta(years=args.data_year_offset)
        github_end = end - relativedelta(years=args.data_year_offset)
    else:
        github_start = start
        github_end = end
    
    # Create async S3 session
    session_boto = aioboto3.Session()
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(args.concurrency)
    
    headers = {
        "Authorization": f"token {args.token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Timeout for the entire session
    timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session_boto.client(
            's3',
            endpoint_url=args.endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY", "rustfsadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY", "rustfsadmin")
        ) as s3_client:
            
            # Generate all date pairs
            date_pairs = []
            current_date = start
            github_date = github_start
            
            while current_date <= end:
                date_pairs.append((current_date, github_date))
                current_date += timedelta(days=1)
                github_date += timedelta(days=1)
            
            total_days = len(date_pairs)
            logger.info(f"📅 Starting migration of {total_days} days with {args.concurrency} concurrent connections")
            
            # Process all days concurrently (limited by semaphore)
            tasks = [
                process_single_day(session, s3_client, args, curr_date, gh_date, semaphore, headers)
                for curr_date, gh_date in date_pairs
            ]
            
            # Process with progress tracking
            total_files = 0
            completed = 0
            
            for future in asyncio.as_completed(tasks):
                result = await future
                total_files += result
                completed += 1
                
                # Progress logging every 30 days
                if completed % 30 == 0 or completed == total_days:
                    logger.info(f"📊 Progress: {completed}/{total_days} days processed ({completed*100/total_days:.1f}%)")
            
            logger.info(f"🎉 Initial Load Complete! Processed {completed} days with {total_files} files.")

def main():
    args = parse_args()
    if not args.token:
        logger.warning("⚠️ GITHUB_TOKEN missing. Rate limits will be strictly enforced.")
    
    # Run the async function
    asyncio.run(migrate_range_async(args))

if __name__ == "__main__":
    main()