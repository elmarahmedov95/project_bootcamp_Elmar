from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse, os

def get_spark_session(s3_access_key, s3_secret_key) -> SparkSession:

    spark = (SparkSession.builder.appName("IcebergNessie")
             .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
             .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
             .getOrCreate())

    return spark

def create_name_space(spark, namespace):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace};")


def get_bronze_data(spark, bucket_name, path, start_date, end_date, year_offset=0):
    # Parse the date strings into datetime objects
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Apply year offset to map to actual data dates
    # If year_offset is 2 and start_date is 2026-02-01, it becomes 2024-02-01
    if year_offset:
        start = start - relativedelta(years=year_offset)
        end = end - relativedelta(years=year_offset)
    
    # Calculate the total number of days in the date range (inclusive)
    num_days = (end - start).days + 1
    
    # Generate a list of paths for every single day
    # Example format: 's3a://bronze/nyc-taxi-data/2024/02/28/'
    target_paths = [
        f"s3a://{bucket_name}/{path}/{(start + timedelta(days=i)).strftime('%Y/%m/%d')}/"
        for i in range(num_days)
    ]
    
    print(f"Reading data from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} (after applying year offset of {year_offset})")
    print(f"Sample path: {target_paths[0] if target_paths else 'No paths generated'}")
    
    df = (spark.read
        .option("recursiveFileLookup", "true")
        # Unpack the list so Spark reads all the daily paths at once
        .parquet(*target_paths) 
    )

    return df

## tripid
def get_trip_id(df, output_col_name):
    df = df.withColumn(output_col_name, F.monotonically_increasing_id())
    return df


# 2. tpep_pickup_datetime and	tpep_dropoff_datetime need to be converted long minites granularity as new columns

def make_time_minute(df, input_column_name, output_column_name):
    df = df.withColumn(
        output_column_name, 
        F.date_format(F.col(input_column_name), "yyyyMMddHHmm").cast("long")
    )
    
    return df

# 3. RatecodeID will be casted frm double to integer
def cast_ratecodeid(df, input_column):
    df = df.withColumn(input_column, F.col(input_column).cast("int"))
    return df


# writing facttrip
def write_to_iceberg(spark, df, table_name, mode):
    # Check if table exists
    table_exists = spark.catalog.tableExists(f"nessie.silver.{table_name}")
    
    # Only create table if it doesn't exist
    if not table_exists:
        spark.createDataFrame([], df.schema).writeTo(f"nessie.silver.{table_name}").create()

    df.write.format("iceberg").mode(mode).save(f"nessie.silver.{table_name}")


def parse_args():
    parser = argparse.ArgumentParser(description="Data Transformation.")
    
    # Arguments
    parser.add_argument("--source_bucket", default=os.getenv("SOURCE_BRONZE_BUCKET"), help="Silver Bucket Name")
    parser.add_argument("--namespace", default=os.getenv("SILVER_NAMESPACE"), help="Target iceberg namespace nessie.silver")
    parser.add_argument("--static_bronze_path", default=os.getenv("STATIC_BRONZE_PATH"), help="Source bronze bucket static path default nyc-taxi-data")
    parser.add_argument("--start_date",  default=os.getenv("FACT_TRIP_BRONZE_START_DATE"), help="start date of fact trip")
    parser.add_argument("--end_date",  default=os.getenv("FACT_TRIP_BRONZE_END_DATE"), help="end date of fact trip")
    parser.add_argument("--data_year_offset", type=int, default=int(os.getenv("DATA_YEAR_OFFSET", "0")), help="Year offset to apply to dates (e.g., 2 means 2026->2024)")
    parser.add_argument("--fact_table_name", default=os.getenv("FACT_TRIP_TABLE_NAME"), help="Target silver table default facttrip")
    parser.add_argument("--overwride_mode", default=os.getenv("FACT_TRIP_OVERWRITE_MODE"), help="You can use overwrite for recreate default append")
    parser.add_argument("--access_key_id", default=os.getenv("AWS_ACCESS_KEY_ID"), help="S3 AWS_ACCESS_KEY_ID")
    parser.add_argument("--secret_key", default=os.getenv("AWS_SECRET_ACCESS_KEY"), help="S3 AWS_SECRET_ACCESS_KEY")
    
    return parser.parse_args()

def main():
    args = parse_args()
    spark = get_spark_session(s3_access_key=args.access_key_id, s3_secret_key=args.secret_key)

    df = get_bronze_data(spark, bucket_name=args.source_bucket, 
                    path=args.static_bronze_path,
                    start_date=args.start_date, 
                    end_date=args.end_date,
                    year_offset=args.data_year_offset)
    
    df2 = get_trip_id(df=df, output_col_name="tripid")

    # pickup
    df3 = make_time_minute(df=df2,  input_column_name="tpep_pickup_datetime", output_column_name="tpep_pickup_datetime_minute")

    # dropoff
    df4 = make_time_minute(df=df3,  input_column_name="tpep_dropoff_datetime", output_column_name="tpep_dropoff_datetime_minute")

    df5 = cast_ratecodeid(df=df4, input_column="RatecodeID")


    write_to_iceberg(spark=spark, df=df5, table_name=args.fact_table_name, mode=args.overwride_mode)



if __name__ == "__main__":
    main()