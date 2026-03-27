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
    # The namespace parameter is expected to be like "nessie.silver"
    # We need to create just the "silver" namespace under the "nessie" catalog
    if '.' in namespace:
        catalog, ns = namespace.split('.', 1)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns};")
    else:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace};")


def load_dim_loacations(spark):
    df_dimlocation = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .load("s3a://raw/taxi_zone_lookup.txt")

    return df_dimlocation

def load_dim_paymenttype(spark):
    df_payment_type = spark.createDataFrame([
        {'paymenttypeid': 1, 'paymenttype': 'Credit card'},
        {'paymenttypeid': 2, 'paymenttype': 'Cash'},
        {'paymenttypeid': 3, 'paymenttype': 'No charge'},
        {'paymenttypeid': 4, 'paymenttype': 'Dispute'},
        {'paymenttypeid': 5, 'paymenttype': 'Unknown'},
        {'paymenttypeid': 6, 'paymenttype': 'Voided trip'}
    ])
    return df_payment_type

def load_dim_ratecode(spark):
    df_rate_code = spark.createDataFrame([
        {'ratecodeid': 1, 'ratedescription': 'Standard rate'},
        {'ratecodeid': 2, 'ratedescription': 'JFK'},
        {'ratecodeid': 3, 'ratedescription': 'Newark'},
        {'ratecodeid': 4, 'ratedescription': 'Nassau or Westchester'},
        {'ratecodeid': 5, 'ratedescription': 'Negotiated fare'},
        {'ratecodeid': 6, 'ratedescription': 'Group ride'}
    ])
    
    return df_rate_code

def load_dim_time(spark, start_date, end_date, year_offset):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Apply year offset to map to actual data dates
    # If year_offset is 2 and start_date is 2026-02-01, it becomes 2024-02-01
    if year_offset:
        start_adjusted = start - relativedelta(years=year_offset)
        end_adjusted = end - relativedelta(years=year_offset)
        # Format dates as strings for SQL query
        start_date_str = start_adjusted.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_adjusted.strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_date_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end.strftime("%Y-%m-%d %H:%M:%S")

    df_sequence = spark.sql(f"""
        SELECT explode(
            sequence(
                to_timestamp('{start_date_str}'), 
                to_timestamp('{end_date_str}'), 
                interval 1 minute
            )
        ) as Date
    """)

    # 2. Derive all the required columns from the generated Date column
    df_time_dim = df_sequence.select(
        # Format the date as a string 'yyyyMMddHHmm' and cast it to a Long integer
        F.date_format(F.col("Date"), "yyyyMMddHHmm").cast("long").alias("timeid"),
        F.col("Date"),
        F.dayofmonth(F.col("Date")).alias("dayofmonth"),
        F.weekofyear(F.col("Date")).alias("weekofyear"),
        F.month(F.col("Date")).alias("month"),
        F.year(F.col("Date")).alias("year"),
        F.hour(F.col("Date")).alias("hour"),
        F.minute(F.col("Date")).alias("minute")
    )

    return df_time_dim


# writing dimensions
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
    parser.add_argument("--source_bucket", default=os.getenv("SOURCE_BRONZE_BUCKET"), help="Bronze Bucket Name")
    parser.add_argument("--namespace", default=os.getenv("SILVER_NAMESPACE"), help="Target iceberg namespace nessie.silver")
    parser.add_argument("--static_bronze_path", default=os.getenv("STATIC_BRONZE_PATH"), help="Source bronze bucket static path default nyc-taxi-data")
    parser.add_argument("--dimtime_start_date",  default=os.getenv("DIM_TIME_START_DATE"), help="start date of fact trip")
    parser.add_argument("--dimtime_end_date",  default=os.getenv("DIM_TIME_END_DATE"), help="end date of fact trip")
    parser.add_argument("--data_year_offset", type=int, default=int(os.getenv("DATA_YEAR_OFFSET", "0")), help="Year offset to apply to dates (e.g., 2 means 2026->2024)")
    parser.add_argument("--overwride_mode", default=os.getenv("FACT_TRIP_OVERWRITE_MODE"), help="You can use overwrite for recreate default append")
    parser.add_argument("--access_key_id", default=os.getenv("AWS_ACCESS_KEY_ID"), help="S3 AWS_ACCESS_KEY_ID")
    parser.add_argument("--secret_key", default=os.getenv("AWS_SECRET_ACCESS_KEY"), help="S3 AWS_SECRET_ACCESS_KEY")
    
    return parser.parse_args()

def main():
    args = parse_args()
    spark = get_spark_session(s3_access_key=args.access_key_id, s3_secret_key=args.secret_key)

    create_name_space(spark, args.namespace)

    df_dim_loacations = load_dim_loacations(spark)
    
    df_dim_paymenttype = load_dim_paymenttype(spark=spark)

    df_dim_ratecode = load_dim_ratecode(spark=spark)

    df_dim_time = load_dim_time(spark=spark, start_date=args.dimtime_start_date, 
                                end_date=args.dimtime_end_date, year_offset=args.data_year_offset)

    for df in [(df_dim_loacations,'dimlocation'), (df_dim_paymenttype, 'dimpayment'), (df_dim_ratecode, 'dimratecode'), (df_dim_time, 'dimtime')]:
        write_to_iceberg(spark=spark, df=df[0], table_name=df[1], mode=args.overwride_mode)



if __name__ == "__main__":
    main()