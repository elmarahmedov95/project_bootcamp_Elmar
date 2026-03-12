# imports
from sqlalchemy import create_engine
import pandas as pd
import boto3
from botocore.config import Config
import io

# db connection
engine = create_engine("postgresql://train:Ankara06@cnpg-cluster-rw.cnpg-database.svc.cluster.local:5432/traindb")
#engine_dev = create_engine("postgresql://train:Ankara06@localhost:30000/traindb")

# read data from db
df = pd.read_sql(sql="select * from public.books;", con=engine)

print(df.head())


# write data to s3
def get_s3_client():
    s3 = boto3.client('s3',
                      #endpoint_url='http://localhost:30902',
                      endpoint_url='http://rustfs-svc.rustfs.svc.cluster.local:9000',
                      aws_access_key_id='rustfsadmin',
                      aws_secret_access_key='rustfsadmin',
                      config=Config(signature_version='s3v4'))
    return s3

def save_df_to_s3(df, bucket, key, s3):
    ''' Store file to s3''' 
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

    except Exception as e:
        raise print(e)

s3 = get_s3_client()

save_df_to_s3(df=df, bucket='raw', key='traindb/public/books/books.csv', s3=s3)