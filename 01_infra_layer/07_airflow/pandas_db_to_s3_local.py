# imports
from sqlalchemy import create_engine
import pandas as pd
import boto3
from botocore.config import Config
import io

# db connection
engine = create_engine("postgresql://train:Ankara06@cnpg-cluster-rw.cnpg-database.svc.cluster.local:5432/traindb")
engine_dev = create_engine("postgresql://train:Ankara06@localhost:30000/traindb")

# read data from db
df = pd.read_sql(sql="select * from public.books;", con=engine_dev)

print(df.head()) 