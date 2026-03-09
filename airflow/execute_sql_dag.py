from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

start_date = datetime(2026,2,11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('sql_operator_dag',default_args=default_args,schedule='@once',catchup=False) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgresql_traindb_conn",
        sql="""create table if not exists 
        public.books(id SERIAL PRIMARY KEY, 
        name varchar(255));"""
    )
    insert_books = SQLExecuteQueryOperator(
        task_id = "inset_books"
        conn_id = "postgresql_traindb_conn"
        sql = """insert table public.books values('Great Expectations'), ('Idiot'), ('Father Goriot');"""
    )

    fetch_books = SQLExecuteQueryOperator(
        task_id="fetch_books",
        conn_id='postgresql_traindb_conn',
        sql="""SELECT * FROM public.books;""",
        return_last=True,
    )

    create_table >> insert_books >> fetch_books