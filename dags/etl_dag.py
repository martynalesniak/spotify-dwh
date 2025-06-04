from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_to_postgres

def run_etl():
    tables = ["dim_date", "dim_track", "dim_artist", "dim_region", "fact_charts"]
    for table in tables:
        df = extract_data(table)
        df_transformed = transform_data(df, table)
        load_to_postgres(df_transformed, table)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG('spotify_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    etl_task = PythonOperator(
        task_id='run_spotify_etl',
        python_callable=run_etl
    )
