import os

from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_chart_data
from etl.extract.extract import extract_multiple_sources
from etl.load import load_to_postgres
import pandas as pd

from etl.transform.transform import RegionDimension, ChartPreprocessor, DateDimension, ArtistDimension, TrackDimension, FactChart



with DAG('spotify_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    def extract_task(**kwargs):
        file_paths = kwargs['params']['file_paths']
        output_dir = kwargs['params']['output_dir']
        enrich = kwargs['params'].get('enrich', True)
        
        results = extract_multiple_sources(file_paths, output_dir, enrich=enrich)
        # Pushujemy pierwszy plik do transform_task (lub zmień logikę jeśli wiele plików)
        kwargs['ti'].xcom_push(key='csv_path', value=results[0])  # jeśli results to lista plików


    def transform_task(**kwargs):
        path = kwargs['ti'].xcom_pull(key='csv_path')
        df = pd.read_csv(path)

        preprocessor = ChartPreprocessor()
        df = preprocessor.transform(df)

        date_dim_processor = DateDimension(df, date_column='date')
        dim_date = date_dim_processor.transform()

        region_dim_processor = RegionDimension(df, region_column='region')
        dim_region = region_dim_processor.transform()

        artist_dim_processor = ArtistDimension(df)
        dim_artist = artist_dim_processor.transform()

        track_dim_processor = TrackDimension(df)
        dim_track = track_dim_processor.transform()

        fact_chart = FactChart(df, dim_track, dim_artist, dim_date, dim_region)
        fact_df = fact_chart.transform()

        output_dir = kwargs['params']['output_dir']  # lepiej przekazywać parametry do DAG
        source_name = os.path.basename(path).split('.')[0]
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')

        dim_date_path = os.path.join(output_dir, f'dim_date_{source_name}_{timestamp}.csv')
        dim_region_path = os.path.join(output_dir, f'dim_region_{source_name}_{timestamp}.csv')
        dim_artist_path = os.path.join(output_dir, f'dim_artist_{source_name}_{timestamp}.csv')
        dim_track_path = os.path.join(output_dir, f'dim_track_{source_name}_{timestamp}.csv')
        fact_path = os.path.join(output_dir, f'fact_{source_name}_{timestamp}.csv')

        dim_date.to_csv(dim_date_path, index=False)
        dim_region.to_csv(dim_region_path, index=False)
        dim_artist.to_csv(dim_artist_path, index=False)
        dim_track.to_csv(dim_track_path, index=False)
        fact_df.to_csv(fact_path, index=False)

        kwargs['ti'].xcom_push(
            key='transformed_files',
            value={
                'dim_date': dim_date_path,
                'dim_region': dim_region_path,
                'dim_artist': dim_artist_path,
                'dim_track': dim_track_path,
                'fact': fact_path
            }
        )


    def load_task(**kwargs):
        transformed_files = kwargs['ti'].xcom_pull(key='transformed_files')
        if not transformed_files:
            raise ValueError("Nie znaleziono ścieżek do przetworzonych plików w XCom.")

        dim_date = pd.read_csv(transformed_files['dim_date'])
        dim_region = pd.read_csv(transformed_files['dim_region'])
        dim_artist = pd.read_csv(transformed_files['dim_artist'])
        dim_track = pd.read_csv(transformed_files['dim_track'])
        fact_df = pd.read_csv(transformed_files['fact'])

        engine = create_engine('postgresql+psycopg2://user:password@host:port/dbname')

        dim_date.to_sql('dim_date', con=engine, if_exists='replace', index=False)
        dim_region.to_sql('dim_region', con=engine, if_exists='replace', index=False)
        dim_artist.to_sql('dim_artist', con=engine, if_exists='replace', index=False)
        dim_track.to_sql('dim_track', con=engine, if_exists='replace', index=False)
        fact_df.to_sql('fact_chart', con=engine, if_exists='replace', index=False)

        kwargs['ti'].xcom_push(key='load_status', value='Load completed successfully')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG('spotify_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         params={
             'file_paths': ['/path/to/input1.csv', '/path/to/input2.csv'],
             'output_dir': '/tmp/spotify_etl_output'
         }) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )

    extract >> transform >> load


