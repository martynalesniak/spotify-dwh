from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import os
from etl.extract.extract import EnrichedCSVExtractor

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

BASE_DIR = "/data"
EXTRACTED_DIR = os.path.join(BASE_DIR, "extracted")
TRANSFORMED_DIR = os.path.join(BASE_DIR, "transformed")


EXTRACTED_FILES = {
    'charts': os.path.join(EXTRACTED_DIR, "charts_extracted.csv"),
    'billboard200': os.path.join(EXTRACTED_DIR, "billboards200_extracted.csv"),
    'digital_songs': os.path.join(EXTRACTED_DIR, "digital_songs_extracted.csv"),
    'hot100': os.path.join(EXTRACTED_DIR, "hot100_extracted.csv"),
    'radio': os.path.join(EXTRACTED_DIR, "radio_extracted.csv"),
    'streaming_songs': os.path.join(EXTRACTED_DIR, "streaming_songs_extracted.csv"),
}

def get_file_offset(file_path):
    """Pobierz offset z Airflow Variables"""
    return Variable.get(f"offset_{os.path.basename(file_path)}", default_var=0)

def update_file_offset(file_path, offset):
    """Zaktualizuj offset w Airflow Variables"""
    Variable.set(f"offset_{os.path.basename(file_path)}", offset)

def init_extracted_files():
    """Inicjalizuj pliki extracted z nagłówkami"""
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    for file in EXTRACTED_FILES.values():
        if not os.path.exists(file):
            pd.DataFrame().to_csv(file, index=False)

def extract_in_batches(file_path, source_type, limit=100):
    """Ekstrahuj dane w batchach i dopisz do pliku pośredniego"""
    offset = get_file_offset(file_path)
    extractor = EnrichedCSVExtractor(source_type)
    
    # nowe dane
    df = extractor.extract_data(file_path, offset=offset, limit=limit, enrich=True)
    
    if df.empty:
        return False 
        
    # dopisuje do extracted_source.csv
    extracted_file = EXTRACTED_FILES[source_type]
    df.to_csv(extracted_file, mode='a', header=not os.path.exists(extracted_file), index=False)
    
    # aktualizuje ile przetworzonych
    update_file_offset(file_path, offset + len(df))
    return True

def transform_data(source_type, min_records=1000):
    """Transformuj dane gdy zgromadzono wystarczającą ilość"""

    extracted_file = EXTRACTED_FILES[source_type]
    transformed_file = os.path.join(TRANSFORMED_DIR, f"{source_type}_transformed.csv")
    
    # sprawdza czy juz zebralismy tyle danych co trzeba
    if os.path.exists(extracted_file):
        df = pd.read_csv(extracted_file)
        if len(df) >= min_records:

            # tutaj trzeba dodac jak to tranformujemy
            # bla bla
            # bla bla
            #bla blabla

            transformed_df = df.copy() 
            
            # Zapisz przetworzone dane
            os.makedirs(TRANSFORMED_DIR, exist_ok=True)
            transformed_df.to_csv(transformed_file, index=False)
            
            # wywalamy z extracted
            pd.DataFrame().to_csv(extracted_file, index=False)
            return transformed_file
        
    return None

def load_to_warehouse(source_type):
    """Załaduj przetworzone dane do hurtowni"""
    transformed_file = os.path.join(TRANSFORMED_DIR, f"{source_type}_transformed.csv")
    
    if os.path.exists(transformed_file):
        df = pd.read_csv(transformed_file)
        
        # Tutaj dodaj swoją logikę ładowania do warehouse
        # np. połączenie z bazą i insert
        
        print(f"Loaded {len(df)} {source_type} records to warehouse")
        
        os.remove(transformed_file)
        return True
    return False

with DAG(
    'optimized_spotify_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    init_files = PythonOperator(
        task_id='init_extracted_files',
        python_callable=init_extracted_files,
    )

    for source_type, file_path in SOURCE_FILES.items():
        extract_task = PythonOperator(
            task_id=f'extract_{source_type}',
            python_callable=extract_in_batches,
            op_kwargs={
                'file_path': file_path,
                'source_type': source_type,
                'limit': 100,
            },
        )

        transform_task = PythonOperator(
            task_id=f'transform_{source_type}',
            python_callable=transform_data,
            op_kwargs={'source_type': source_type},
            trigger_rule='all_done',
        )

        load_task = PythonOperator(
            task_id=f'load_{source_type}',
            python_callable=load_to_warehouse,
            op_kwargs={'source_type': source_type},
            trigger_rule='all_done',
        )

        init_files >> extract_task >> transform_task >> load_task




# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from etl.runner import ETLProcessor

# default_args = {
#     'owner': 'airflow',
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
# }

# def get_etl_processor():
#     return ETLProcessor(base_dir="/data")

# with DAG(...) as dag:
    
#     init_task = PythonOperator(
#         task_id='init_files',
#         python_callable=lambda: get_etl_processor().init_extracted_files()
#     )
    
#     for source_type in ['charts', 'billboard200', ...]:
#         extract_task = PythonOperator(
#             task_id=f'extract_{source_type}',
#             python_callable=lambda: get_etl_processor().extract(
#                 file_path=f"/app/sources/{source_type}.csv",
#                 source_type=source_type,
#                 offset=get_offset_from_airflow()  # Zaimplementuj tę funkcję
#             )
#         )
        
#         # Analogicznie transform i load