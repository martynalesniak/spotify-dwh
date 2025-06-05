import sys
sys.path.append('/opt/airflow/etl')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from etl_processor import ETLProcessor

def create_etl_dag(source_name: str, schedule: str = None):

    etl = ETLProcessor()

    with DAG(
        dag_id=f"etl_{source_name}",
        start_date=datetime(2024, 1, 1),
        schedule_interval=schedule,
        catchup=False,
        tags=["etl", source_name],
    ) as dag:

        def extract_task(**kwargs):
            return etl.extract(source_type=source_name, offset=0)

        def transform_task(**kwargs):
            return etl.transform(source_type=source_name)

        def load_task(**kwargs):
            ti = kwargs["ti"]
            transform_result = ti.xcom_pull(task_ids="transform")
            return etl.load(source_type=source_name, transformed_files=transform_result["output_files"])

        with TaskGroup(group_id="etl_pipeline"):

            extract = PythonOperator(
                task_id="extract",
                python_callable=extract_task,
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_task,
            )

            load = PythonOperator(
                task_id="load",
                python_callable=load_task,
            )

            extract >> transform >> load

    return dag

globals()["etl_charts"] = create_etl_dag("charts")
