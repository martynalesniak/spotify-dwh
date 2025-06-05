# from etl_dag_template import create_etl_dag

# # Tworzy DAG dla "charts"
# globals()["etl_charts"] = create_etl_dag("charts")

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(dag_id="test_dag", start_date=datetime(2025,6,1), schedule_interval=None, catchup=False) as dag:
    t1 = DummyOperator(task_id="dummy_task")