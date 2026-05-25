from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from x_etl import run_x_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'x_dag',
    default_args=default_args,
    description='The first DAG with ETL process',
    schedule=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='x_etl_pipeline',
    python_callable=run_x_etl,
    dag=dag, 
)

run_etl