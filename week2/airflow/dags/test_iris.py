from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def test_function():
    print("Hello, Airflow!")

with DAG(
    dag_id='test_iris_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 17),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='test_task',
        python_callable=test_function,
    )
