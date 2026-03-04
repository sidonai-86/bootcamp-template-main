from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello World")

dag = DAG(
    'hello_world',
    description='Простой пример Hello World DAG',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag
)
