from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'Roadmappers',
    'start_date': days_ago(1),
    'retries': 1,
    'catchup': False,
    'tag': 'roadmappers'
}

dag = DAG(
    dag_id="hello_roadmappers",
    start_date=datetime(2026, 1,1),
    schedule_interval=None,
    default_args=default_args
)

hello_rm = BashOperator(
    task_id="hello_rm", 
    bash_command='echo "Привет Roadmappers! Ты лучшее, что есть на просторах интеренета!"', 
    dag=dag
)

hello_rm