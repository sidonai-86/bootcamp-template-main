from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="DEMO_create_mart_ch__dbt",
    start_date=days_ago(5),
    schedule_interval=None,
)

create_data_marts = BashOperator(
    task_id="create_test_gp_table", 
    bash_command="cd /opt/dbt && dbt run --target click --select click.test_ch", 
    dag=dag
)

create_data_marts