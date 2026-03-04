from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="DEMO_create_mart_gp__dbt",
    start_date=days_ago(5),
    schedule_interval=None,
)

create_data_marts = BashOperator(
    task_id="create_test_gp_table", 
    bash_command="cd /opt/dbt && dbt run --target gp --select gp.test_gp", 
    dag=dag
)

create_data_marts