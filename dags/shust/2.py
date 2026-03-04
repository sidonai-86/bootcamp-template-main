from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import time
import os

BRANCHES = ["branch_1", "branch_2"]

def decide_branch(branch):
    path = f"/tmp/branches/{branch}/report.csv"
    if os.path.exists(path):
        return f"check_files.process_{branch}"
    return f"check_files.skip_{branch}"

def process_file(branch):
    print(f"✅ Обработка файла из {branch}")


def generate_file():
    # имитация ожидания
    time.sleep(20)

    # путь к файлу
    path = "/tmp/branches/branch_1"
    os.makedirs(path, exist_ok=True)  # создаём каталог, если его нет

    file_path = os.path.join(path, "report.csv")

    # создаём файл с примерным содержимым
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("date,value\n2026-01-26,42\n")
    print(f"✅ Файл создан: {file_path}")


with DAG(
    dag_id="sensors_branches_advanced_fixed",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_file_task = PythonOperator(
        task_id=f"generate_file",
        python_callable=generate_file,
    )

    start = EmptyOperator(task_id="start")

    with TaskGroup("check_files") as check_files:
        for branch in BRANCHES:
            # создаём все задачи заранее
            wait_file = FileSensor(
                task_id=f"wait_file_{branch}",
                filepath=f"/tmp/branches/{branch}/report.csv",
                poke_interval=10,
                timeout=60,
                mode="reschedule",
            )

            branch_task = BranchPythonOperator(
                task_id=f"branch_{branch}",
                python_callable=decide_branch,
                op_args=[branch],
                trigger_rule=TriggerRule.ALL_DONE,
            )

            process = PythonOperator(
                task_id=f"process_{branch}",
                python_callable=process_file,
                op_args=[branch],
            )

            skip = EmptyOperator(task_id=f"skip_{branch}")

            # порядок соединения
            wait_file >> branch_task
            branch_task >> process
            branch_task >> skip

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> check_files >> end
    start >> generate_file_task
