from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import random
from datetime import datetime


default_args = {
    'owner': 'avpalatov',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="avpalatov__Create_Table_PG__zakaz_events",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция статусов заказов",
    catchup=False,
    tags=['avpalatov', 'zakaz_events']
)


def generate_zakaz_events(**kwargs):
    statuses = ["new", "delayed", "in_proc", "approved", "cancelled", "delivered"]
    events = []
    for _ in range(4):
        event = {
            "zakaz_id": random.randint(1000, 9999),
            "status": random.choice(statuses),
            "ts": datetime.now().isoformat()
        }
        events.append(event)
    kwargs['ti'].xcom_push(key='events', value=events)


def insert_zakaz_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="avpalatov_pg")
    events = kwargs['ti'].xcom_pull(key='events', task_ids='generate_zakaz_events')
    for e in events:
        hook.run(
            "INSERT INTO avpalatov.zakaz_events (zakaz_id, status, ts) VALUES (%s, %s, %s)",
            parameters=(e["zakaz_id"], e["status"], e["ts"])
        )

def update_zakaz_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="avpalatov_pg")

    # Удалим одну строку (крайнюю по времени)
    hook.run("""
        DELETE FROM avpalatov.zakaz_events
        WHERE id = (
            SELECT id FROM avpalatov.zakaz_events
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим строки с id кратные 3
    hook.run("""
        UPDATE avpalatov.zakaz_events
        SET status =
            CASE
                WHEN status = 'new' THEN 'delayed'
                WHEN status = 'delayed' THEN 'in_proc'
                WHEN status = 'in_proc' THEN 'approved'
                WHEN status = 'approved' THEN 'delivered'
                ELSE status
            END,
            ts = CURRENT_TIMESTAMP
        WHERE status IN ('new', 'in_proc', 'approved', 'delayed')
        AND id % 3 = 0;
    """)


generate_zakaz_events = PythonOperator(
    task_id="generate_zakaz_events",
    python_callable=generate_zakaz_events,
    dag=dag,
)

insert_zakaz_events = PythonOperator(
    task_id="insert_zakaz_events",
    python_callable=insert_zakaz_events_func,
    provide_context=True,
    dag=dag,
)

modify_zakaz_events = PythonOperator(
    task_id="modify_zakaz_events",
    python_callable=update_zakaz_events_func,
    dag=dag,
)


generate_zakaz_events >> insert_zakaz_events >> modify_zakaz_events