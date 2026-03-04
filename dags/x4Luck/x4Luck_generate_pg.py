from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import random
from datetime import datetime

default_args = {"owner": "x4Luck", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="x4Luck_Create_Table_PG_events_order",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция статусов заказов",
    catchup=False,
    tags=["x4Luck", "events_order"],
)


def generate_order_events(**kwargs):
    statuses = ["new", "delayed", "in_proc", "approved", "cancelled", "delivered"]
    events = []
    for _ in range(4):
        event = {
            "order_id": random.randint(1000, 9999),
            "status": random.choice(statuses),
            "ts": datetime.now().isoformat(),
        }
        events.append(event)
    kwargs["ti"].xcom_push(key="events", value=events)


def insert_zakaz_events_func(**kwargs):
    # ✅ Коннект исправлен на x4Luck
    hook = PostgresHook(postgres_conn_id="x4luck_pg_con")
    events = kwargs["ti"].xcom_pull(key="events", task_ids="generate_order_events")
    
    if not events:
        return

    for e in events:
        # ✅ Схема приведена к нижнему регистру (x4luck)
        hook.run(
            "INSERT INTO x4luck.events_order (order_id, status, ts) VALUES (%s, %s, %s)",
            parameters=(e["order_id"], e["status"], e["ts"]),
        )


def update_zakaz_events_func(**kwargs):
    # ✅ Коннект исправлен на x4Luck (такой же, как в insert)
    hook = PostgresHook(postgres_conn_id="x4luck_pg_con")

    # Удалим одну строку (крайнюю по времени)
    hook.run("""
        DELETE FROM x4luck.events_order
        WHERE id = (
            SELECT id FROM x4luck.events_order
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим строки с id кратные 5
    hook.run("""
        UPDATE x4luck.events_order
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
        AND id % 5 = 0;
    """)


generate_order_events = PythonOperator(
    task_id="generate_order_events",
    python_callable=generate_order_events,
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


generate_order_events >> insert_zakaz_events >> modify_zakaz_events