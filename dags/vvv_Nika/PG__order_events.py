from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import random
from datetime import datetime


default_args = {
    'owner': 'vvv_Nika',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Table_PG__order_events",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция статусов заказов",
    catchup=False,
    tags=['technical', 'order_events']
)


def generate_events(**kwargs):
    statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
    events = []
    for _ in range(4):
        event = {
            "order_id": random.randint(1000, 9999),
            "status": random.choice(statuses),
            "ts": datetime.now().isoformat()
        }
        events.append(event)
    kwargs['ti'].xcom_push(key='events', value=events)


def insert_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="source_db")
    events = kwargs['ti'].xcom_pull(key='events', task_ids='generate_events')
    for e in events:
        hook.run(f"""
            INSERT INTO public.events (order_id, status, ts)
            VALUES ({e["order_id"]}, '{e["status"]}', '{e["ts"]}');
        """)


def update_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="source_db")

    # Удалим одну строку (крайнюю по времени)
    hook.run("""
        DELETE FROM public.events
        WHERE id = (
            SELECT id FROM public.events
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим одну строку (самую новую)
    hook.run("""
        UPDATE public.events
        SET status =
            CASE
                WHEN status = 'created' THEN 'processing'
                WHEN status = 'processing' THEN 'shipped'
                WHEN status = 'shipped' THEN 'delivered'
                ELSE status
            END,
            ts = CURRENT_TIMESTAMP
        WHERE status IN ('created', 'processing', 'shipped')
        AND id % 5 = 0;
    """)


generate_events = PythonOperator(
    task_id="generate_events",
    python_callable=generate_events,
    dag=dag,
)

insert_events = PythonOperator(
    task_id="insert_events",
    python_callable=insert_events_func,
    provide_context=True,
    dag=dag,
)

modify_events = PythonOperator(
    task_id="modify_events",
    python_callable=update_events_func,
    dag=dag,
)


generate_events >> insert_events >> modify_events