# DAG shushiato_API_to_S3__thespacedevs «(SpaceX/TBD launches)» 

## Загрузка запусков ракет из TheSpaceDevs API
#API Docs: [https://lldev.thespacedevs.com/2.3.0/swagger/](https://lldev.thespacedevs.com/2.3.0/swagger/)

### Что делает DAG
#- По понедельникам (07:00 UTC) загружает запуски за **предыдущую неделю** (Пн-Вс)
#- Ручной режим: диапазон дат через **Trigger DAG with config**
#- Данные сохраняются в S3 в формате **Parquet (snappy)**

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models.param import Param, ParamsDict

import pandas as pd
import requests
import logging
import io
# import os
import pyarrow

# создаёт параметры DAG для UI с валидацией и подсказками.
dag_params = ParamsDict({
    'start_date': Param('2026-01-01', type='string', description='YYYY-MM-DD'),
    'end_date': Param('2026-01-31', type='string', description='YYYY-MM-DD'),
})
# подключение к s3 и путь сохранения
MINIO_CONN_ID = 'minios3_conn'
TELEGRAM_USER = 'shushiato'
BUCKET_NAME = 'dev'
# подключение к телеграм
TELEGRAM_ID = '{{var.value.SHUSHIATO_TELEGRAM_CHAT_ID}}'
TELEGRAM_CONN_ID = 'shushiato_tg' #Создайте connection
# подключение smtp
EMAIL_TO = 'n255shu@ya.ru'
EMAIL_CONN_ID = 'smtp_shushiato_yandex' #в Airflow Connections

# @task — декоратор TaskFlow API, превращает обычную Python-функцию в Airflow задачу
# Преимущества TaskFlow:
# Автоматический XCom: return → автоматически xcom_push
# Передача данных: task2(task1()) — данные текут между задачами
# Нет boilerplate: Не нужно PythonOperator(task_id=..., python_callable=...)
@task
def determine_dates(**context):
    'Определяет даты: manual conf или предыдущая неделя по понедельникам'
    ti = context["ti"] # TaskInstance — текущая задача
    ds = context["ds"] # Data interval start — дата выполнения (YYYY-MM-DD)
    dag_run = context.get("dag_run") # Текущий запуск DAG (или None)
    conf = dag_run.conf if dag_run.conf else {} #JSON-параметры из "Trigger DAG with config" в UI
    is_history = bool(conf.get('start_date') and conf.get('end_date'))
    
    if is_history:
        # Manual
        # извлекает дату из параметров ручного запуска DAG.
        start_date = conf['start_date']
        end_date = conf['end_date']
        mode = 'manual_history'
        filename_prefix = 'thespacedevs_history'
    else:
        # Авто: предыдущая неделя Пн-Вс
        execution_date = context['execution_date']
        # Всегда получает понедельник предыдущей недели независимо от дня запуска
        prev_monday = execution_date - timedelta(days=execution_date.weekday() + 7)
        prev_sunday = prev_monday + timedelta(days=6)
        start_date = prev_monday.strftime('%Y-%m-%d')
        end_date = prev_sunday.strftime('%Y-%m-%d')
        mode = 'scheduled'
        filename_prefix = 'thespacedevs'

    logging.info(f"Режим: {mode}, {start_date} → {end_date}")
    period_str = f"{start_date.replace('-','')}_{end_date.replace('-','')}"

    return {
        'start_date': start_date,
        'end_date': end_date,
        'mode': mode,
        'filename_prefix': filename_prefix,
        'period_str': period_str
    }

@task
def extract_launches(dates_dict, **context):
    'Выгрузка запусков ракет за указанный период'
    start_date = dates_dict['start_date'] #'2026-01-01' 
    end_date = dates_dict['end_date'] #'2026-01-27'
    
    #'https://ll.thespacedevs.com/2.3.0/launches/'
    url = 'https://lldev.thespacedevs.com/2.3.0/launches/'
    params = {
        'window_start__gte': start_date,
        'window_start__lte': end_date,
        'mode': 'detailed',
        'ordering': '-window_start',
        'limit': 100,
        'format': 'json'
    }
    
    response = requests.get(url, params=params, timeout=30)
    if response.status_code != 200:
        raise ValueError(f"API Error: {response.status_code}")
    
    data = response.json()
    results = data.get('results', [])
    
    df = pd.json_normalize(results)
    df['update_at'] = datetime.now()
    df['period_start'] = start_date
    df['period_end'] = end_date
    
    rows_count = len(df)
    logging.info(f"Выгружено {rows_count} строк за {start_date} - {end_date}")
    
    return df

@task
def save_to_minio(df, dates_dict, **context):
    'Сохранение Parquet в dev/shushiato/{start}_{end}.parquet'
    if df.empty:
        logging.warning(f'Нет данных за {start_date}-{end_date}')
        return pd.DataFrame()
    
    prefix = dates_dict['filename_prefix']
    period = dates_dict['period_str']
    key = f'shushiato/{prefix}_{period}.parquet'

    # S3Hook.load_bytes() требует байты, а не DataFrame.
    # Решение: DataFrame → Parquet-байты → S3 без временных файлов
    # Создаёт пустой буфер в RAM (имитация файла)
    buffer = io.BytesIO()
    # Записывает DataFrame в буфер как Parquet-байты
    df.to_parquet(buffer, index=False)
    # Возвращается в начало для чтения
    buffer.seek(0)
    # Подключается к s3
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    # Загружает в s3
    s3_hook.load_bytes(
        buffer.getvalue(), 
        key=key,
        bucket_name=BUCKET_NAME, 
        replace=True
    )

    s3_path = f"s3://{BUCKET_NAME}/{key}"
    rows = len(df)

    logging.info(f"📁 {s3_path} ({rows} строк)")
    
    context['task_instance'].xcom_push(key='status', value='success')
    context['task_instance'].xcom_push(key='s3_path', value=s3_path)
    context['task_instance'].xcom_push(key='rows_count', value=rows)
    context['task_instance'].xcom_push(key='period', value=f"{dates_dict['start_date']}-{dates_dict['end_date']}")

default_args = {
    'owner' : 'shushiato',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    }

with DAG(
    dag_id='shushiato_API_to_S3__thespacedevs',
    default_args=default_args,    
    schedule_interval='0 7 * * 1',  # Понедельник 7:00
    start_date=datetime(2026, 1, 1),
    catchup=False, # Заполнить пропуски с start_date до сегодня
    params=dag_params,
    tags=['shushiato', 'minio', 'parquet', 'thespacedevs', 'telegram'],  
) as dag:

    dates = determine_dates()
    df = extract_launches(dates)
    save_result = save_to_minio(df, dates)
  
    # Email уведомление
    # ti.xcom_pull(key="status", task_ids="save_to_minio")
    # Извлекает данные из предыдущей задачи:
    # task_ids="save_to_minio" — из какой задачи
    # key="status" — какой XCom ключ
    # or "unknown" — дефолт, если нет данных

    email_notify = EmailOperator(
        task_id='email_notify',
        to=EMAIL_TO,
        subject='{{ dag.dag_id }} завершен: {{ ti.xcom_pull(key="status", task_ids="save_to_minio") or "unknown" }}',
        html_content='{{ ti.xcom_pull(key="s3_path", task_ids="save_to_minio") or "No data" }} ({{ ti.xcom_pull(key="rows_count", task_ids="save_to_minio") or 0 }} строк)',
        trigger_rule='all_done',  # Отправлять даже при ошибках
        conn_id=EMAIL_CONN_ID,
        dag=dag,
    )
    
    # Telegram уведомление (требует apache-airflow-providers-telegram)
    telegram_notify = TelegramOperator(
        task_id='telegram_notify',
        telegram_conn_id=TELEGRAM_CONN_ID,
        chat_id=TELEGRAM_ID,  # User ID или @channel
        text='{{ ti.xcom_pull(task_ids="save_to_minio", key="status") == "success" and "✅ Успех: " + (ti.xcom_pull(key="rows_count", task_ids="save_to_minio") | string) + " строк" or "⚠️ Пусто/Ошибка" }}',
        trigger_rule='all_done',
        dag=dag,
    )
    
    save_result >> [email_notify, telegram_notify]
