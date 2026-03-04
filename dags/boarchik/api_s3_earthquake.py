"""
# DAG «World biggest earthquakes»

## Загрузка землятрясений
API Docs: https://earthquake.usgs.gov/fdsnws/event/1/

Пример конфига:
```json
{
"start_date": "2026-01-01",
"end_date": "2026-01-02"
}

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import requests 
import pandas as pd 
import datetime as dt

def api_download(**context):
    # Получаем данные по пяти самым сильным землятрясениям за день

    ds = context["ds"]
    ti = context["ti"]

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds) 
    end_date = conf.get("end_date", f"{ds} + 23 hours + 59 minutes + 59 seconds")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"


    # Выгружаем данные из API
    response = requests.get(
            url,
            params={
                "format": "geojson",
                "starttime": start_date,
                "endtime": end_date,
                "orderby":"magnitude",
                "limit":"5",
            },
            timeout=60,
    )
    
    # Проверяем ошибки
    response.raise_for_status()

    # Преобразовываем в словарь Python
    j_data = response.json()

    print(j_data)

    # Дата выгрузки данных
    load_time = dt.datetime.today()
    load_date = load_time.strftime("%Y-%m-%d %H:%M:%S")
    # print(load_date)

    # Создаём пустой список
    all_data = []

    for list in j_data["features"]: 
        #print(list)
        # Достаём необходимые поля из выгрузки 
        mag        = list["properties"]["mag"]
        place      = list["properties"]["place"]
        event_url  = list["properties"]["url"]
        event_time = list["properties"]["time"]/1000 # переводим в секунды

        # Переводим в формат даты
        date = dt.datetime.fromtimestamp(event_time)
        event_date = date.strftime("%Y-%m-%d %H:%M:%S")

        # Создаём словарь c нужными полями    
        records = {}

        records["mag"] = mag
        records["place"] = place
        records["event_url"] = event_url
        records["event_time"] = event_date
        records["update_at"] = load_date

        #print(records)
        all_data.append(records)

    # Преобразуем справочник в таблицу
    df = pd.json_normalize(all_data)

    filename = f"boarchik/earthquakes_{ds}.parquet"

    if is_history:
        filename = f"boarchik/earthquakes_{start_date}_{end_date}.parquet"
    else:          
        filename = f"boarchik/earthquakes_{ds}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",
            replace=True,
        )

    rows_cnt = len(df)
 
    ti.xcom_push(key="load_date", value=start_date)
    ti.xcom_push(key="rows_cnt", value=rows_cnt)
    
    print(f"==== Загружено {rows_cnt} строк за период {start_date} — {end_date}")

default_args = {
    "owner": "boarchik",
    "start_date": days_ago(7),
    "retries": 2,
}

dag = DAG(
    dag_id="boarchik_pogorelov_API_to_S3__earthquake",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=True,
    description="API earthquake",
    tags=["api", "earthquake", "s3", "telegram"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="boarchik_tg",
    chat_id="{{ var.value.BOARCHIK_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Пять самых мощных землятрясений по миру за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>earthquakes_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='rows_cnt') }}</b>"
    ),
    dag=dag,
)

upload >> send_message_telegram