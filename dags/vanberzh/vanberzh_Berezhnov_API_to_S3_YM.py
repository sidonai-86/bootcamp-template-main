from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone, timedelta

import pandas as pd
import io
import requests
import logging
import time


COUNTER_ID = 103580753
API_TOKEN = "y0__xDe_Im4BBiirjkg2Zzs_xP3srDDcLIeER6ns2I71_wDiFmYXg"
BASE_URL = "https://api-metrika.yandex.net"

S3_BUCKET = "dev"
S3_PREFIX = "vanberzh"

DEFAULT_FIELDS = {
    "visits": [
        "ym:s:visitID",
        "ym:s:isNewUser",
        "ym:s:counterUserIDHash",
        "ym:s:clientID",
        "ym:s:regionCountry",
        "ym:s:regionCity",
        "ym:s:regionCountryID",
        "ym:s:regionCityID",
        "ym:s:operatingSystem",
        "ym:s:deviceCategory",
        "ym:s:startURL",
        "ym:s:pageViews",
        "ym:s:lastTrafficSource",
        "ym:s:dateTimeUTC"
    ],
    "hits": [
        "ym:pv:watchID",
        "ym:pv:pageViewID",
        "ym:pv:counterUserIDHash",
        "ym:pv:clientID",
        "ym:pv:regionCountry",
        "ym:pv:regionCity",
        "ym:pv:regionCountryID",
        "ym:pv:regionCityID",
        "ym:pv:operatingSystem",
        "ym:pv:deviceCategory",
        "ym:pv:URL",
        "ym:pv:productEventType",
        "ym:pv:lastTrafficSource",
        "ym:pv:dateTime"
    ]
}

HEADERS = {
    "Authorization": f"OAuth {API_TOKEN}"
}


def load_metrika_logs(source: str, ds: str, **context):

    logging.info(f"[{source}] Start loading for date={ds}")

    create_url = f"{BASE_URL}/management/v1/counter/{COUNTER_ID}/logrequests"
    payload = {
        "date1": ds,
        "date2": ds,
        "fields": DEFAULT_FIELDS[source],
        "source": source
    }

    response = requests.post(create_url, headers=HEADERS, data=payload)
    response.raise_for_status()

    request_id = response.json()["log_request"]["request_id"]
    logging.info(f"[{source}] logrequest created, request_id={request_id}")

    status_url = f"{BASE_URL}/management/v1/counter/{COUNTER_ID}/logrequest/{request_id}"

    for i in range(30):
        status_response = requests.get(status_url, headers=HEADERS)
        status_response.raise_for_status()

        status = status_response.json()["log_request"]["status"]
        logging.info(f"[{source}] status={status}")

        if status == "processed":
            break

        time.sleep(10)
    else:
        raise Exception(f"[{source}] Logrequest not processed")

    parts_response = requests.get(status_url, headers=HEADERS)
    parts_response.raise_for_status()

    parts = parts_response.json()["log_request"]["parts"]
    logging.info(f"[{source}] parts count={len(parts)}")

    df = pd.DataFrame()

    for part in parts:
        part_number = part["part_number"]
        download_url = f"{status_url}/part/{part_number}/download"

        download_response = requests.get(download_url, headers=HEADERS)
        download_response.raise_for_status()

        part_df = pd.read_csv(io.BytesIO(download_response.content), delimiter='\t')
        df = pd.concat([df, part_df], ignore_index=True)

    logging.info(f"[{source}] shape:{df.shape}")

    if not df.empty:
        moscow_tz = timezone(timedelta(hours=3))
        df["updated_at"] = datetime.now(moscow_tz)

        filename = f"{S3_PREFIX}/{source}/api_YM_{ds}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name=S3_BUCKET,
            replace=True
        )

        file_size_mb = round(buffer.getbuffer().nbytes / 1024 / 1024, 2)
        logging.info(f"[{source}] saved to s3://{S3_BUCKET}/{filename}, size: {file_size_mb}")

    clean_url = f"{status_url}/clean"
    requests.post(clean_url, headers=HEADERS)

    ti = context["ti"]

    ti.xcom_push(key="rows_count", value=df.shape[0])
    ti.xcom_push(key="columns_count", value=df.shape[1])
    ti.xcom_push(key="file_size_mb", value=file_size_mb)
    ti.xcom_push(key="business_date", value=ds)
    ti.xcom_push(key="source", value=source)


def build_telegram_message(**context):

    ti = context["ti"]

    visits_rows = ti.xcom_pull(task_ids="load_visits", key="rows_count")
    visits_cols = ti.xcom_pull(task_ids="load_visits", key="columns_count")
    visits_size = ti.xcom_pull(task_ids="load_visits", key="file_size_mb")

    hits_rows = ti.xcom_pull(task_ids="load_hits", key="rows_count")
    hits_cols = ti.xcom_pull(task_ids="load_hits", key="columns_count")
    hits_size = ti.xcom_pull(task_ids="load_hits", key="file_size_mb")

    business_date = context["ds"]

    message = f"""
    Yandex Metrika loaded\n
\n\n
    Date: {business_date}\n
\n
        Visits\n
    Rows: {visits_rows}\n
    Cols: {visits_cols}\n
    Size: {visits_size} MB\n
\n
    Hits\n
    Rows: {hits_rows}\n
    Cols: {hits_cols}\n
    Size: {hits_size} MB\n
    """

    ti.xcom_push(key="telegram_message", value=message.strip())


DAG_ID = "vanberzh_Berezhnov_API_to_S3_YM"

with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["api", "s3", "yandex_metrika"]
) as dag:

    load_visits = PythonOperator(
        task_id="load_visits",
        python_callable=load_metrika_logs,
        op_kwargs={"source": "visits", "ds": "{{ ds }}"},
        provide_context=True
    )

    load_hits = PythonOperator(
        task_id="load_hits",
        python_callable=load_metrika_logs,
        op_kwargs={"source": "hits", "ds": "{{ ds }}"},
        provide_context=True
    )

    build_message = PythonOperator(
        task_id="build_telegram_message",
        python_callable=build_telegram_message,
        provide_context=True
    )

    send_telegram = TelegramOperator(
        task_id="send_telegram_notification",
        telegram_conn_id="vanberzh_tg",
        chat_id="{{ var.value.vanberzh_chat_id }}",  # ..."chat":{"id":CHAT_ID,"firs...
        text="{{ ti.xcom_pull(task_ids='build_telegram_message', key='telegram_message') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )

    [load_visits, load_hits] >> build_message >> send_telegram
