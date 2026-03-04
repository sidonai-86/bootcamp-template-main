from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable


log = logging.getLogger(__name__)

TOKEN = Variable.get("vpuzey_yametrica_token")
COUNTER_ID = 103580753
URL = "https://api-metrika.yandex.net/stat/v1/data"

METRICS = [
    "ym:s:visits",
    "ym:s:users",
    "ym:s:newUsers",
    "ym:s:pageviews",
    "ym:s:bounceRate",
]
DIMENSIONS = ["ym:s:date", "ym:s:regionCountry", "ym:s:regionCity"]

S3_CONN_ID = "minios3_conn"
S3_BUCKET = "dev"


def get_data(**context) -> dict:
    business_date = context["ds"]  # YYYY-MM-DD
    log.info(
        "get_data started. business_date=%s, counter_id=%s", business_date, COUNTER_ID
    )

    headers = {"Authorization": f"OAuth {TOKEN}"}
    params = {
        "date1": business_date,
        "date2": business_date,
        "ids": COUNTER_ID,
        "metrics": ",".join(METRICS),
        "dimensions": ",".join(DIMENSIONS),
        "limit": 1000,
    }

    resp = requests.get(URL, headers=headers, params=params, timeout=60)
    log.info("Metrika response status=%s", resp.status_code)

    try:
        resp.raise_for_status()
    except Exception:
        log.exception(
            "Metrika request failed. status=%s, body_head=%s",
            resp.status_code,
            resp.text[:500],
        )
        raise

    raw = resp.json()
    log.info("Metrika payload: data_len=%s", len(raw.get("data", [])))

    return {"business_date": business_date, "raw": raw}


def transform_and_upload_to_s3(**context) -> dict:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="get_data")

    business_date = payload["business_date"]
    raw = payload["raw"]
    updated_at = datetime.now(timezone.utc).isoformat()

    log.info(
        "transform_and_upload_to_s3 started. business_date=%s, raw_data_len=%s",
        business_date,
        len(raw.get("data", [])),
    )

    rows = []
    for item in raw.get("data", []):
        dims = item["dimensions"]
        mets = item["metrics"]

        rows.append(
            {
                "country_code": dims[1].get("iso_name"),
                "country_name": dims[1].get("name"),
                "city_id": dims[2].get("id"),
                "city_name": dims[2].get("name"),
                "visits": mets[0],
                "users": mets[1],
                "new_users": mets[2],
                "pageviews": mets[3],
                "bounce_rate": mets[4],
                "business_date": business_date,
                "updated_at": updated_at,
            }
        )

    df = pd.DataFrame(rows)
    total_rows = int(len(df))

    s3_key = f"API_TO_S3/geo_{business_date}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    log.info(
        "Uploading parquet to S3. bucket=%s, key=%s, rows=%s, bytes=%s",
        S3_BUCKET,
        s3_key,
        total_rows,
        buf.getbuffer().nbytes,
    )

    if hasattr(hook, "load_bytes"):
        hook.load_bytes(
            bytes_data=buf.getvalue(),
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
    else:
        client = hook.get_conn()
        client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.getvalue())

    log.info("Upload finished OK. bucket=%s, key=%s", S3_BUCKET, s3_key)

    return {
        "business_date": business_date,
        "s3_key": s3_key,
        "total_rows": total_rows,
    }


default_args = {"owner": "airflow"}

with DAG(
    dag_id="vpuzey_API_toS3_yametrica",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["metrika", "s3", "telegram"],
) as dag:

    t_get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
    )

    t_transform_upload = PythonOperator(
        task_id="transform_and_upload_to_s3",
        python_callable=transform_and_upload_to_s3,
    )

    t_send_tlg = TelegramOperator(
        task_id="send_tlg_notification",
        telegram_conn_id="vpuzey_tg",
        chat_id="{{ var.value.vpuzey_chat_id }}",
        text=(
            "✅ Метрика за <b>{{ ti.xcom_pull(task_ids='transform_and_upload_to_s3')['business_date'] }}</b> "
            "загружена в S3\n"
            "Key: <code>{{ ti.xcom_pull(task_ids='transform_and_upload_to_s3')['s3_key'] }}</code>\n"
            "Строк: <b>{{ ti.xcom_pull(task_ids='transform_and_upload_to_s3')['total_rows'] }}</b>"
        ),
    )

    t_get_data >> t_transform_upload >> t_send_tlg
