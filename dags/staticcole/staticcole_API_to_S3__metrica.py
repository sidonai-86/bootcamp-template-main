from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
import requests
import pandas as pd
from datetime import datetime, date, timedelta
import time
import io


def create_request(counter_id, headers, params):
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests"
    resp = requests.post(url, headers=headers, params=params, timeout=30)

    if not resp.ok:
        raise RuntimeError(f"Create request failed: HTTP {resp.status_code}. Body: {resp.text[:1000]}")

    data = resp.json()

    if "log_request" not in data or "request_id" not in data["log_request"]:
        raise RuntimeError(f"Unexpected response format: {data}")

    return data["log_request"]["request_id"]

def get_request_status(counter_id, request_id, headers):
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}'

    response = requests.get(url, headers=headers)
    data = response.json()

    log_req = data["log_request"]
    status = log_req.get("status")

    return {
        "request_id": request_id,
        "processed": status == "processed",
        "status": status,
        "parts": log_req.get("parts", []),
    }

def get_data(counter_id, headers, request_status):
    dfs = []
    request_id = request_status["request_id"]

    for part in request_status.get("parts", []):
        part_number = part["part_number"]
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download'
        response = requests.get(url, headers=headers, timeout=60)
        if not response.ok:
            raise RuntimeError(
                f"Download failed: HTTP {response.status_code}. "
            )
        df_part = pd.read_csv(io.StringIO(response.text), sep="\t", header=0)
        dfs.append(df_part)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def clear_request(request_id: int, counter_id: int, headers: dict) -> dict:
    request_url = (
        f"https://api-metrika.yandex.net/management/v1/"
        f"counter/{counter_id}/logrequest/{request_id}"
    )

    try:
        resp = requests.get(request_url, headers=headers, timeout=30)
        resp.raise_for_status()
        request_info = resp.json().get("request", {})
    except requests.RequestException as e:
        return {
            "request_id": request_id,
            "status": "failed",
            "reason": f"failed to fetch request info: {e}",
        }

    status = request_info.get("status")

    if status != "processed":
        return {
            "request_id": request_id,
            "status": "skipped",
            "reason": f"request status is '{status}', not 'processed'",
        }

    clean_url = (
        f"https://api-metrika.yandex.net/management/v1/"
        f"counter/{counter_id}/logrequest/{request_id}/clean"
    )

    try:
        cr = requests.post(clean_url, headers=headers, timeout=30)
        cr.raise_for_status()
        return {
            "request_id": request_id,
            "status": "cleaned",
        }
    except requests.RequestException as e:
        return {
            "request_id": request_id,
            "status": "failed",
            "reason": f"clean failed: {e}",
        }

def run_metrika_logs_export(counter_id, token, params,
                           poll_interval_sec=10,
                           max_wait_sec=15 * 60,
                           cleanup_processed=True):

    if not token:
        raise ValueError("YANDEX_METRIKA_TOKEN is empty")
    if not counter_id:
        raise ValueError("YANDEX_METRIKA_COUNTER_ID is empty")

    headers = {"Authorization": f"OAuth {token}"}

    request_id = create_request(counter_id, headers, params)

    deadline = time.time() + max_wait_sec
    last_status = None

    while time.time() < deadline:
        status = get_request_status(counter_id, request_id, headers)
        last_status = status.get("status")

        if status["processed"]:
            df = get_data(counter_id, headers, status)
            if cleanup_processed:
                clear_request(request_id, counter_id, headers)
            return df

        time.sleep(poll_interval_sec)

    raise TimeoutError(f"Logrequest {request_id} not processed in time. Last status={last_status!r}")

def api_to_s3(**context):
    TOKEN = os.getenv("YANDEX_METRIKA_TOKEN")
    COUNTER_ID = os.getenv("YANDEX_METRIKA_COUNTER_ID")

    ti = context["ti"]

    end_date = context["ds"]
    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d")
        - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    metrics = [
        "ym:s:dateTime",
        "ym:s:dateTimeUTC",
        "ym:s:clientID",
        "ym:s:visitID",
        "ym:s:isNewUser",
        "ym:s:pageViews",
        "ym:s:<attribution>TrafficSource",
        "ym:s:<attribution>UTMTerm",
        "ym:s:<attribution>UTMSource",
        "ym:s:<attribution>UTMMedium",
        "ym:s:<attribution>UTMContent",
        "ym:s:<attribution>UTMCampaign",
    ]

    params = {
        "date1": start_date,
        "date2": end_date,
        "fields": ",".join(metrics),
        "source": "visits",
    }

    df = run_metrika_logs_export(
        COUNTER_ID,
        TOKEN,
        params,
        poll_interval_sec=10,
        max_wait_sec=15 * 60,
        cleanup_processed=True,
    )

    df['updated_at'] = str(datetime.now())
    filename = f'staticcole/metrika_visits/metrica_visits_{start_date}_{end_date}.parquet'

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

    total_rows = len(df)

    ti.xcom_push(key="load_date", value=start_date)
    ti.xcom_push(key="total_rows", value=total_rows)
    
    print(f"==== Загружено {total_rows} строк за период {start_date} — {end_date}")

default_args = {
    'owner':'staticcole',
    'start_date': '2026-01-24'
}

dag = DAG('staticcole_API_to_s3_metrika', default_args=default_args, 
        description="Daily visit data from Yandex Metrika",
        schedule='0 10 * * *',
        catchup = True,
        tags=["metrika", 'api', 's3'], 
        )


upload = PythonOperator(task_id="upload", python_callable=api_to_s3, dag=dag)

upload