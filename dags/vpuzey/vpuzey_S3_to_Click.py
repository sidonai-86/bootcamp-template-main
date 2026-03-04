from __future__ import annotations

import io
import os
import re
import logging
import urllib.request
import urllib.parse
from datetime import date, datetime

import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.short_circuit import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago


log = logging.getLogger(__name__)

# --- MinIO / S3 ---
S3_CONN_ID = "minios3_conn"
S3_BUCKET = "dev"
S3_PREFIX = "API_TO_S3/"
KEY_RE = re.compile(r"^API_TO_S3/geo_(\d{4}-\d{2}-\d{2})\.parquet$")

# Самый первый файл
EARLIEST_DATE = date(2026, 1, 25)

# --- ClickHouse ---
CH_HOST = "clickhouse01"
CH_PORT = 8123
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

CH_DB = "vpuzey"
CH_TABLE = "metrika_geo"


# ---------- Telegram failure callback  ----------
def _tg_send_plain(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text[:3800],
        "disable_web_page_preview": True,
    }
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def tg_on_failure(context):
    """
    Callback НЕ ДОЛЖЕН падать.
    Даже если Telegram недоступен — DAG должен корректно зафейлиться.
    """
    try:
        token = Variable.get("vpuzey_token")
        chat_id = Variable.get("vpuzey_chat_id")

        ti = context["ti"]
        dag_id = context["dag"].dag_id
        task_id = ti.task_id
        run_id = context.get("run_id")
        try_number = ti.try_number
        log_url = ti.log_url
        exc = context.get("exception")

        text = (
            "AIRFLOW TASK FAILED\n"
            f"DAG: {dag_id}\n"
            f"Task: {task_id}\n"
            f"Run: {run_id}\n"
            f"Try: {try_number}\n"
            f"Log: {log_url}\n\n"
            f"Exception: {type(exc).__name__ if exc else 'Unknown'}\n"
            f"{str(exc) if exc else ''}"
        )

        _tg_send_plain(token, chat_id, text)
    except Exception:
        log.exception("tg_on_failure callback failed (ignored)")


# ---------- ClickHouse ----------
def get_ch_client():
    if not CH_USER or not CH_PASSWORD:
        raise RuntimeError(
            "CLICKHOUSE_USER / CLICKHOUSE_PASSWORD are not set in Airflow environment"
        )
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )


def _get_max_loaded_date(client) -> date | None:
    """Максимальная business_date в distributed таблице (как watermark по данным)."""
    q = f"SELECT max(business_date) FROM {CH_DB}.{CH_TABLE}"
    v = client.query(q).result_rows[0][0]
    return v  # date или None


def _parse_date_from_key(key: str) -> date | None:
    m = KEY_RE.match(key)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


# ---------- Tasks ----------
def load_backlog_from_s3_to_clickhouse(**context) -> dict:
    """
    Грузит ВСЕ parquet-файлы из S3 начиная с EARLIEST_DATE,
    но только те, что новее max(business_date) в ClickHouse.
    Вставка идёт строго в distributed таблицу vpuzey.metrika_geo.
    """
    client = get_ch_client()

    max_loaded = _get_max_loaded_date(client)
    if max_loaded is None:
        log.info("ClickHouse empty. Starting from earliest=%s", EARLIEST_DATE)
        cutoff = EARLIEST_DATE
    else:
        cutoff = max_loaded
        log.info(
            "ClickHouse max(business_date)=%s. Will load keys with date > %s",
            max_loaded,
            cutoff,
        )

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    keys = s3.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX) or []
    log.info("S3 listed keys=%s under prefix=%s", len(keys), S3_PREFIX)

    dated_keys: list[tuple[date, str]] = []
    for k in keys:
        d = _parse_date_from_key(k)
        if d is None:
            continue
        if d < EARLIEST_DATE:
            continue
        if max_loaded is not None and d <= cutoff:
            continue
        dated_keys.append((d, k))

    dated_keys.sort(key=lambda x: x[0])
    if not dated_keys:
        log.info("No new parquet files to load.")
        return {"loaded_dates": [], "loaded_files": 0, "total_rows": 0}

    cols = [
        "business_date",
        "updated_at",
        "country_code",
        "country_name",
        "city_id",
        "city_name",
        "visits",
        "users",
        "new_users",
        "pageviews",
        "bounce_rate",
        "version",
    ]

    loaded_dates: list[str] = []
    total_rows_all = 0

    for d, key in dated_keys:
        log.info("Loading key=%s (date=%s)", key, d)

        obj = s3.get_key(key=key, bucket_name=S3_BUCKET)
        body: bytes = obj.get()["Body"].read()

        table = pq.read_table(io.BytesIO(body))
        df = table.to_pandas()
        total_rows = int(len(df))

        # типы под ClickHouse
        df["business_date"] = pd.to_datetime(df["business_date"]).dt.date
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True).dt.tz_convert(
            None
        )

        for c in ["country_code", "country_name", "city_name"]:
            df[c] = df[c].where(df[c].notna(), "").astype(str)

        df["city_id"] = (
            pd.to_numeric(df["city_id"], errors="coerce").fillna(0).astype("int64")
        )

        # version для ReplacingMergeTree: unix timestamp updated_at
        df["version"] = (df["updated_at"].astype("int64") // 1_000_000_000).astype(
            "uint64"
        )

        df = df[cols]

        # IMPORTANT: insert into Distributed
        client.insert(f"{CH_DB}.{CH_TABLE}", df, column_names=cols)

        log.info("Inserted OK into %s.%s: rows=%s", CH_DB, CH_TABLE, total_rows)
        loaded_dates.append(d.isoformat())
        total_rows_all += total_rows

    return {
        "loaded_dates": loaded_dates,
        "loaded_files": len(loaded_dates),
        "total_rows": total_rows_all,
        "latest_date": loaded_dates[-1],
    }


def validate_loaded(**context) -> dict:
    """Проверяем, что последняя загруженная дата реально появилась в CH."""
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="load_backlog") or {}
    latest = payload.get("latest_date")

    if not latest:
        log.info("Nothing loaded -> skip validation.")
        return {"validated": False, "count_in_ch": 0, "latest_date": None}

    client = get_ch_client()
    q = (
        f"SELECT count() "
        f"FROM {CH_DB}.{CH_TABLE} "
        f"WHERE business_date = toDate('{latest}')"
    )
    cnt = int(client.query(q).result_rows[0][0])
    log.info("Validation OK. date=%s count=%s", latest, cnt)

    return {"validated": True, "latest_date": latest, "count_in_ch": cnt}


def has_new_data(**context) -> bool:
    """
    ShortCircuit: если новых файлов не было, то notify_tg НЕ запускаем.
    Возвращает True -> downstream tasks выполняются, False -> пропускаются.
    """
    payload = context["ti"].xcom_pull(task_ids="load_backlog") or {}
    loaded_files = int(payload.get("loaded_files", 0))
    log.info("has_new_data: loaded_files=%s", loaded_files)
    return loaded_files > 0


# ---------- DAG ----------
default_args = {
    "owner": "airflow",
    "on_failure_callback": tg_on_failure,
    "retries": 1,
}

with DAG(
    dag_id="vpuzey_s3_to_clickhouse_metrika_geo",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="5 0 * * *",
    catchup=False,
    tags=["s3", "clickhouse", "metrika"],
) as dag:

    t_load_backlog = PythonOperator(
        task_id="load_backlog",
        python_callable=load_backlog_from_s3_to_clickhouse,
    )

    t_validate = PythonOperator(
        task_id="validate_loaded",
        python_callable=validate_loaded,
    )

    t_has_new_data = ShortCircuitOperator(
        task_id="has_new_data",
        python_callable=has_new_data,
    )

    t_notify = TelegramOperator(
        task_id="notify_tg",
        telegram_conn_id="vpuzey_tg",
        chat_id="{{ var.value.vpuzey_chat_id }}",
        text=(
            "✅ S3 → ClickHouse загрузка выполнена\n"
            "Файлов: <b>{{ ti.xcom_pull(task_ids='load_backlog')['loaded_files'] }}</b>\n"
            "Даты: <code>{{ ti.xcom_pull(task_ids='load_backlog')['loaded_dates'] }}</code>\n"
            "Всего строк: <b>{{ ti.xcom_pull(task_ids='load_backlog')['total_rows'] }}</b>\n"
            "Последняя дата: <b>{{ ti.xcom_pull(task_ids='validate_loaded')['latest_date'] }}</b>\n"
            "CH count(): <b>{{ ti.xcom_pull(task_ids='validate_loaded')['count_in_ch'] }}</b>"
        ),
    )

    t_load_backlog >> t_validate >> t_has_new_data >> t_notify
