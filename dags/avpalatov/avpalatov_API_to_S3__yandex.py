"""
# DAG avpalatov_API_to_S3__yandex «(date,country,city,language,browser,device,os,hour,visits,users,new_users,page_views,update_at)»

## Загрузка закгрузка метрик из api-metrika.yandex.net
API Docs: https://yandex.ru/dev/metrika/ru/logs/fields/visits

### Что делает DAG
- Ежедневно (в 05:00 UTC) загружает метрики за **предыдущий день**
- Параметры: date,country,city,language,browser,device,os,hour,visits,users,new_users,page_views,update_at
- Данные сохраняются в S3 в формате **Parquet**

### Режимы работы

#### 🟢 Обычный режим (по расписанию)
- Загружается один день (`ds`)
- Создаётся файл: yandex_YYYY-MM-DD.parquet

#### 🟣 Ретро-режим (ручной запуск)
- Запускается через **Trigger DAG with config**
- Загружает данные за указанный диапазон дат
- Создаётся один файл с префиксом `history`:

Пример конфига:
```json
{
"start_date": "2026-01-01",
"end_date": "2026-01-10"
}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import os
import requests
import pandas as pd
import datetime
import pyarrow


def api_download(**context):

    ti = context["ti"]

    ds = context["ds"]
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    load_date = datetime.date.today()
    start_date = conf.get("start_date", ds)
    end_date = conf.get("end_date", ds)

    TOKEN = "y0__xDe_Im4BBiirjkg2Zzs_xP3srDDcLIeER6ns2I71_wDiFmYXg"
    COUNTER_ID = 103580753

    headers = {
        "Authorization": f"OAuth {TOKEN}"
    }

    metrics = [
        "ym:s:visits",
        "ym:s:users",
        "ym:s:newUsers",
        "ym:s:pageviews"
    ]

    # Измерения:
    dimensions = [
        "ym:s:date",
        "ym:s:regionCountry",
        "ym:s:regionCity",
        "ym:s:browserLanguage", 
        "ym:s:browser",  # Наименование браузера
        "ym:s:deviceCategory",  # Тип устройства (desktop/mobile/tablet)
        "ym:s:operatingSystem",  # Операционная система
        "ym:s:hour",
    ]

    params = {
        "date1": start_date,
        "date2": end_date,
        "ids": COUNTER_ID,
        "metrics": ",".join(metrics),
        "dimensions": ",".join(dimensions)
    }

    url = "https://api-metrika.yandex.net/stat/v1/data"

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    dimension_names = data["query"]["dimensions"]
    metric_names = data["query"]["metrics"]

    # Строим список строк
    rows = []
    for row in data["data"]:
        dim_values = [d["name"] for d in row["dimensions"]]
        metrics = row["metrics"]
        rows.append(dim_values + metrics)

    # Названия колонок
    columns = dimension_names + metric_names

    # Финальный DataFrame
    df = pd.DataFrame(rows, columns=columns)
    df["update_at"] = load_date

    if is_history:
        filename = f"avpalatov/yandex/yandex_history_{start_date}_{end_date}.parquet"
    else:
        filename = f"avpalatov/yandex/yandex{ds}.parquet"


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
    
    print(f"==== Загружено {len(df)} строк за период {start_date} — {end_date}")


default_args = {
    "owner": "avpalatov",
    "start_date": days_ago(1),
    "retries": 3,
}

dag = DAG(
    dag_id="avpalatov_API_to_S3_yandex",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=False,
    description="Запуск каждый день в 05:00 UTC",
    tags=["yandex", "metrika", "visits", "api", "s3"],
)


upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

# send_email = EmailOperator(
#     task_id="send_email",
#     to="palatoff@yandex.ru",
#     subject="Yandex parquet загружен за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
#     html_content="""
#         <p>Файл <b>yandex_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
#         <p>Путь: <code>s3://dev/avpalatov/yandex/yandex_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
#         <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
#     """,
#     conn_id="smtp_avpalatov_yandex",
#     dag=dag,
# )

# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="avpalatov_tg",
    chat_id="{{ var.value.AVPALATOV_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Яндекс метрика за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>yandex_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

dag.doc_md = __doc__

upload >> [send_message_telegram]