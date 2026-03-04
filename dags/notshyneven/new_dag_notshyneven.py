from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io


import pandas as pd
import requests
from datetime import datetime, timezone


def api_download(start_date, end_date):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"  # берем url

    response = requests.get(
        url,
        params={
            "format": "geojson",
            "minmagnitude": 5,
            "starttime": start_date,
            "endtime": end_date,
        },
    )

    response.raise_for_status()  # проверяю чтобы не было ошибок

    data = response.json()  # превращает данные в словарь

    features = data.get(
        "features", []
    )  # выбираю все события из словаря и если пустой не даст ошибку

    df = pd.json_normalize(features)  # преобразую в DataFrame

    df["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0)

    print(f"[INFO] Количество загруженных событий: {len(df)}")

    date_str = start_date[:10]

    filename = f"notshyneven/earthquake_{date_str}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)  # загрузка в S3
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",  # в какой папке будет храниться
            replace=True,
        )


default_args = {  # на сайте airflow отобразятся эти данные
    "owner": "notshyneven",  # owner
    "start_date": days_ago(7),  # дата начала календаря
    "retries": 2,  # попытки запуска
}


dag = DAG(  # описание дага
    dag_id="notshyneven_nemov_API_to_S3_earthquake",  # название дага
    default_args=default_args,
    schedule_interval="0 3 * * *",  # каждый день в 03:00 UTC       # расписание запуска кода cron выражение
    catchup=True,  # если True - загрузит данные за прошлый период
    description="Данные о землетрясениях магнитудой >= 5 за последие 7 дней",  # описание возле названия дага
    tags=["magnitude", "5", "api", "s3"],
)

dag.doc_md = r"""
# 🌍 DAG: Землетрясения в S3

**👨‍💻 Автор:** [@notshyneven](https://t.me/notshyneven)  
**📡 Источник:** [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/)

---

## 📝 Описание

- Ежедневно загружает данные о **землетрясениях ≥ 5** и сохраняет в **S3 Parquet**.  
- Динамические даты через **Jinja**: предыдущий день.  
- Файл: `dev/notshyneven/earthquake_YYYY-MM-DD.parquet`  

---

## ⏰ Расписание

- Запуск каждый день в **03:00 UTC** 🕒  
- Начало загрузки: за **последнюю неделю** от `start_date`.

---

## 🛡️ Проверки

- `response.raise_for_status()` для API ✅  
- `data.get("features", [])` — безопасно при пустом ответе  
- DataFrame пустой — файл не загружается 🚫  

---

## 🎯 Вывод

- Надежный DAG для ежедневного импорта землетрясений 🔄  
- Все файлы удобно сортируются по дате 📂  
- Готов для production 🏗️
"""

upload = PythonOperator(
    task_id="upload",
    python_callable=api_download,
    op_kwargs={
        "start_date": "{{ macros.ds_add(ds, -1) }}T00:00:00",
        "end_date": "{{ macros.ds_add(ds, -1) }}T23:59:59",
    },
    dag=dag,
)
# task_id - название функции внутри aiflow

upload
