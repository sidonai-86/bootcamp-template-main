"""
# DAG sid998800_API_to_S3__news «(author, title, description, url, urlToImage, publishedAt)»

## Загрузка новостей по заданному ключевому слова на выбранномй языке
API Docs: https://newsapi.org/docs

### Что делает DAG
- Ежедневно (в 02:00 UTC) загружает новости по заданному ключевому слову query
- Параметры: author, title, description, url, urlToImage, publishedAt
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
from airflow.models import Variable

import io
import pandas as pd
import requests
import pyarrow
from datetime import datetime, timedelta

# ваше ключевое слово для поиска новости переменная в Airflow
QUERY = Variable.get("NEWS_QUERY")


def api_download(**context):

    query = QUERY
    context["task_instance"].xcom_push(key="query", value=query)
    ti = context["ti"]

    ds = context["ds"]  # Текущая дата выполнения
    prev_ds = context.get("prev_ds")  # Предыдущая дата
    next_ds = context.get("next_ds")  # Следующая дата

    # Получаем ключ из переменных Airflow
    news_api = Variable.get("NEWS_API_KEY")

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds)  # Дата начала поиска
    end_date = conf.get("end_date", ds)  # Текущая дата

    url = "https://newsapi.org/v2/everything"

    # Параметры поиска
    params = {
        "q": query,
        "from": start_date,
        "to": end_date,
        "sortBy": "popularity",
        "language": "ru",
        "apiKey": news_api,
    }

    # Отсылаем get request запрос на сервер с заданными ранее параметрами
    response = requests.get(url, params)

    # Проверяем ошибки
    response.raise_for_status()

    # Передаем ответ от сервера в формате json
    data = response.json()

    df = pd.json_normalize(data["articles"])

    # Добавляем колонку с текущий временем
    df["updated_at"] = datetime.now()

    # print("Тип столбца author:", df['author'].dtype)
    # print("Уникальные значения:", df['author'].unique())
    # print("Пропущенные значения:", df['author'].isna().sum())

    # print("Примеры значений:")
    # for i, val in enumerate(df['author'].head()):
    #     print(f"  {i}: {repr(val)} (тип: {type(val)})")

    # Создаем колонку 'author' если её нет
    if "author" not in df.columns:
        df["author"] = None  # или 'Неизвестно'

    # Замена пустых строк в колонке
    df["author"] = df["author"].fillna("Неизвестно").replace("", "Неизвестно")

    # # Удалил колонку, т.к. в ней дубли из колонки source.name
    # del df["source.id"]

    # Преобразуем строку даты в datetime
    if "publishedAt" in df.columns:
        df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce")
    else:
        print("⚠️ ВНИМАНИЕ: Колонка 'publishedAt' отсутствует в данных!")
        # Создаем пустую колонку
        df["publishedAt"] = pd.NaT

    # Преобразуем строку даты в строки
    df["publishedAt"] = df["publishedAt"].astype(str)

    # Сортируем по дате публикации
    df = df.sort_values(by="publishedAt", ascending=False)

    # Генерируем имя файла
    if is_history:
        filename = f"sid998800/api/news_history_{QUERY}_{start_date}_{end_date}.parquet"
    else:
        filename = f"sid998800/api/news_actual_{QUERY}_{ds}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
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

    print(f"==== Загружено {len(df)} строк за период {ds} — {ds}")


default_args = {
    "owner": "sid998800",
    "start_date": days_ago(7),
    "retries": 2,
}

dag = DAG(
    dag_id=f"sid998800__API_to_S3__news_{QUERY}",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # каждый день в 03:00 UTC
    catchup=True,
    description="API news to S3",
    tags=["news", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

# send_email = EmailOperator(
#     task_id="send_email",
#     to="igor-rayban@yandex.ru",
#     subject="News parquet загружен за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
#     html_content="""
#         <p>Файл <b>news_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
#         <p>Путь: <code>s3://dev/api/sid998800/news_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
#         <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
#     """,
#     conn_id="smtp_sid998800_yandex",
#     dag=dag,

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="sid_tg",
    chat_id="{{ var.value.sid998800_tg_chat_id }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text="""
✅ Новости за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} загружены
Файл: <code>news_{{ ti.xcom_pull(task_ids='upload', key='query') }}_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>
Запрос: <b>{{ ti.xcom_pull(task_ids='upload', key='query') }}</b>
Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>
""".strip(),
    dag=dag,
)


upload >> send_message_telegram
