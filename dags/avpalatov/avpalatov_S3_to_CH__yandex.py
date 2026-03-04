"""
# DAG avpalatov_S3_to_CH__yandex «date,country,city,language,browser,device,os,hour,visits,users,new_users,page_views,update_at»

## Загрузка метрик Яндекса из S3 в ClickHouse

### Что делает DAG
- Ежедневно (в 05:00 UTC) загружает в ClickHouse метрики за **предыдущий день**
- Ожидает выполнения DAG avpalatov_API_to_S3_yandex
- Поляы: date,country,city,language,browser,device,os,hour,visits,users,new_users,page_views,update_at
- Данные сохраняются в таблицу avpalatov.yandex_metrics

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
from airflow.providers.telegram.hooks.telegram import TelegramHook
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import os
from pyspark.sql import SparkSession
from datetime import timedelta
import datetime

def read_yandex_s3(**context):

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

    spark = SparkSession.builder \
    .appName("SparkExample2") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ) \
    .getOrCreate()

    # Подключение к Spark
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Устанавливаем таймауты и keep-alive как числа (без 's')
    # Значения в секундах или миллисекундах (зависит от версии, обычно keepalivetime в сек)
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60") 
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.attempts.maximum", "10")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
    hadoop_conf.set("fs.s3a.readahead.range", "65536")

    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # Чтение parquet-файла
    s3_path_yandex = "s3a://dev/avpalatov/yandex/"
    # df = spark.read.parquet(s3_path_yandex)

    paths = []
    
    if is_history:
        # History файл: yandex_history_2026-01-28_2026-01-28.parquet
        filename = f"yandex_history_{start_date}_{end_date}.parquet"
        path = f"{s3_path_yandex}{filename}"
        paths.append(path)
        print(f"History mode: {path}")
    else:
        # Daily файл: yandex20260131.parquet
        filename = f"yandex{ds}.parquet"
        path = f"{s3_path_yandex}{filename}"
        paths.append(path)
        print(f"Daily mode: {path}")
    
    # Чтение файла(ов)
    df = spark.read.parquet(*paths)

    if not df.isEmpty():
        renamed_df = (df
                    .withColumnRenamed("ym:s:date", "date")
                    .withColumnRenamed("ym:s:regionCountry", "country")
                    .withColumnRenamed("ym:s:regionCity", "city")
                    .withColumnRenamed("ym:s:browserLanguage", "language")
                    .withColumnRenamed("ym:s:browser", "browser")
                    .withColumnRenamed("ym:s:deviceCategory", "device")
                    .withColumnRenamed("ym:s:operatingSystem", "os")
                    .withColumnRenamed("ym:s:hour", "hour")
                    .withColumnRenamed("ym:s:visits", "visits")
                    .withColumnRenamed("ym:s:users", "users")
                    .withColumnRenamed("ym:s:newUsers", "new_users")
                    .withColumnRenamed("ym:s:pageviews", "page_views")
        )
    
        # ⬇️ Параметры подключения к CLICKHOUSE
        jdbc_url = 'jdbc:clickhouse://clickhouse01:8123/avpalatov'
        db_user = os.getenv('CLICKHOUSE_USER')
        db_password = os.getenv('CLICKHOUSE_PASSWORD')
        table_name = 'yandex_metrics'

        renamed_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", table_name) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("truncate", "true") \
            .mode("append") \
            .save()

    print("Таблица сохранена в Clickhouse")

    total_rows = df.count()

    ti.xcom_push(key="load_date", value=start_date)
    ti.xcom_push(key="total_rows", value=total_rows)
    
    print(f"==== Загружено {df.count()} строк за период {start_date} — {end_date}")

    spark.stop()

def send_telegram_with_xcom(**context):
    """Отправка Telegram с XCom значениями"""
    ti = context['ti']
    
    load_date = ti.xcom_pull(task_ids='load_yandex_s3', key='load_date') or 'N/A'
    total_rows = ti.xcom_pull(task_ids='load_yandex_s3', key='total_rows') or 0
    
    # ✅ 1. Получаем chat_id из Variable (НЕ Jinja!)
    chat_id = Variable.get("AVPALATOV_TELEGRAM_CHAT_ID")
    
    # ✅ 2. Короткое сообщение
    message = (
        f"✅ Яндекс метрика загружена из S3 в ClickHouse\n"
        f"Дата: {load_date}\n"
        f"Строк: {total_rows:,}"
    )
    
    hook = TelegramHook(telegram_conn_id="avpalatov_tg")
    
    api_params = {
        "chat_id": chat_id,           # ✅ Число/строка, НЕ Jinja
        "text": message,              # ✅ < 4096 символов
        "parse_mode": "HTML"          # ✅ HTML теги
    }
    
    print(f"Отправляем в chat_id={chat_id}, сообщение: {message}")
    hook.send_message(api_params=api_params)


default_args = {
    "owner": "avpalatov",
    "start_date": days_ago(1),
    "retries": 3,
}

dag = DAG(
    dag_id="avpalatov_S3_to_CH__yandex",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=False,
    description="Запуск каждый день в 05:00 UTC",
    tags=["yandex", "metrika", "ClickHouse", "Spark", "s3"],
)

wait_for_upload = ExternalTaskSensor(
    task_id='wait_for_dag_upload',
    external_dag_id='avpalatov_API_to_S3_yandex',   # ← ID первого DAG
    external_task_id='upload',                      # ← Конкретная задача
    execution_delta=timedelta(hours=0),             # ← Смещение расписания
    timeout=7200,                                   # 2 часа максимум
    mode='reschedule',                              # Освобождает слот
    allowed_states=['success'],                     # Только success
    failed_states=['failed'],                       # Прерывать при failed
    allow_nested_operators=True,
    dag=dag
)

upload = PythonOperator(
    task_id="load_yandex_s3",
    python_callable=read_yandex_s3,
    dag=dag
)

# https://api.telegram.org/bot<YourBOTToken>/getUpdates
# send_message_telegram = TelegramOperator(
#     task_id="send_message_telegram",
#     telegram_conn_id="avpalatov_tg",
#     chat_id="{{ var.value.AVPALATOV_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
#     text=(
#         "✅ Яндекс метрика за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
#         "загружена из S3 в ClickHouse\n"
#         "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
#     ),
#     dag=dag,
# )

send_message_telegram = PythonOperator(
    task_id="send_message_telegram",
    python_callable=send_telegram_with_xcom,
    provide_context=True,
    dag=dag
)

dag.doc_md = __doc__

wait_for_upload >> upload >> send_message_telegram