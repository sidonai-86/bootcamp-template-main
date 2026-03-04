from format_strategies import FORMAT_STRATEGIES
from db_utils import S3MaxDateManager

from datetime import timedelta, date, datetime
import requests
import logging
from io import StringIO, BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from airflow.exceptions import AirflowException

logger = logging.getLogger("airflow.task")

def earthquake_api_to_s3(starttime, endtime, config):

    serialize_strategy_name = config["strategy_name"]

    if serialize_strategy_name not in FORMAT_STRATEGIES:
        raise ValueError(
            f"Format '{serialize_strategy_name}' is not supported. Use: {list(FORMAT_STRATEGIES.keys())}"
        )

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    load_date = starttime
    params = {
        "format": "csv",
        "starttime": starttime,
        "endtime": endtime,
    }

    logger.info(f"📡 Загрузка данных за {load_date}")

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = pd.read_csv(StringIO(response.text))

    data["updated_at"] = pd.to_datetime(datetime.now()).floor('us')
    data["time"] = pd.to_datetime(data["time"]).dt.floor('us')
    data["updated"] = pd.to_datetime(data["updated"]).dt.floor('us')



    if len(data) == 0:
        logger.info(f"🔍 Нет событий за {load_date}")
        return

    output_buffer = BytesIO()

    # Применяем выбранную стратегию сохранения
    strategy_settings = FORMAT_STRATEGIES[serialize_strategy_name]
    save_func = strategy_settings["func"]
    save_func(data, output_buffer)

    # Возвращаем курсор в начало буфера, чтобы читать из него
    output_buffer.seek(0)

    logger.info(f"✅ Данные за {load_date} сереализованы.")

    # filename = f"dev/alexxxxxxela/earthquake/events_{load_date}.parquet"
    s3_path = f"{config['base_path']}events_{load_date}.{strategy_settings['ext']}"

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_bytes(
        bytes_data=output_buffer.getvalue(),
        key=s3_path,
        bucket_name="dev",
        replace=True,
    )

    if not hook.check_for_key(key=s3_path, bucket_name="dev"):
        raise AirflowException(f"Файл {s3_path} не был создан в S3")

    logger.info(f"✅ Данные за {load_date} загружены в S3.")

    manager = S3MaxDateManager(
        table_name="earthquake",
        init_value=starttime,
        postgres_conn_id="metadata_db",
        postgres_db_schema="alex_b",
    )
    manager.update_max_value(load_date)

    logger.info("✅ Метаданные обновлены.")
