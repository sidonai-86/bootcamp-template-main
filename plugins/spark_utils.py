import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("spark-utils")


def get_spark_session(app_name: str) -> SparkSession:
    """
    Создает и возвращает SparkSession с предустановленными пакетами.

    Параметры:
    app_name: str - Название Spark-приложения, которое будет отображаться в Spark UI.

    Возвращает:
    SparkSession - Инициализированная SparkSession с необходимыми зависимостями.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2",
            "org.postgresql:postgresql:42.5.0",
        ]))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    logger.info(f"SparkSession создан: {app_name}")
    return spark


def get_creds(dbname: str, table: str, platform: str) -> dict:
    """
    Формирует JDBC-конфигурацию для подключения к PostgreSQL или ClickHouse.

    Параметры:
    dbname: str - Название базы данных или схемы.
    table: str - Имя таблицы, к которой нужно подключиться.
    platform: str - Тип платформы — 'PG' (PostgreSQL) или 'CH' (ClickHouse).

    Возвращает:
    Словарь с параметрами подключения
    """
    db_configs = {
        "PG": {
            "url": f"jdbc:postgresql://postgres_source:5432/{dbname}",
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "table": table,
            "driver": "org.postgresql.Driver",
        },
        "CH": {
            "url": f"jdbc:clickhouse://clickhouse01:8123/{dbname}",
            "user": os.getenv("CLICKHOUSE_USER"),
            "password": os.getenv("CLICKHOUSE_PASSWORD"),
            "table": f"{dbname}.{table}",
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        },
    }

    if platform not in db_configs:
        raise ValueError(f"Неизвестная платформа: {platform}. Используй 'PG' или 'CH'.")

    creds = db_configs[platform]
    if not creds["user"] or not creds["password"]:
        raise ValueError(f"Не найдены креды для {platform}. Проверь env переменные.")

    return creds


def load_table(spark, dbname: str, table: str, platform: str):
    """
    Загружает таблицу из PostgreSQL или ClickHouse в Spark DataFrame.

    Параметры:
    spark: SparkSession - Активная сессия Spark.
    dbname: str - Название базы данных.
    table: str - Имя таблицы.
    platform: str - Платформа подключения: 'PG' или 'CH'.

    Возвращает:
    pyspark.sql.DataFrame - DataFrame с загруженными данными из указанной таблицы.
    """
    creds = get_creds(dbname, table, platform)
    logger.info(f"Чтение: {platform} -> {creds['table']} -> ({creds['url']})")
    return (
        spark.read.format("jdbc")
        .option("url", creds["url"])
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("dbtable", creds["table"])
        .option("driver", creds["driver"])
        .load()
    )


def write_table(df, dbname: str, table: str, platform: str, mode: str = "append") -> None:
    """
    Сохраняет Spark DataFrame в таблицу PostgreSQL или ClickHouse.

    Параметры:
    ----------
    df: pyspark.sql.DataFrame - DataFrame, который нужно записать в базу данных.
    dbname:str - Название базы данных или схемы.
    table: str - Имя таблицы для записи.
    platform:str - Тип платформы — 'PG' или 'CH'.
    mode: str - Режим записи: 'append' (по умолчанию), 'overwrite', 'ignore', 'error'.
    """
    creds = get_creds(dbname, table, platform)
    logger.info(f"Запись: {platform} -> {creds['table']} -> ({mode})")
    (
        df.write.format("jdbc")
        .option("url", creds["url"])
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("dbtable", creds["table"])
        .option("driver", creds["driver"])
        .mode(mode)
        .save()
    )
    logger.info(f"Данные записаны в {creds['table']}")