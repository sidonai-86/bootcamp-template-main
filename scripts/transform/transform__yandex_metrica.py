import sys
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import clickhouse_connect
import boto3

# инициализация спарк
spark = (
    SparkSession.builder.appName("YandexMetricaProcessor")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# инициализация s3
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)

bucket = "dev"

# инициализация  CH
jdbc_url = "jdbc:clickhouse://clickhouse01:8123"
db_host = "clickhouse01"
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
database = "tarasova_ym"
cluster_name = "company_cluster"

client = clickhouse_connect.get_client(
    host=db_host,
    port=8123,
    username=db_user,
    password=db_password,
)

# --------Подготовка данных для витрин-----------


def create_daily_metrics(df):
    """Витрина 1: Ежедневные метрики"""
    daily_metrics_df = df.groupBy(
        to_date(col("ym:s:dateTime")).alias("business_date")  # дата
    ).agg(
        countDistinct("ym:s:clientID").alias("dau"),  # DAU
        count("ym:pv:pageViewID").alias(
            "pageviews"
        ),  # общее кол-во просмотренных страниц
        round(avg("ym:s:visitDuration"), 2).alias(
            "avg_duration"
        ),  # средняя продолжительность сеанса
        round(
            (sum(when(col("ym:s:pageViews") == 1, 1).otherwise(0)) / count("*")), 2
        ).alias(
            "bounce_rate"  # кол-во "отказных" визитов
        ),
    )

    return daily_metrics_df


def create_traffic_sources(df):
    """Витрина 2: Источники трафика"""
    traffic_sources_df = (
        df.withColumn(
            "source",
            coalesce(
                col("ym:s:lastsignReferalSource"), lit("unknown")
            ),  # откуда юзер перешёл
        )
        .groupBy(to_date(col("ym:s:dateTime")).alias("business_date"), "source")
        .agg(
            countDistinct("ym:s:visitID").alias(
                "sessions"
            ),  # кол-во визитов по каждому источнику
            sum(when(col("ym:s:isNewUser") == "1", 1).otherwise(0)).alias(
                "new_users"
            ),  # кол-во новых юзеров по источнику
            sum(when(col("ym:s:isNewUser") == "0", 1).otherwise(0)).alias(
                "returning_users"
            ),  # кол-во повторных юзеров
        )
    )

    return traffic_sources_df


def create_new_vs_returning(df):
    """Витрина 2: Источники трафика"""
    new_vs_returning_df = df.groupBy(
        to_date(col("ym:s:dateTime")).alias("business_date")
    ).agg(
        sum(when(col("ym:s:isNewUser") == "1", 1).otherwise(0)).alias(
            "new_users"
        ),  # новые юзеры за день
        sum(when(col("ym:s:isNewUser") == "0", 1).otherwise(0)).alias(
            "returning_users"
        ),  # повторные юзеры за день
    )

    return new_vs_returning_df


def create_geo_stats(df):
    """Витрина 3: Гео-статистика"""
    geo_stats_df = (
        df.withColumn(
            "country", coalesce(col("ym:s:regionCountry"), lit("unknown"))
        )  # страна
        .withColumn("city", coalesce(col("ym:s:regionCity"), lit("unknown")))  # город
        .groupBy(
            to_date(col("ym:s:dateTime")).alias("business_date"),
            "country",
            "city",  # дата
        )
        .agg(
            countDistinct("ym:s:visitID").alias(
                "sessions"
            ),  # кол-во визитов в городе за день
            countDistinct("ym:s:clientID").alias("dau"),
        )  # ДАУ
    )

    return geo_stats_df


def create_devices(df):
    """Витрина 4: Устройства"""
    device_mapping_expr = create_map(
        [lit(x) for x in ["1", "desktop", "2", "tablet", "3", "mobile"]]
    )

    devices_df = (
        df.withColumn(
            "device_category",  # тип устройства
            coalesce(
                device_mapping_expr[col("ym:pv:deviceCategory")],
                col("ym:pv:deviceCategory"),
                lit("unknown"),
            ),
        )
        .withColumn(
            "browser", coalesce(col("ym:pv:browser"), lit("unknown"))
        )  # браузер
        .withColumn("os", coalesce(col("ym:pv:operatingSystem"), lit("unknown")))  # OC
        .groupBy(
            to_date(col("ym:s:dateTime")).alias("business_date"),
            "device_category",
            "browser",
            "os",
        )
        .agg(
            countDistinct("ym:s:visitID").alias("sessions")
        )  # кол-во визитов в день с OC, браузер, устройства
        .orderBy("device_category", "browser", desc("os"))
    )

    return devices_df


def create_top_pages(df):
    """Витрина 5: Популярные страницы"""
    top_pages_df = (
        df.withColumn("url", coalesce(col("ym:s:startURL"), lit("unknown")))
        .groupBy(to_date(col("ym:s:dateTime")).alias("business_date"), "url")
        .agg(
            countDistinct("ym:pv:pageViewID").alias(
                "pageviews"
            ),  # кол-во просмотров страницы
            round(avg("ym:s:visitDuration"), 2).alias(
                "avg_duration"
            ),  # средняя длительность пребывания на странице
        )
    )

    return top_pages_df


def create_session_depth(df):
    """Витрина 6: Глубина сессии"""
    session_depth_df = df.groupBy(
        to_date(col("ym:s:dateTime")).alias("business_date")
    ).agg(
        round(avg("ym:s:pageViews"), 2).alias("avg_pageviews"),
        max("ym:s:pageViews").alias("max_pageviews"),
        round(
            (sum(when(col("ym:s:pageViews") == 1, 1).otherwise(0)) / count("*")), 2
        ).alias("bounce_rate"),
    )

    return session_depth_df


# --------Функция записи данных в БД ClickHouse---------


def write_to_clickhouse(df, distributed_table_name):
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", f"tarasova_ym.{distributed_table_name}")
        .mode("append")
        .save()
    )


# Получаем пары файлов за каждую бизнес-дату из переменной окружения
file_pairs_json = os.getenv("FILE_PAIRS_JSON")
if not file_pairs_json:
    print("ERROR: FILE_PAIRS_JSON environment variable not set")

file_pairs = json.loads(file_pairs_json)


# -----------Трансформация данных, запись в БД-----------
for date_str, files_tuple in file_pairs.items():
    df_visits_expanded = None
    df_hits = None

    # Сначала читаем оба файла
    for file in files_tuple:
        if "visits" in file:
            df_visits = spark.read.parquet(f"s3a://{bucket}/{file}")
            df_visits.cache()  # ✅ Раскомментировать cache

            # Обработка visits
            df_visits_expanded = (
                df_visits.withColumn(
                    "watchIDs_str", regexp_replace(col("ym:s:watchIDs"), "[\\[\\]]", "")
                )
                .withColumn("watch_id", explode(split(col("watchIDs_str"), ",")))
                .withColumn("watch_id", trim(col("watch_id")))
                .withColumn("watch_id", col("watch_id").cast("long"))
                .drop("watchIDs_str")
            )
            df_visits_expanded.cache()  # ✅ Кэшируем результат

        elif "hits" in file:
            df_hits = spark.read.parquet(f"s3a://{bucket}/{file}")
            df_hits.cache()

    # ✅ ПРОВЕРКА: оба файла должны быть прочитаны
    if df_visits_expanded is None or df_hits is None:
        print(
            f"❌ Missing data for date {date_str}. Visits: {df_visits_expanded is not None}, Hits: {df_hits is not None}"
        )
        continue

    try:
        # Джойним hits c visits по clientID и watchID
        df_joined = df_visits_expanded.join(
            df_hits,
            (
                (df_hits["ym:pv:clientID"] == df_visits_expanded["ym:s:clientID"])
                & (df_hits["ym:pv:watchID"] == df_visits_expanded["watch_id"])
            ),
            "left",  # ✅ Изменил на inner чтобы избежать null в результатах
        )

        # Кэшируем joined результат для повторного использования
        df_joined.cache()

        # Создаем и сохраняем витрины
        print(f"Creating datamarts for date {date_str}")

        write_to_clickhouse(create_daily_metrics(df_joined), "daily_metrics_distr")
        write_to_clickhouse(create_traffic_sources(df_joined), "traffic_sources_distr")
        write_to_clickhouse(create_geo_stats(df_joined), "geo_stats_distr")
        write_to_clickhouse(create_top_pages(df_joined), "top_pages_distr")
        write_to_clickhouse(
            create_new_vs_returning(df_joined), "new_vs_returning_distr"
        )
        write_to_clickhouse(create_session_depth(df_joined), "session_depth_distr")
        write_to_clickhouse(create_devices(df_joined), "devices_distr")

        # ✅ Очищаем кэш после обработки
        df_joined.unpersist()
        df_visits_expanded.unpersist()
        df_hits.unpersist()

        # Запись в служебную таблицу информации об обработанных файлах
        for file in files_tuple:
            client.command(
                f"""
                INSERT INTO {database}.processed_files (file_name, business_date)
                VALUES ('{file}', toDate('{date_str}'))
                """
            )
        print(f"✅ Successfully processed date {date_str}")

    except Exception as e:
        print(f"❌ Error processing date {date_str}: {e}")
        # ✅ Очищаем кэш даже при ошибке
        if "df_joined" in locals():
            df_joined.unpersist()
        if "df_visits_expanded" in locals():
            df_visits_expanded.unpersist()
        if "df_hits" in locals():
            df_hits.unpersist()
