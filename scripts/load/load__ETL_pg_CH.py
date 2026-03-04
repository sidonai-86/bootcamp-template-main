import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    PG_JDBC_URL = require_env("POSTGRES_JDBC_URL")      # jdbc:postgresql://host:5432/db
    PG_USER     = require_env("POSTGRES_USER")
    PG_PASS     = require_env("POSTGRES_PASSWORD")

    PG_TABLE_SHOPS       = os.getenv("PG_TABLE_SHOPS", "public.shops")
    PG_TABLE_SHOP_TZ     = os.getenv("PG_TABLE_SHOP_TZ", "public.shop_timezone")

    CH_JDBC_URL = require_env("CLICKHOUSE_JDBC_URL")    # jdbc:clickhouse://clickhouse01:8123/kimarec
    CH_USER     = require_env("CLICKHOUSE_USER")
    CH_PASS     = require_env("CLICKHOUSE_PASSWORD")
    CH_TABLE    = os.getenv("CH_TABLE", "st_timezone_distr2")  # итоговая таблица

    #спарк сессия
    spark = (
            SparkSession
            .builder
            .appName("halltape_pyspark_local")
            .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
                "org.postgresql:postgresql:42.5.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .getOrCreate()
    )

   #Чтение таблиц из PG
    shops_df = (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("dbtable", PG_TABLE_SHOPS)
        .option("fetchsize", 1000)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    shops_df_tmz = (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("dbtable", PG_TABLE_SHOP_TZ)
        .option("fetchsize", 1000)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    #трансформация
    joined_df = shops_df.join(
        shops_df_tmz,
        shops_df.st_id == shops_df_tmz.plant,
        how="left"
    )

    df_with_nulls = (
        joined_df
        .withColumn(
            "time_zone",
            F.when(F.trim(F.col("time_zone")) == "", None).otherwise(F.col("time_zone"))
        )
        .dropDuplicates()
        .where(F.col("plant").isNotNull())
    )

    finall_df = (
        df_with_nulls.groupBy("st_id")
        .agg(
            F.first("shop_name", ignorenulls=True).alias("shop_name"),
            F.first("plant", ignorenulls=True).alias("plant"),
            F.first("time_zone", ignorenulls=True).alias("tz_code"),
        )
    )

    result = finall_df.withColumn(
        "tz_code",
        F.coalesce(F.col("tz_code"), F.lit("RUS03"))
    )

    final_df = (
        result
        .withColumn("tz_code", F.regexp_extract("tz_code", r"(\d+)$", 1).cast("int"))
        .select("st_id", "shop_name", "tz_code")
        .withColumn("updated_at", F.current_date())   # текущая дата на стороне Spark-кластера
    )

    #Сохранение в Кликхаус
    (
        final_df.write.format("jdbc")
        .option("url", CH_JDBC_URL)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", CH_TABLE)
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")  
        .mode("append")  # append предполагает, что таблица уже создана в CH и схема совпадает
        .save()
    )

    print("✅ PG → ClickHouse успешно записано.")
    spark.stop()
