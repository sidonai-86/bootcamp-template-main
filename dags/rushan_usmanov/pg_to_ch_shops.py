from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring
import os


def pg_to_ch_shops():
    # ---------- Spark ----------
    spark = (
        SparkSession.builder
        .appName("PG_TO_CH_SHOPS")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.0,"
            "com.clickhouse:clickhouse-jdbc:0.6.0"
        )
        .getOrCreate()
    )

    # ---------- Postgres ----------
    pg_url = "jdbc:postgresql://postgres_source:5432/source"
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")

    shops_df = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "public.shops")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(
            col("st_id").cast("long").alias("id"),
            col("shop_name")
        )
    )

    shop_tz_df = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "public.shop_timezone")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(
            col("plant").cast("long").alias("id"),
            col("time_zone")
        )
    )

    # ---------- Transform ----------
    final_df = (
        shops_df
        .join(shop_tz_df, on="id", how="inner")
        .withColumn(
            "tz_code",
            when(col("time_zone") == "RUS04", 4)
            .when(col("time_zone").isNull() | (col("time_zone") == ""), 3)
            .otherwise(substring(col("time_zone"), -2, 2).cast("int"))
        )
        .select(
            col("id"),
            col("shop_name"),
            col("tz_code").cast("int")
        )
    )

    final_df.printSchema()

    # ---------- ClickHouse ----------
    ch_url = "jdbc:clickhouse://clickhouse01:8123/usmanov_rr"
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_table = "shops_local"

    (
        final_df
        .repartition(8)
        .write
        .format("jdbc")
        .option("url", ch_url)
        .option("dbtable", ch_table)
        .option("user", ch_user)
        .option("password", ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", 50000)
        .mode("append")
        .save()
    )

    print("✅ PG → CH shops залито успешно")
    spark.stop()


# ---------- DAG ----------
default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="pg_to_ch_shops_inline",
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["spark", "postgres", "clickhouse"],
) as dag:

    PythonOperator(
        task_id="pg_to_ch_shops",
        python_callable=pg_to_ch_shops
    )
