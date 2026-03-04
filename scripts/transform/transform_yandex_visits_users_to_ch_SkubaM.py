import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from clickhouse_manager import ClickHouseManager


def main():
    spark = SparkSession.builder.appName(
        "transform_yandex_visits_users_to_ch"
    ).getOrCreate()


    jdbc_url = os.environ["CLICKHOUSE_JDBC_URL"]
    ch_host = os.environ["CLICKHOUSE_HOST"]
    ch_user = os.environ.get("CLICKHOUSE_USER", "")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_database = os.environ.get("CLICKHOUSE_DATABASE", "SkubaM")
    cluster = os.environ.get("CLICKHOUSE_CLUSTER", "company_cluster")

    table_name = os.environ["TABLE_NAME"]
    local_table = f"{table_name}_local"

    s3_bucket = os.environ["S3_BUCKET"]
    manifest_key = os.environ["S3_MANIFEST_KEY"]


    ch = ClickHouseManager(
        host=ch_host,
        user=ch_user,
        password=ch_password,
        database=ch_database,
    )

    ch.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{local_table}
    ON CLUSTER '{cluster}'
    (
        date Date,
        browser LowCardinality(String),
        visits UInt64,
        users UInt64,
        updated_at DateTime
    )
    ENGINE = ReplicatedReplacingMergeTree(
        '/clickhouse/tables/{{shard}}/{ch_database}/{local_table}',
        '{{replica}}',
        updated_at
    )
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, browser)
    """)

    ch.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{table_name}
    ON CLUSTER '{cluster}'
    AS {ch_database}.{local_table}
    ENGINE = Distributed(
        '{cluster}', '{ch_database}', '{local_table}', rand()
    )
    """)


    manifest_path = f"s3a://{s3_bucket}/{manifest_key}"
    keys = [
        r["value"].strip()
        for r in spark.read.text(manifest_path).collect()
        if r["value"] and r["value"].strip()
    ]

    parquet_paths = [f"s3a://{s3_bucket}/{k}" for k in keys]

    print("DEBUG parquet sample:", parquet_paths[0])


    df = spark.read.parquet(*parquet_paths)

    df_norm = df.select(
        to_date(col("date")).alias("date"),
        col("browser").cast("string"),
        col("visits").cast("long"),
        col("users").cast("long"),
        to_timestamp(col("update_at")).alias("updated_at"),
    )

    (
        df_norm.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{ch_database}.{table_name}")
        .option("user", ch_user)
        .option("password", ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

    print("✅ DONE")


if __name__ == "__main__":
    main()