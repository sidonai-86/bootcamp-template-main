import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from clickhouse_manager import ClickHouseManager

def main():
    spark = (SparkSession.builder
             .appName("pg_to_ch_shops")
             .getOrCreate())

    # PG
    pg_url = os.environ["PG_JDBC_URL"]
    pg_user = os.environ["PG_USER"]
    pg_password = os.environ["PG_PASSWORD"]

    shops_table = os.environ.get("PG_SHOPS_TABLE", "public.shops")
    tz_table = os.environ.get("PG_TZ_TABLE", "public.shop_timezone")

    shops_df = (spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", pg_url)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("dbtable", shops_table)
        .load()
    )

    shop_timezone_df = (spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", pg_url)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("dbtable", tz_table)
        .load()
    )

    # transform
    w_plant = Window.partitionBy("plant")

    tz_pre = (shop_timezone_df
        .select(
            "plant",
            "time_zone",
            F.count(F.lit(1)).over(w_plant).alias("counts_plant")
        )
    )

    tz_df = (tz_pre
        .withColumn(
            "tz_code_raw",
            F.regexp_extract(F.coalesce(F.col("time_zone"), F.lit("")), r"([+-]?\d+)", 1)
        )
        .withColumn(
            "tz_code",
            F.when(F.col("tz_code_raw") != "", F.col("tz_code_raw").cast("int"))
             .otherwise(F.lit(3))
        )
        .where(
            (F.col("counts_plant") == 1) |
            ((F.col("counts_plant") > 1) & (F.col("time_zone").isNotNull()) & (F.col("time_zone") != ""))
        )
        .select("plant", "tz_code")
    )

    df_final = (shops_df.alias("s")
        .join(tz_df.alias("tz"), F.col("tz.plant") == F.col("s.st_id"), "inner")
        .select(
            F.col("s.st_id").cast("long").alias("st_id"),
            F.col("s.shop_name").cast("string").alias("shop_name"),
            F.col("tz.tz_code").cast("int").alias("tz_code"),
        )
    )

    # ClickHouse
    ch_host = os.environ["CLICKHOUSE_HOST"]
    ch_url = os.environ["CLICKHOUSE_JDBC_URL"]
    ch_user = os.environ.get("CLICKHOUSE_USER", "")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_db = os.environ.get("CLICKHOUSE_DATABASE", "SkubaM")
    cluster = os.environ.get("CLICKHOUSE_CLUSTER", "company_cluster")
    table_name = os.environ.get("TABLE_NAME", "shops")
    local_table = f"{table_name}_local"

    ch = ClickHouseManager(
        host=ch_host,
        user=ch_user,
        password=ch_password,
        database=ch_db,
    )


    ch.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {ch_db}.{local_table}
    ON CLUSTER '{cluster}'
    (
        st_id UInt64,
        shop_name LowCardinality(String),
        tz_code Int8
    )
    ENGINE = ReplicatedMergeTree(
        '/clickhouse/tables/{{shard}}/{ch_db}/{local_table}',
        '{{replica}}'
    )
    ORDER BY (st_id)
    """)

    ch.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {ch_db}.{table_name}
    ON CLUSTER '{cluster}'
    AS {ch_db}.{local_table}
    ENGINE = Distributed(
        '{cluster}', '{ch_db}', '{local_table}', rand()
    )
    """)

    ch.execute_sql(f"TRUNCATE TABLE {ch_db}.{local_table} ON CLUSTER '{cluster}'")

    (df_final.write
        .format("jdbc")
        .option("url", ch_url)
        .option("dbtable", f"{ch_db}.{table_name}")
        .option("user", ch_user)
        .option("password", ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

    print("Данные перезалиты")

if __name__ == "__main__":
    main()