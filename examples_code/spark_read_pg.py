import os


# ⬇️ Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

shops_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", "public.shops") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

shop_tz_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", "public.shop_timezone") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

# Регистрируем DataFrame-ы как временные вьюхи
shops_df.createOrReplaceTempView("shops")
shop_tz_df.createOrReplaceTempView("shop_timezone")
