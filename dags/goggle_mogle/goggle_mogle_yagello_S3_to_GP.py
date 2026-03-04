import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'goggle_mogle',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='goggle_mogle_s3_to_gp',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    s3_to_gp = SparkSubmitOperator(
        task_id="spark_s3_to_gp_batch",
        application="/opt/airflow/scripts/transform/transform__s3_to_gp_goggle_mogle.py",
        conn_id="spark_default",
        env_vars={
            "GP_HOST": "master",
            "GP_PORT": "5432",
            "GP_DB": "postgres",
            "GP_USER": "gpadmin",
            "GP_PASSWORD": "4r5t6y7u"
        },
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        },
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.5.0"
        )
    )

    def run_update_mart():
        import psycopg2
        
        conn = psycopg2.connect(
            host="master",
            port="5432",
            dbname="postgres",
            user="gpadmin",
            password="4r5t6y7u"
        )
        cur = conn.cursor()
        
        update_mart_sql = """
        -- 1. Удаляем хвост за последний 1 час
        DELETE FROM goggle_mogle.btc_ohlcv_1m 
        WHERE period_start >= (
            SELECT COALESCE(MAX(period_start) - INTERVAL '1 hour', '1970-01-01') 
            FROM goggle_mogle.btc_ohlcv_1m
        );

        -- 2. Вставляем обновленные данные
        INSERT INTO goggle_mogle.btc_ohlcv_1m
        SELECT 
            date_trunc('minute', trade_time) AS period_start,
            symbol,
            (array_agg(price ORDER BY trade_time ASC, trade_id ASC))[1] AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            (array_agg(price ORDER BY trade_time DESC, trade_id DESC))[1] AS close_price,
            SUM(quantity) AS total_volume,
            SUM(amount_usdt) AS total_amount_usdt,
            COUNT(trade_id)::INTEGER AS trades_count
        FROM goggle_mogle.btc_trades_raw
        WHERE date_trunc('minute', trade_time) >= (
            SELECT COALESCE(MAX(period_start), '1970-01-01') 
            FROM goggle_mogle.btc_ohlcv_1m
        )
        GROUP BY 1, 2;
        """
        
        cur.execute(update_mart_sql)
        conn.commit()
        
        cur.close()
        conn.close()
        print("✅ Витрина btc_ohlcv_1m успешно обновлена!")

    update_mart = PythonOperator(
        task_id="update_ohlcv_1m_mart",
        python_callable=run_update_mart
    )

    # 3. Функция для переливки витрины из Greenplum в ClickHouse
    def run_transfer_to_ch():
        import os
        import requests
        
        # Достаем доступы из переменных окружения Airflow (как в твоем Spark-джобе)
        ch_user = os.getenv("CLICKHOUSE_USER", "default")
        ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
        
        ch_url = 'http://clickhouse01:8123/'
        
        # Наш рабочий SQL-запрос
        query = """
        INSERT INTO goggle_mogle.btc_ohlcv_1m
        SELECT * FROM postgresql(
            'master:5432', 
            'postgres', 
            'btc_ohlcv_1m', 
            'gpadmin', 
            '4r5t6y7u',
            'goggle_mogle'
        );
        """
        
        print(f"🚀 Отправляем запрос в ClickHouse по адресу {ch_url}...")
        
        # Отправляем POST-запрос с аутентификацией
        response = requests.post(
            ch_url, 
            data=query.encode('utf-8'),
            auth=(ch_user, ch_password)
        )
        
        if response.status_code != 200:
            raise Exception(f"❌ Ошибка ClickHouse: {response.text}")
            
        print("✅ Данные успешно перелиты в ClickHouse!")

    transfer_to_ch = PythonOperator(
        task_id="transfer_gp_to_ch",
        python_callable=run_transfer_to_ch
    )

    s3_to_gp >> update_mart >> transfer_to_ch