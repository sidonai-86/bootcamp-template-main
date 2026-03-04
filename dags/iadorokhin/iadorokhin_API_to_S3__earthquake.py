from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


import pandas as pd
import requests
import io
from datetime import datetime
import pyarrow



def api_download(**context):

    ds = context["ds"] # {{"ds"}} JINJA date from calendar

    start_date = f"{ds}T00:00:00"
    end_date = f"{ds}T23:59:59"    

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    nickname = "iadorokhin"
    
    update_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    response = requests.get(
        url,
        params={
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date
        }, timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    features = data.get('features', [])
    df = pd.json_normalize(features)

    #filename_csv = f"{nickname}/{nickname}_{ds}.csv"
    filename_parquet = f"{nickname}/{nickname}_{ds}.parquet"

    df["update_at"] = update_at
    df["bisness_date"] = ds

    print(f'qty_rows____{len(df)}')
    
    if not df.empty:
        """buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_string(
            string_data=buffer.getvalue(),
            key=filename_csv,
            bucket_name="dev",
            replace=True
        )"""

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=filename_parquet,
            bucket_name="dev",
            replace=True
        )


default_args = {
    "owner": "iadorokhin",
    "start_date": days_ago(3),
    "retries": 2,
}

dag = DAG(
    dag_id="iadorokhin__API_to_S3__earthquake",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # everyday in 10:00 UTC crone 
    catchup=True,
    description="API earthquake to S3",
    tags=["weather", "temp", "rain", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)












