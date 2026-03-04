from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import io
import pandas as pd
import requests


def api_download(**context):
    load_date = context["ds"]
    ti = context["ti"]

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    params = {
        "format": "geojson",
        "starttime": f"{load_date}T00:00:00",
        "endtime": f"{load_date}T23:59:59",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    features = data["features"]

    earthquake = []
    for feature in features:
        props = feature["properties"]
        geom = feature["geometry"]

        earthquake.append(
            {
                "id": feature["id"],
                "time": datetime.fromtimestamp(props['time'] / 1000.0).strftime("%Y-%m-%d %H:%M:%S"),
                "latitude": geom["coordinates"][0],
                "longtitude": geom["coordinates"][1],
                "depth": geom["coordinates"][2],
                "place": props["place"],
                "mag": props["mag"],
                "magType": props["magType"],
                "tsunami": props["tsunami"],
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )

    df = pd.DataFrame(earthquake)
    filename = f"vadng/API_to_S3/earthquake_{load_date}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",
            replace=True,
        )

    total_rows = len(df)
    print(f"Данные загружены за {load_date}")
    print(f"Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
    "owner": "vadng",
    "start_date": days_ago(10),
    "retries": 2,
}

dag = DAG(
    dag_id="vadng__API_to_S3__earthquake",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=True,
    description="API earthquake to S3",
    tags=["earthquake", "api", "s3"],
)

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)
upload
