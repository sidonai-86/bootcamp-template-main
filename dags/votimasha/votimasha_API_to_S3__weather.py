# %%
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io


def api_download(**context):

    ds = context['ds']
    e_date = ds
    s_date = ds

    response = requests.get(
        'https://api.open-meteo.com/v1/forecast',
        params={
            "latitude": 62.08,
            "longitude": 35.21,
            "hourly": [
                'temperature_2m',
                'apparent_temperature',
                'rain',
                'snowfall'
            ],
            "timezone": 'auto',
            "start_date": s_date,
            "end_date": e_date
        },
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    hourly = data.get('hourly', {})
    df = pd.DataFrame({
        'date and time': hourly.get('time', []),
        'real temperature': hourly.get('temperature_2m', []),
        'temperature feels like': hourly.get('apparent_temperature', []),
        'rain': hourly.get('rain', []),
        'snow': hourly.get('snowfall', []),
    })

    print('Количество строк:', len(df))

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=f'votimasha/api__weather/kizhi_{ds}.parquet',
            bucket_name="dev",
            replace=True,
        )


default_args = {
    "owner": "votimasha",
    "start_date": days_ago(10),
    "retries": 3,
}

dag = DAG(
    dag_id="votimasha_API_to_S3__weather",
    default_args=default_args,
    schedule_interval="0 11 * * *",
    catchup=True,
    description="Kizhi's weather | API Python >> AirFlow >> S3",
    tags=[
        'weather',
        'kizhi',
        'homework',
        'rain',
        'snow',
        'temperature',
        'api'
    ],
)

upload = PythonOperator(
    task_id="upload",
    python_callable=api_download,
    dag=dag,
)