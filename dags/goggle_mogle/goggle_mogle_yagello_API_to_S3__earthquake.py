import requests
import pandas as pd
from datetime import datetime
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def api_download(**context):

    ds = context['ds']
    start_time = f"{ds} 00:00:00"
    end_time   = f"{ds} 23:59:59"

    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'

    response = requests.get(url,
        params={
            'format': 'geojson',
            'starttime': start_time,
            'endtime': end_time
        }
    )

    data = response.json()
    df = pd.json_normalize(data)

    filename = f'goggle_mogle/earthquake/{ds}.parquet'

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id='minios3_conn')
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name='dev',
            replace=True,
        )
    
        print(f'Что-то загрузилось')
        
    else:
        print(f'Ничо не загрузилось')

default_args = {
    'owner': 'goggle_mogle',
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    dag_id='goggle_mogle_yagello_API_to_S3__earthquake',
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup=False
)

upload = PythonOperator(task_id='upload', python_callable=api_download, dag=dag)

trigger_transform_dag = TriggerDagRunOperator(
    task_id="trigger_s3_to_ch",
    trigger_dag_id="goggle_mogle_yagello_S3_to_CH__earthquake",
    execution_date="{{ ds }}",
    wait_for_completion=False,
    dag=dag
)

upload >> trigger_transform_dag
