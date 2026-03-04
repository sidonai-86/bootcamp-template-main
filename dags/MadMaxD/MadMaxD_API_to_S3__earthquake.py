from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.email import EmailOperator
#from airflow.providers.telegram.operators.telegram import TelegramOperator
#from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests
import csv
from datetime import datetime, timedelta
from tqdm import tqdm
import os
from airflow.utils.dates import days_ago


def api_download(**context):  
    
   print(context['dag_run'].conf)
   url_table = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
   url_count = 'https://earthquake.usgs.gov/fdsnws/event/1/count'
   start_date = context["params"]['dis']
   end_date = context["params"]['die']
   print(start_date, end_date)
   format = "csv"
   start_date_range, end_time_range = "", ""
   
   
   delta = datetime.strptime(end_date, "%Y-%m-%d")- datetime.strptime(start_date, "%Y-%m-%d")
   
   for i in range(1, delta.days + 1):
       start_date_range, end_time_range =(datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days = i - 1)).strftime("%Y-%m-%d"), (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days = i)).strftime("%Y-%m-%d")
       
       response_table = requests.get(url=url_table,
                                  params={"starttime": start_date_range
                                          , "endtime" : end_time_range,
                                          "format": format})
       response_count = requests.get(url=url_count, 
                                  params={"starttime": start_date_range, 
                                          "endtime" : end_time_range, 
                                          "format": format})
       
       
       list_res_table = response_table.content.decode('utf-8').splitlines()
       count_res = response_count.content.decode("utf-8")
       list_res_sep = list(csv.reader(list_res_table, delimiter=','))
       df = pd.DataFrame(columns=list_res_sep[0], data=list_res_sep[1:])
       if response_table.status_code == 200 and response_count.status_code == 200:
           if len(df) == int(count_res) and len(df) > 0: 
               df['update_at'] =  pd.to_datetime(pd.Timestamp.now()).date()
               df['date_report'] = pd.to_datetime(df['time']).dt.date
               buffer = io.BytesIO()
               df.to_parquet(buffer, index=False)
               buffer.seek(0)  # возвращаемся в начало буфера
               path_folder_s3 = f'MadMaxD/eartquake/{start_date_range}.parquet'
               hook = S3Hook(aws_conn_id="minios3_conn")
               
               hook.load_bytes(
                   bytes_data=buffer.read(), key=path_folder_s3, 
                   bucket_name="dev", replace=True
               )
               print(f"==== Данные по погоде загружены за {start_date_range}")
               print(f"==== Кол-во строк {len(df)}")
           elif len(df) != int(count_res):
               print(f"Файл {start_date_range}.parquet количество строк query != count")
           elif len(df) == 0:
               print(f"Файл {start_date_range}.parquet пустой")
       else:
            print(response_table.status_code, response_count.status_code)
default_args = {
    "owner": "MadMaxD",
    "start_date": (datetime.now() - timedelta(days=1)),
    "retries": 2,
    "params":{
        "dis": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "die": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    }

}

dag = DAG(
    dag_id="MadMaxD__API_to_S3__earthquake",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=False,
    description="API earthquake to S3",
    tags=["earthquak", "MadMaxD", "api", "s3"],
) 

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

upload      