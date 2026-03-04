from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime


def get_yandex_api(start_date, end_date):
    try:
        import requests
        import pandas as pd
        import io

        TOKEN = Variable.get("yandex-token")
        url = 'https://api-metrika.yandex.net/stat/v1/data'
        COUNTER_ID = 103580753
        headers = {"Authorization": f"OAuth {TOKEN}"}

        metrics = [
            'ym:s:visits',
            'ym:s:newUsers',
            'ym:s:users',
            'ym:s:pageviews'
            ]


        dimensions = [
            "ym:s:date",
            "ym:s:clientID",
            "ym:s:mobilePhone",
            "ym:s:mobilePhoneModel",
            "ym:s:operatingSystem",
            "ym:s:browser",
            "ym:s:regionCountry",
            "ym:s:regionCity",
            "ym:s:dateTime"
        ]

        params = {
            "date1": start_date,
            "date2": end_date,
            "metrics": (',').join(metrics),
            "ids" : COUNTER_ID,
            "dimensions": (',').join(dimensions),
            "limit": 10000,
            "offset": 1
        }

        rows = []
        response = requests.get(url=url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        dimension = data['query']['dimensions']
        metric = data['query']['metrics']

        columns_before_rename = dimension + metric
        columns_after_rename = [col[5:] for col in columns_before_rename] + ['updated_at']

        for row in data['data']:
            dimension_values = [item['name'] for item in row['dimensions']]
            metrics_values = row['metrics']
            now = datetime.now()
            now_c = now.strftime('%Y-%m-%d %H:%M:%S')
            rows.append(dimension_values + metrics_values + [now_c])
        df = pd.DataFrame(data=rows, columns=columns_after_rename)
        values = {'mobilePhone': 'Web', 'mobilePhoneModel': 'Web'}
        df_replace = df.fillna(value=values).rename(columns={'dateTime':'datetime_visit'})
        
        filename = f'SkubaM/visitsByDevice/visitsUsersByDevice_{start_date}_{end_date}.parquet'

        if not df_replace.empty:
            buffer = io.BytesIO()
            df_replace.to_parquet(buffer, index=False)
            buffer.seek(0)
            hook = S3Hook(aws_conn_id="minios3_conn")
            hook.load_bytes(
                bytes_data=buffer.read(),
                key=filename,
                bucket_name="dev",
                replace=True,
            )
        print(f'Данные в {filename} успешно загружены, кол-во строк {len(df_replace)}')
    except requests.exceptions.RequestException as e:
        print(f'Ошибка подключения {e}')


default_args = {
    'owner': 'SkubaM',
    'start_date': datetime(2026, 2, 1),
    'retries': 2,
}

dag = DAG(
    dag_id="SkubaM_API_to_S3_yandex_visits_users_by_device",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=True,
    description="Выгрузка кол-во посетителей и новых пользователь с указанием девайса, операционной системы и страны(города)",
    tags=["yandex", "visits", "users", "browser", "device"],
)

upload = PythonOperator(
    task_id='upload',
    python_callable=get_yandex_api,
    op_kwargs={
        'start_date': '{{ ds }}',
        'end_date': '{{ ds }}'
        },
    dag=dag
    )

upload