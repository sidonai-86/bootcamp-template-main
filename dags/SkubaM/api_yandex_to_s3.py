from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from airflow.utils.dates import days_ago


def api_yandex(start_date, end_date, **context):
    import requests
    import pandas as pd
    import io

    TOKEN = "y0__xDe_Im4BBiirjkg2Zzs_xP3srDDcLIeER6ns2I71_wDiFmYXg"
    COUNTER_ID = 103580753

    path = 'tmp/'
    city = ('Moscow','Saint Petersburg')
    dimensions = ['ym:s:date', 'ym:s:browser']

    url = 'https://api-metrika.yandex.net/stat/v1/data/'

    headers = {
        'Authorization': f"OAuth {TOKEN}"
    }


    metrics = [
        'ym:s:visits',
        'ym:s:users'
    ]

    params = {
        'metrics': ','.join(metrics),
        'date1': start_date,
        'date2': end_date,
        'ids': COUNTER_ID,
        'dimensions': ','.join(dimensions),
        'filters': f'ym:s:regionCityName=.{city}'
    }


    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        columns = []
        dimensions_name = data['query']['dimensions']
        metrics_name = data['query']['metrics']
        strip_names = dimensions_name + metrics_name
        for name in strip_names:
            columns.append(name[5:])
        columns.append('update_at')

        rows = []
        for row in data['data']:
            dimension_values = [dim['name'] for dim in row['dimensions']]
            metrics = row['metrics']
            now = datetime.now()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            rows.append(dimension_values + metrics + [now_str])

        df = pd.DataFrame(rows, columns=columns)
        df_sorted = df.sort_values(['date', 'browser'])
        filename = f"SkubaM/msc_spb_visits_history_{start_date}_{end_date}.parquet"

        if not df.empty:
            buffer = io.BytesIO()
            df_sorted.to_parquet(buffer, index=False)
            buffer.seek(0)

            hook = S3Hook(aws_conn_id="minios3_conn")
            hook.load_bytes(
                bytes_data=buffer.read(),
                key=filename,
                bucket_name="dev",
                replace=True,
            )
        print(f'Данные загружены: Кол-во строк {len(df_sorted)}')            
    except EOFError as e:
        print(f'Ошибка: {e}')


default_args = {
    'owner': 'SkubaM',
    'start_date': days_ago(7),
    'retries': 2,
}


dag = DAG(
    dag_id="SkubaM_API_to_S3_yandex_visits",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=True,
    description="Выгрузка кол-во посетителей и новых пользователь в городе Москва и Санкт-Петербург с разбивкой на браузеры",
    tags=["yandex", "visits", "users", "browser"],
)

upload = PythonOperator(
    task_id='upload',
    python_callable=api_yandex,
    op_kwargs={
        'start_date': '{{ ds }}',
        'end_date': '{{ ds }}'
        },
    dag=dag
    )