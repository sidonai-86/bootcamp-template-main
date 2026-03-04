from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests
from datetime import datetime


def api_load(**context):

	ds = context["ds"]

	ti = context["ti"]

	start_date = ds
	end_date = ds

	TOKEN = "y0__xDe_Im4BBiirjkg2Zzs_xP3srDDcLIeER6ns2I71_wDiFmYXg"
	COUNTER_ID = 103580753

	headers = {
		"Authorization": f"OAuth {TOKEN}"
	}

	metrics = [
		"ym:s:visits",
		"ym:s:users",
		"ym:s:avgDaysBetweenVisits",
		"ym:s:robotVisits",
		"ym:s:blockedPercentage"
	]

	dimensions = [
		"ym:s:date", 
		"ym:s:gender",
	]

	params = {
		"date1": start_date,
		"date2": end_date,
		"ids": COUNTER_ID,
		"metrics": ",".join(metrics),
		"dimensions": ",".join(dimensions)
	}

	url = "https://api-metrika.yandex.net/stat/v1/data"

	response = requests.get(url, headers=headers, params=params)
	data = response.json()

	dimension_names = data["query"]["dimensions"]
	metric_names = data["query"]["metrics"]

	rows = []
	for row in data["data"]:
		dim_values = [d["name"] for d in row["dimensions"]]
		metrics = row["metrics"]
		rows.append(dim_values + metrics)

	columns = dimension_names + metric_names

	df = pd.DataFrame(rows, columns=columns).sort_values(
		by=["ym:s:date", "ym:s:gender"], ascending=True, ignore_index=True
	)
	df["update_at"] = datetime.now()

	filename = f"snccnmb/api_yandex_metrika/api_YM_{ds}.parquet"

	if len(df) > 0:
		buffer = io.BytesIO()
		df.to_parquet(buffer, index=False)
		buffer.seek(0)

		hook = S3Hook(aws_conn_id="minios3_conn")
		hook.load_bytes(
			bytes_data=buffer.read(), 
			key=filename, 
			bucket_name="dev", 
			replace=True
		)

	total_rows = len(df)
	print(f"==== Данные событий загружены за {start_date}")
	print(f"==== Кол-во строк {total_rows}")

	ti.xcom_push(key="load_date", value=start_date)
	ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
	"owner": "snccnmb",
	"start_date": days_ago(10),
	"retries": 2,
}


dag = DAG(
	dag_id="snccnmb_chatov_API_to_S3_YM",
	default_args=default_args,
	schedule_interval="0 15 * * *",
	catchup=True,
	description="API yandex metrika to S3",
	tags=["yandex", "metrika", "visits", "hits", "api", "s3"],
)


upload = PythonOperator(task_id='upload', python_callable=api_load, dag=dag)


upload
