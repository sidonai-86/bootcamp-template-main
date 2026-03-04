from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import sys
from datetime import datetime, timedelta
import pandas as pd
import os
import time
import requests
from io import StringIO, BytesIO


# -------- Логирование ----------
os.makedirs("/opt/airflow/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/opt/airflow/logs/ym_logs.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


# -------- Класс для работы с Yandex Metrica --------
class ProcessingYM:
    def __init__(
        self,
        token: str,
        counter_id: int,
        bucket: str = "dev",
        s3_prefix: str = "Nataliia_Tarasova",
        aws_conn_id: str = "minios3_conn",
    ):
        self.token = token
        self.counter_id = counter_id
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.headers = {"Authorization": f"OAuth {self.token}"}
        self.aws_conn_id = aws_conn_id
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

    # -------- Скачивание данных из Yandex Metrica --------
    def create_log_request(self, source, date1, date2, fields):
        url = f"https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests"
        payload = {
            "date1": date1,
            "date2": date2,
            "fields": ",".join(fields),
            "source": source,
        }
        r = requests.post(url, headers=self.headers, data=payload, timeout=60)
        r.raise_for_status()
        request_id = r.json()["log_request"]["request_id"]
        logging.info(f"Создан запрос {request_id} для {source} ({date1} - {date2})")
        return request_id

    def wait_for_request(self, request_id):
        url = f"https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}"
        while True:
            r = requests.get(url, headers=self.headers, timeout=60)
            r.raise_for_status()
            status = r.json()["log_request"]["status"]
            logging.info(f"Статус {request_id}: {status}")
            if status == "processed":
                return r.json()["log_request"]["parts"]
            elif status in ["failed", "cleaned"]:
                raise RuntimeError(f"Ошибка: статус {status}")
            time.sleep(10)

    def download_parts(self, request_id, parts, key_cols=None):
        dfs = []
        for part in parts:
            part_num = part["part_number"]
            url = f"https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/part/{part_num}/download"
            r = requests.get(url, headers=self.headers, timeout=60)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text), sep="\t")
            df["update_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dfs.append(df)
        new_data = pd.concat(dfs, ignore_index=True)
        if key_cols:
            new_data = new_data.drop_duplicates(subset=key_cols)
        else:
            new_data = new_data.drop_duplicates()
        return new_data

    def save_raw_logs(self, date1, date2, visits_fields, hits_fields):
        req_visits = self.create_log_request("visits", date1, date2, visits_fields)
        parts_visits = self.wait_for_request(req_visits)
        visits_df = self.download_parts(
            req_visits, parts_visits, key_cols=["ym:s:visitID"]
        )

        req_hits = self.create_log_request("hits", date1, date2, hits_fields)
        parts_hits = self.wait_for_request(req_hits)
        hits_df = self.download_parts(req_hits, parts_hits, key_cols=["ym:pv:watchID"])

        return visits_df, hits_df

    # -------- Загрузка в S3 через Hook --------
    def upload_to_s3(
        self, df: pd.DataFrame, df_name: str, filter_column: str, date1: str, date2: str
    ):
        if df.empty:
            logging.warning(f"⚠️ Нет данных для {df_name}")
            return

        for single_date in pd.date_range(start=date1, end=date2):
            day_str = single_date.strftime("%Y-%m-%d")
            daily_df = df[df[filter_column].str.startswith(day_str)]
            if daily_df.empty:
                continue
            buffer = BytesIO()
            daily_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_key = f"{self.s3_prefix}/{df_name}_{day_str}.parquet"
            # если при инкреметнальной загрузке данные запишутся за дату, которая уже есть в бакете,
            # предыдущие данные затрутся и заменятся новыми из-за replace=True
            self.s3_hook.load_file_obj(
                buffer, bucket_name=self.bucket, key=s3_key, replace=True
            )
            logging.info(
                f"Загружено {len(daily_df)} строк в s3://{self.bucket}/{s3_key}"
            )
