from __future__ import annotations

import logging
import socket

import clickhouse_connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

CH_HOST = Variable.get("CLICKHOUSE_HOST")
CH_PORT = int(Variable.get("CLICKHOUSE_PORT"))
CH_USER = Variable.get("CLICKHOUSE_USER")
CH_PASSWORD = Variable.get("CLICKHOUSE_PASSWORD")

# Часто встречающиеся имена сервисов в docker-compose/сети
CANDIDATE_HOSTS = [
    "clickhouse",
    "clickhouse-server",
    "clickhouse1",
    "clickhouse-01",
    "ch",
    "db",
    "company_clickhouse",
    "host.docker.internal",   # иногда работает на macOS Docker Desktop
    "localhost",              # почти всегда НЕ то, но пусть будет для диагностики
]


def debug_dns(**context) -> dict:
    """Проверка DNS изнутри Airflow контейнера: какие имена вообще резолвятся."""
    results = {}
    log.info("Current Variables: CLICKHOUSE_HOST=%r CLICKHOUSE_PORT=%s", CH_HOST, CH_PORT)

    for host in CANDIDATE_HOSTS + ([CH_HOST] if CH_HOST else []):
        if not host:
            continue
        try:
            ip = socket.gethostbyname(host)
            log.info("DNS OK: %s -> %s", host, ip)
            results[host] = {"dns": "ok", "ip": ip}
        except Exception as e:
            log.info("DNS FAIL: %s -> %s", host, e)
            results[host] = {"dns": "fail", "error": str(e)}

    return {"dns_results": results}


def debug_clickhouse_connect(**context) -> dict:
    """
    Пробуем подключиться clickhouse-connect к тем хостам, которые резолвятся.
    Для каждого: тестовый запрос SELECT 1.
    """
    ti = context["ti"]
    dns_payload = ti.xcom_pull(task_ids="debug_dns") or {}
    dns_results = dns_payload.get("dns_results", {})

    ok_hosts = [h for h, r in dns_results.items() if r.get("dns") == "ok"]
    if not ok_hosts:
        log.warning("No resolvable hosts found. Fix Docker DNS/network or add correct hostname.")
        return {"clickhouse_tests": {}, "ok": False}

    tests = {}
    for host in ok_hosts:
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=CH_PORT,
                username=CH_USER or None,
                password=CH_PASSWORD or None,
            )
            v = client.query("SELECT 1").result_rows[0][0]
            log.info("CH OK: host=%s port=%s -> SELECT 1 = %s", host, CH_PORT, v)
            tests[host] = {"connect": "ok", "select_1": v}
        except Exception as e:
            log.info("CH FAIL: host=%s port=%s -> %s", host, CH_PORT, e)
            tests[host] = {"connect": "fail", "error": str(e)}

    ok = any(t.get("connect") == "ok" for t in tests.values())
    return {"clickhouse_tests": tests, "ok": ok}


default_args = {"owner": "airflow"}

with DAG(
    dag_id="vpuzey_debug_clickhouse_dns",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,   # вручную запускать удобно
    catchup=False,
    tags=["debug", "clickhouse"],
) as dag:

    t_dns = PythonOperator(
        task_id="debug_dns",
        python_callable=debug_dns,
    )

    t_ch = PythonOperator(
        task_id="debug_clickhouse_connect",
        python_callable=debug_clickhouse_connect,
    )

    t_dns >> t_ch
