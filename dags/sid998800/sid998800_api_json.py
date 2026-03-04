import requests
import json
import glob

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
from airflow import DAG

DATA_DIR = Path("/tmp/roadmappers/api_data")
API_URL = "https://jsonplaceholder.typicode.com/posts"
CHUNK_SIZE = 60  # объявляем кол-во строк в одном блоке(чанке) данных


# Скачивает данные и разбивает на файлы по CHUNK_SIZE строк
def extract_and_split():
    # Создаем каталог /tmp/roadmappers/api_data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(API_URL, timeout=10)  # Считываем данные их файла
    response.raise_for_status()  # Выбросит исключение при ошибке HTTP
    data = response.json()  # Переводим данные в формат JSON

    total_rows = len(
        data
    )  # У нас JSON формата [{...},{...},{...}] получаем кол-во {...} оно по умолчанию равно 100
    total_files = (
        total_rows + CHUNK_SIZE - 1
    ) // CHUNK_SIZE  # получаем кол-во файлов 100 + 60 - 1//60 = 2

    for file_num in range(total_files):
        # Вычисляем границы текущего чанка
        start_idx = file_num * CHUNK_SIZE  # начало: 0, 60, 120...
        end_idx = start_idx + CHUNK_SIZE  # конец: 60, 120, 180...

        # Вырезаем нужный кусок данных
        chunk = data[start_idx:end_idx]
        # Формируем путь к файлу (номер с 1 для людей)
        file_path = DATA_DIR / f"data_part_{file_num + 1}.json"

        # Сохраняем с красивым форматированием
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)

        # Информируем о результате
        print(f"✅ Создан файл {file_path} ({len(chunk)} записей)")


# Подсчитывает количество строк во всех JSON-файлах в директории
def count_rows_in_files(**context):
    # Находим все JSON файлы
    json_files = sorted(DATA_DIR.glob("data_part_*.json"))

    if not json_files:
        print("❌ Нет файлов для обработки")
        return {}

    results = {}
    total_rows = 0

    print("\n" + "=" * 50)
    print("📊 АНАЛИЗ ФАЙЛОВ:")
    print("=" * 50)

    for file_num, file_path in enumerate(json_files, 1):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            rows_count = len(data)
            file_name = file_path.name

            results[file_name] = rows_count
            total_rows += rows_count

            print(f"📄 файл {file_num} - количество строк {rows_count}")

    print("=" * 50)
    print(f"📈 ВСЕГО: {total_rows} строк в {len(json_files)} файлах")
    print("=" * 50 + "\n")

    # Сохраняем в XCom для других задач
    context["ti"].xcom_push(key="file_stats", value=results)
    context["ti"].xcom_push(key="total_rows", value=total_rows)
    context["ti"].xcom_push(key="files_count", value=len(json_files))

    return results


# Подсчет количества файлов
def count_files_python(**context):
    files = list(DATA_DIR.glob("data_part_*.json"))
    count = len(files)

    # Детальный вывод всех найденных файлов
    print(f"\n📁 Найдено файлов в каталоге {DATA_DIR}: {count}")
    if count > 0:
        print("📋 Список файлов:")
        for i, file in enumerate(files, 1):
            print(f"   {i}. {file.name}")
    print()

    context["ti"].xcom_push(key="files_count", value=count)
    return count


dag = DAG(
    dag_id="sid998800_api_to_files",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["api", "json"],
)

# Задачи DAG

extract_api = PythonOperator(
    task_id="extract_and_split", python_callable=extract_and_split
)

count_all_files = PythonOperator(
    task_id="count_all_files",
    python_callable=count_rows_in_files,
    dag=dag,
)

count_files_task = PythonOperator(
    task_id="count_files",
    python_callable=count_files_python,
    dag=dag,
)

extract_api >> count_all_files >> count_files_task
