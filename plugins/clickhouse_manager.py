import clickhouse_connect
from datetime import datetime
from typing import Optional, List
import logging

logger = logging.getLogger("airflow.task")


class ClickHouseManager:
    def __init__(self, host: str, user: str, password: str, port: int = 8123, database: str = "default"):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )
        self.database = database

    def execute_sql(self, query: str):
        """Выполняет произвольный SQL-запрос"""
        try:
            self.client.command(query)
            print("✅ SQL выполнен успешно")
        except Exception as e:
            print(f"❌ Ошибка выполнения SQL: {e}")

    def table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица"""
        try:
            result = self.client.query(f"""
                SELECT 1 
                FROM system.tables 
                WHERE database = '{self.database}' 
                AND name = '{table_name}'
                LIMIT 1
            """)
            return len(result.result_rows) > 0
        except Exception as e:
            print(f"❌ Ошибка проверки таблицы: {e}")
            return False

    def get_max_updated_at(self, table_name: str) -> Optional[datetime]:
        """Получает максимальное значение updated_at из таблицы"""
        if not self.table_exists(table_name):
            print(f"❗ Таблица {table_name} не существует")
            return None

        try:
            result = self.client.query(f"""
                SELECT MAX(updated_at) AS max_updated_at 
                FROM {self.database}.{table_name}
            """)
            value = result.result_rows[0][0]
            if value is None:
                print("⚠️ Таблица существует, но пустая")
                return None
            print(f"📅 Максимальная дата: {value}")
            return value
        except Exception as e:
            print(f"❌ Ошибка получения max(updated_at): {e}")
            return None

    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """Возвращает список колонок таблицы"""
        if not self.table_exists(table_name):
            print(f"❗ Таблица {table_name} не существует")
            return None

        try:
            result = self.client.query(f"""
                SELECT name 
                FROM system.columns 
                WHERE database = '{self.database}' 
                AND table = '{table_name}'
            """)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            print(f"❌ Ошибка получения колонок: {e}")
            return None