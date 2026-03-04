from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


class S3MaxDateManager:
    def __init__(
        self,
        table_name: str,
        init_value: str,
        postgres_conn_id: str = "metadata_db",
        postgres_db_schema: str = 'public'
    ):
        self.table_name = table_name
        self.init_value = init_value
        self.postgres_db_schema = postgres_db_schema
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_max_value(self) -> str:
        sql = f"""
            SELECT COALESCE(MAX(max_value), %s)
            FROM {self.postgres_db_schema}.s3_max_values
            WHERE table_name = %s
        """
        result = self.hook.get_first(sql, parameters=(self.init_value, self.table_name))
        return result[0].strftime("%Y-%m-%d")

    def update_max_value(self, date_value: str, updated_at=datetime.now()):
        sql = f"""
            INSERT INTO {self.postgres_db_schema}.s3_max_values (table_name, max_value, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE
            SET max_value = EXCLUDED.max_value,
                updated_at = EXCLUDED.updated_at
        """
        self.hook.run(sql, parameters=(self.table_name, date_value, updated_at))

