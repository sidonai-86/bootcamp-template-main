from io import StringIO, BytesIO
import pandas as pd

# --- BEST PRACTICE 1: Выносим логику форматов в отдельную конфигурацию ---
# Это позволяет легко добавлять новые форматы, не меняя основной код таски.
def save_as_parquet(df: pd.DataFrame, buffer: BytesIO):
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us')


def save_as_csv(df: pd.DataFrame, buffer: BytesIO):
    # Используем сжатие gzip для CSV, чтобы экономить место в S3
    text_buffer = StringIO()
    df.to_csv(text_buffer, index=False)
    buffer.write(text_buffer.getvalue().encode("utf-8"))


def save_as_json(df: pd.DataFrame, buffer: BytesIO):
    # JSON Lines формат удобен для больших данных
    text_buffer = StringIO()
    df.to_json(text_buffer, orient="records", lines=True)
    buffer.write(text_buffer.getvalue().encode("utf-8"))


# Словарь стратегий (Mapping)
FORMAT_STRATEGIES = {
    "parquet": {"func": save_as_parquet, "ext": "parquet"},
    "csv": {"func": save_as_csv, "ext": "csv"},
    "json": {"func": save_as_json, "ext": "json"},
}