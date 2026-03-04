from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType, StringType, IntegerType, LongType, 
    ShortType, ByteType, FloatType, DoubleType, 
    DateType, TimestampType, DecimalType
)
from typing import Dict, List
class SparkClickHouseDtypesPrepare:
    """
    Модуль для приведения типов Spark DataFrame к типам ClickHouse
    перед записью через JDBC.
    Поддерживает DateTime64 с разной точностью через суффиксы.
    """
    
    # Маппинг ClickHouse типов -> Spark типы (для простых колонок)
    CH_TO_SPARK_SIMPLE = {
        'uint8': 'short',
        'uint16': 'int',
        'uint32': 'long',
        'uint64': 'decimal(20,0)',
        'int8': 'byte',
        'int16': 'short',
        'int32': 'int',
        'int64': 'long',
        'float32': 'float',
        'float64': 'double',
        'string': 'string',
        'date': 'date',
        'datetime': 'timestamp',
        'datetime64_0': 'timestamp',
        'datetime64_3': 'timestamp',
        'datetime64_6': 'timestamp',
        'datetime64_9': 'timestamp',
    }
    
    # Маппинг для элементов массивов
    CH_TO_SPARK_ARRAY_ELEMENT = {
        'uint8': ShortType(),
        'uint16': IntegerType(),
        'uint32': LongType(),
        'uint64': DecimalType(20, 0),
        'int8': ByteType(),
        'int16': ShortType(),
        'int32': IntegerType(),
        'int64': LongType(),
        'float32': FloatType(),
        'float64': DoubleType(),
        'string': StringType(),
        'str': StringType(),
        'date': DateType(),
        'datetime': TimestampType(),
        'datetime64_0': TimestampType(),
        'datetime64_3': TimestampType(),
        'datetime64_6': TimestampType(),
        'datetime64_9': TimestampType(),
    }
    
    # Маппинг типов на их precision
    DATETIME_PRECISION = {
        'datetime': 0,
        'datetime64_0': 0,
        'datetime64_3': 3,
        'datetime64_6': 6,
        'datetime64_9': 9,
    }
    
    def __init__(
        self,
        array_columns: Dict[str, str] = {},
        simple_columns: Dict[str, str] = {},
        need_cols: List[str] = None
    ):
        """
        Args:
            array_columns: {'columnName': 'ch_type', ...} для Array колонок
            simple_columns: {'columnName': 'ch_type', ...} для простых колонок
                Поддерживаемые форматы для datetime:
                - 'datetime' -> секундная точность (DateTime)
                - 'datetime64_0' -> секундная точность (DateTime64(0))
                - 'datetime64_3' -> миллисекунды (DateTime64(3))
                - 'datetime64_6' -> микросекунды (DateTime64(6))
                - 'datetime64_9' -> наносекунды (DateTime64(9))
            need_cols: список имен колонок в нужном порядке для финального DF
        """
        self.array_columns = array_columns
        self.simple_columns = simple_columns
        self.need_cols = need_cols
    
    def prepare(self, df: DataFrame) -> DataFrame:
        """
        Основной метод: принимает Spark DataFrame, возвращает преобразованный
        """
        # Шаг 1: Фильтруем словари по доступным колонкам
        available_columns = set(df.columns)

        if not self.need_cols:
            self.need_cols = available_columns
        
        array_cols_filtered = {
            k: v for k, v in self.array_columns.items() 
            if k in available_columns
        }
        
        simple_cols_filtered = {
            k: v for k, v in self.simple_columns.items() 
            if k in available_columns
        }
        
        need_cols_filtered = [
            col for col in self.need_cols 
            if col in available_columns
        ]
        
        # Шаг 2: Обрабатываем массивы (парсинг строк в array)
        df = self._process_array_columns(df, array_cols_filtered)
        
        # Шаг 3: Приводим типы простых колонок
        df = self._process_simple_columns(df, simple_cols_filtered)
        
        # Шаг 4: Выбираем нужные колонки в правильном порядке
        df = df.select(*need_cols_filtered)
        
        return df
    
    def _is_datetime_type(self, ch_type: str) -> bool:
        """Проверяет, является ли тип datetime-подобным"""
        return ch_type.lower() in self.DATETIME_PRECISION
    
    def _get_datetime_precision(self, ch_type: str) -> int:
        """Возвращает precision для datetime типа"""
        return self.DATETIME_PRECISION.get(ch_type.lower(), 0)
    
    def _process_array_columns(
        self, 
        df: DataFrame, 
        array_columns: Dict[str, str]
    ) -> DataFrame:
        """
        Парсит строковые представления массивов в typed arrays
        """
        for col_name, ch_type in array_columns.items():
            ch_type_lower = ch_type.lower()
            
            # Получаем Spark тип для элементов массива
            element_type = self.CH_TO_SPARK_ARRAY_ELEMENT.get(
                ch_type_lower, 
                StringType()
            )
            
            # Парсим строку в массив
            df = df.withColumn(
                col_name,
                self._parse_string_to_array(F.col(col_name), ch_type_lower)
            )
        
        return df
    
    def _parse_string_to_array(self, col, ch_type: str):
        """
        Парсинг строки в typed array с учетом precision для datetime
        """
        # Убираем квадратные скобки, кавычки, бэкслэши
        cleaned = F.regexp_replace(col, r"[\[\]'\\]", "")
        
        # Сплитим по запятой
        arr = F.split(cleaned, ",")
        
        # Фильтруем пустые строки
        arr = F.expr("filter(arr, x -> x != '')")
        
        # Приводим к нужному типу
        if ch_type == 'date':
            return F.expr(
                "transform(arr, x -> to_date(regexp_replace(x, 'T', ' ')))"
            )
        elif self._is_datetime_type(ch_type):
            precision = self._get_datetime_precision(ch_type)
            
            if precision == 0:
                # DateTime или DateTime64(0) - усекаем до секунд
                return F.expr(
                    "transform(arr, x -> date_trunc('second', to_timestamp(regexp_replace(x, 'T', ' '))))"
                )
            else:
                # DateTime64 с дробными секундами - оставляем timestamp
                return F.expr(
                    "transform(arr, x -> to_timestamp(regexp_replace(x, 'T', ' ')))"
                )
        elif 'int' in ch_type or 'uint' in ch_type:
            element_type = self.CH_TO_SPARK_ARRAY_ELEMENT.get(ch_type, IntegerType())
            cast_type = self._get_spark_cast_string(element_type)
            return F.expr(f"transform(arr, x -> cast(x as {cast_type}))")
        elif 'float' in ch_type:
            element_type = self.CH_TO_SPARK_ARRAY_ELEMENT.get(ch_type, DoubleType())
            cast_type = self._get_spark_cast_string(element_type)
            return F.expr(f"transform(arr, x -> cast(x as {cast_type}))")
        else:
            return arr
    
    def _get_spark_cast_string(self, spark_type) -> str:
        """Конвертирует Spark Type в строку для cast"""
        type_map = {
            ByteType: 'byte',
            ShortType: 'short',
            IntegerType: 'int',
            LongType: 'long',
            FloatType: 'float',
            DoubleType: 'double',
            StringType: 'string',
            DateType: 'date',
            TimestampType: 'timestamp',
        }
        
        for typ, name in type_map.items():
            if isinstance(spark_type, typ):
                return name
        
        if isinstance(spark_type, DecimalType):
            return f'decimal({spark_type.precision},{spark_type.scale})'
        
        return 'string'
    
    def _process_simple_columns(
        self, 
        df: DataFrame, 
        simple_columns: Dict[str, str]
    ) -> DataFrame:
        """
        Приводит типы простых (не-array) колонок с поддержкой DateTime64
        + заполняет NULL значения
        """
        for col_name, ch_type in simple_columns.items():
            ch_type_lower = ch_type.lower()
            spark_type = self.CH_TO_SPARK_SIMPLE.get(ch_type_lower, 'string')
            
            # Кастим тип
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
            
            # Обработка DateTime / DateTime64
            if self._is_datetime_type(ch_type_lower):
                precision = self._get_datetime_precision(ch_type_lower)
                df = self._truncate_timestamp(df, col_name, precision)
            
            # Заполняем NULL
            if any(x in ch_type_lower for x in ['int', 'float', 'uint', 'decimal']):
                df = df.withColumn(
                    col_name, 
                    F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name))
                )
            elif ch_type_lower == 'string':
                df = df.withColumn(
                    col_name, 
                    F.when(F.col(col_name).isNull(), '').otherwise(F.col(col_name))
                )
        
        return df
    
    def _truncate_timestamp(
        self, 
        df: DataFrame, 
        col_name: str, 
        precision: int
    ) -> DataFrame:
        """
        Усекает timestamp до нужной точности
        
        Args:
            precision: 
                0 -> секунды (DateTime, DateTime64(0))
                3 -> миллисекунды (DateTime64(3))
                6 -> микросекунды (DateTime64(6))
                9 -> наносекунды (DateTime64(9))
        """
        if precision == 0:
            # Усекаем до секунд
            df = df.withColumn(
                col_name,
                F.date_trunc('second', F.col(col_name))
            )
        elif precision == 3:
            # Усекаем до миллисекунд
            df = df.withColumn(
                col_name,
                F.expr(f"timestamp_micros(cast(unix_micros({col_name}) / 1000 as bigint) * 1000)")
            )
        elif precision == 6:
            # Микросекунды - оставляем как есть (Spark timestamp = микросекунды)
            pass
        elif precision == 9:
            # Наносекунды - Spark не поддерживает, оставляем микросекунды
            # При необходимости можно конвертировать в String/Decimal
            pass
        
        return df