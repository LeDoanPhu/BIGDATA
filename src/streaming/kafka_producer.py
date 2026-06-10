import json
import os
import sys
import time
from pathlib import Path

import findspark
findspark.init()

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

# Nếu chạy script trực tiếp từ src/streaming, đảm bảo parent src được thêm vào sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from src.config.settings import HDFS_URL, KAFKA_BROKER, KAFKA_TOPIC
    from src.utils.spark_session import create_spark_session
except ImportError:
    from config.settings import HDFS_URL, KAFKA_BROKER, KAFKA_TOPIC
    from utils.spark_session import create_spark_session

try:
    from kafka import KafkaProducer
except ImportError as exc:
    raise ImportError(
        'kafka-python is required to run this producer. Install with: pip install kafka-python'
    ) from exc

LOCAL_DATA_PATH = Path(ROOT_DIR) / 'Data' / 'student_data'
REMOTE_HDFS_PATH = f"{HDFS_URL.rstrip('/')}/student_data.csv"

NUMERIC_FIELDS = [
    'family_income',
    'sleep_hours',
    'stress_level',
    'attendance_rate',
    'math_score',
    'reading_score',
    'writing_score',
]


def load_source_data(spark: SparkSession):
    source_path = str(LOCAL_DATA_PATH) if LOCAL_DATA_PATH.exists() and LOCAL_DATA_PATH.stat().st_size > 0 else REMOTE_HDFS_PATH
    print(f'Loading source data from: {source_path}')
    df = spark.read.csv(source_path, header=True, inferSchema=True)
    print(f'Read schema: {df.schema.simpleString()}')
    return df


def cast_numeric_columns(df):
    for col_name in NUMERIC_FIELDS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))
    return df


def fill_family_income(df):
    if 'family_income' not in df.columns or 'parental_education_level' not in df.columns:
        return df

    family_median = (
        df.filter(F.col('family_income').isNotNull())
          .groupBy('parental_education_level')
          .agg(F.expr('percentile_approx(family_income, 0.5)').alias('family_income_median'))
    )
    df = df.join(family_median, on='parental_education_level', how='left')
    df = df.withColumn(
        'family_income',
        F.when(F.col('family_income').isNull(), F.col('family_income_median')).otherwise(F.col('family_income'))
    )
    global_median_row = df.select(F.expr('percentile_approx(family_income, 0.5)').alias('global_median')).first()
    global_median = global_median_row['global_median'] if global_median_row is not None else None
    if global_median is not None:
        df = df.na.fill({'family_income': float(global_median)})
    return df.drop('family_income_median')


def fill_group_mean(df, target_col):
    if target_col not in df.columns or 'pass_fail' not in df.columns:
        return df

    mean_by_pass = (
        df.groupBy('pass_fail')
          .agg(F.avg(F.col(target_col)).alias(f'{target_col}_mean'))
    )
    df = df.join(mean_by_pass, on='pass_fail', how='left')
    df = df.withColumn(
        target_col,
        F.when(F.col(target_col).isNull(), F.col(f'{target_col}_mean')).otherwise(F.col(target_col))
    )
    global_mean_row = df.select(F.avg(F.col(target_col)).alias('global_mean')).first()
    global_mean = global_mean_row['global_mean'] if global_mean_row is not None else None
    if global_mean is not None:
        df = df.na.fill({target_col: float(global_mean)})
    return df.drop(f'{target_col}_mean')


def transform_data(df):
    print('Transforming data: drop duplicates, cast numeric types, fill missing values')
    df = df.dropDuplicates()
    df = cast_numeric_columns(df)
    df = fill_family_income(df)
    df = fill_group_mean(df, 'sleep_hours')
    df = fill_group_mean(df, 'stress_level')

    if 'source_generated_at' not in df.columns:
        df = df.withColumn('source_generated_at', F.current_timestamp())

    if 'pass_fail' not in df.columns and 'final_result' in df.columns:
        df = df.withColumnRenamed('final_result', 'pass_fail')

    return df


def create_producer(broker_url: str):
    return KafkaProducer(
        bootstrap_servers=[broker_url],
        value_serializer=lambda v: v.encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None,
        acks='all',
        linger_ms=10,
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def produce_to_kafka(df, topic: str, producer, interval_seconds: float = 1.0, max_records: int = None):
    print(f'Producing records to Kafka topic: {topic}')
    sent = 0
    for record in df.toJSON().toLocalIterator():
        producer.send(topic, value=record)
        sent += 1
        if sent % 100 == 0:
            producer.flush()
            print(f'Sent {sent} records...')
        if max_records is not None and sent >= max_records:
            break
        time.sleep(interval_seconds)
    producer.flush()
    print(f'Finished sending {sent} records.')


def main():
    spark = create_spark_session(app_name='KafkaProducerETL')
    try:
        raw_df = load_source_data(spark)
        cleaned_df = transform_data(raw_df)
        producer = create_producer(KAFKA_BROKER)
        produce_to_kafka(cleaned_df, KAFKA_TOPIC, producer, interval_seconds=1.0, max_records=None)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
