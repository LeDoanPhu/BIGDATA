import os
import sys
from pathlib import Path

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F, types as T

# Nếu chạy trực tiếp từ src/streaming, thêm thư mục gốc vào sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from src.config.settings import HDFS_URL, KAFKA_BROKER, KAFKA_TOPIC
    from src.utils.spark_session import create_spark_session
except ImportError:
    from config.settings import HDFS_URL, KAFKA_BROKER, KAFKA_TOPIC
    from utils.spark_session import create_spark_session

CHECKPOINT_LOCATION = ROOT_DIR / 'checkpoints' / 'spark_consumer'
LOCAL_OUTPUT_PATH = ROOT_DIR / 'Data' / 'stream_output'
STREAM_SCHEMA = T.StructType([
    T.StructField('student_id', T.StringType(), True),
    T.StructField('age', T.IntegerType(), True),
    T.StructField('family_income', T.DoubleType(), True),
    T.StructField('parental_education_level', T.StringType(), True),
    T.StructField('daily_study_hours', T.DoubleType(), True),
    T.StructField('attendance_rate', T.DoubleType(), True),
    T.StructField('sleep_hours', T.DoubleType(), True),
    T.StructField('stress_level', T.DoubleType(), True),
    T.StructField('motivation_score', T.DoubleType(), True),
    T.StructField('internet_quality', T.StringType(), True),
    T.StructField('math_score', T.DoubleType(), True),
    T.StructField('reading_score', T.DoubleType(), True),
    T.StructField('writing_score', T.DoubleType(), True),
    T.StructField('pass_fail', T.StringType(), True),
    T.StructField('final_result', T.StringType(), True),
    T.StructField('source_generated_at', T.StringType(), True),
])

NUMERIC_CLAMPING = [
    ('attendance_rate', 0.0, 1.0),
    ('sleep_hours', 0.0, 24.0),
    ('math_score', 0.0, 100.0),
    ('reading_score', 0.0, 100.0),
    ('writing_score', 0.0, 100.0),
]


def ensure_paths():
    CHECKPOINT_LOCATION.mkdir(parents=True, exist_ok=True)
    LOCAL_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


def parse_kafka_value(df):
    parsed = df.select(
        F.from_json(F.col('value').cast('string'), STREAM_SCHEMA).alias('payload'),
        F.col('timestamp').alias('kafka_received_at'),
    )
    return parsed.select('payload.*', 'kafka_received_at')


def normalize_attendance_rate(df):
    if 'attendance_rate' not in df.columns:
        return df
    return df.withColumn(
        'attendance_rate',
        F.when(F.col('attendance_rate').isNotNull(),
               F.when(F.col('attendance_rate') > 1, F.col('attendance_rate') / 100.0)
                .otherwise(F.col('attendance_rate'))
        ).otherwise(None)
    )


def clamp_numeric_values(df):
    for col_name, min_val, max_val in NUMERIC_CLAMPING:
        if col_name not in df.columns:
            continue
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(), None)
             .when(F.col(col_name) < min_val, min_val)
             .when(F.col(col_name) > max_val, max_val)
             .otherwise(F.col(col_name))
        )
    return df


def fill_family_income(df):
    if 'family_income' not in df.columns or 'parental_education_level' not in df.columns:
        return df

    median_by_parent = (
        df.filter(F.col('family_income').isNotNull())
          .groupBy('parental_education_level')
          .agg(F.expr('percentile_approx(family_income, 0.5)').alias('median_income'))
    )
    df = df.join(median_by_parent, on='parental_education_level', how='left')
    df = df.withColumn(
        'family_income',
        F.when(F.col('family_income').isNull(), F.col('median_income')).otherwise(F.col('family_income'))
    )
    global_median = df.select(F.expr('percentile_approx(family_income, 0.5)').alias('global_median')).first()['global_median']
    if global_median is not None:
        df = df.na.fill({'family_income': float(global_median)})
    return df.drop('median_income')


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
    global_mean = df.select(F.avg(F.col(target_col)).alias('global_mean')).first()['global_mean']
    if global_mean is not None:
        df = df.na.fill({target_col: float(global_mean)})
    return df.drop(f'{target_col}_mean')


def transform_stream_batch(df):
    df = df.dropDuplicates()
    df = normalize_attendance_rate(df)
    df = clamp_numeric_values(df)
    df = fill_family_income(df)
    df = fill_group_mean(df, 'sleep_hours')
    df = fill_group_mean(df, 'stress_level')

    if 'pass_fail' not in df.columns and 'final_result' in df.columns:
        df = df.withColumnRenamed('final_result', 'pass_fail')

    if 'source_generated_at' not in df.columns:
        df = df.withColumn('source_generated_at', F.current_timestamp())

    return df


def print_summary(df):
    if 'pass_fail' in df.columns:
        df.groupBy('pass_fail').count().show(truncate=False)
    stats_columns = [c for c in ['math_score', 'reading_score', 'writing_score'] if c in df.columns]
    if stats_columns:
        df.select(*stats_columns).summary('count', 'mean', 'stddev', 'min', 'max').show(truncate=False)


def process_microbatch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f'Batch {batch_id}: no records')
        return

    converted = parse_kafka_value(batch_df)
    cleaned = transform_stream_batch(converted)

    print(f'Batch {batch_id}: received {cleaned.count()} cleaned records')
    print_summary(cleaned)

    output_path = LOCAL_OUTPUT_PATH / f'batch_{batch_id}'
    cleaned.write.mode('overwrite').parquet(str(output_path))
    print(f'Batch {batch_id}: written cleaned data to {output_path}')


def main():
    ensure_paths()
    spark = create_spark_session(app_name='SparkKafkaConsumer')

    kafka_df = (
        spark.readStream
             .format('kafka')
             .option('kafka.bootstrap.servers', KAFKA_BROKER)
             .option('subscribe', KAFKA_TOPIC)
             .option('startingOffsets', 'earliest')
             .load()
    )

    query = (
        kafka_df.writeStream
                .foreachBatch(process_microbatch)
                .option('checkpointLocation', str(CHECKPOINT_LOCATION))
                .trigger(processingTime='10 seconds')
                .start()
    )

    print('Spark consumer started. Listening to Kafka topic:', KAFKA_TOPIC)
    query.awaitTermination()


if __name__ == '__main__':
    main()
