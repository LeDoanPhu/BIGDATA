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

HDFS_OUTPUT_BASE = f"{HDFS_URL.rstrip('/')}/stream_output"
HDFS_CHECKPOINT_BASE = f"{HDFS_URL.rstrip('/')}/checkpoints"
CHECKPOINT_LOCATION = f"{HDFS_CHECKPOINT_BASE}/spark_consumer"
KAFKA_STARTING_OFFSETS = "latest"
STREAM_SCHEMA = T.StructType([
    T.StructField('gender', T.StringType(), True),
    T.StructField('age', T.DoubleType(), True),
    T.StructField('parental_education_level', T.DoubleType(), True),
    T.StructField('family_income', T.DoubleType(), True),
    T.StructField('daily_study_hours', T.DoubleType(), True),
    T.StructField('attendance_rate', T.DoubleType(), True),
    T.StructField('sleep_hours', T.DoubleType(), True),
    T.StructField('stress_level', T.DoubleType(), True),
    T.StructField('motivation_score', T.DoubleType(), True),
    T.StructField('private_tutoring', T.BooleanType(), True),
    T.StructField('internet_quality', T.DoubleType(), True),
    T.StructField('math_score', T.DoubleType(), True),
    T.StructField('reading_score', T.DoubleType(), True),
    T.StructField('writing_score', T.DoubleType(), True),
    T.StructField('pass_fail', T.StringType(), True),
])

NUMERIC_CLAMPING = [
    ('attendance_rate', 0.0, 1.0),
    ('sleep_hours', 0.0, 24.0),
    ('math_score', 0.0, 100.0),
    ('reading_score', 0.0, 100.0),
    ('writing_score', 0.0, 100.0),
]
DEDUP_EXCLUDE_COLUMNS = {'kafka_received_at', 'source_generated_at'}


def ensure_paths():
    if not str(CHECKPOINT_LOCATION).startswith('hdfs://'):
        Path(CHECKPOINT_LOCATION).mkdir(parents=True, exist_ok=True)


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


def fill_sleep_hours_by_age(df):
    if 'sleep_hours' not in df.columns or 'age' not in df.columns:
        return df

    median_by_age = (
        df.filter(F.col('sleep_hours').isNotNull())
          .groupBy('age')
          .agg(F.expr('percentile_approx(sleep_hours, 0.5)').alias('median_sleep_hours'))
    )
    df = df.join(median_by_age, on='age', how='left')
    df = df.withColumn(
        'sleep_hours',
        F.when(F.col('sleep_hours').isNull(), F.col('median_sleep_hours')).otherwise(F.col('sleep_hours'))
    )
    global_median = df.select(F.expr('percentile_approx(sleep_hours, 0.5)').alias('global_median')).first()['global_median']
    if global_median is not None:
        df = df.na.fill({'sleep_hours': float(global_median)})
    return df.drop('median_sleep_hours')


def fill_stress_level_by_motivation_score(df):
    if 'stress_level' not in df.columns or 'motivation_score' not in df.columns:
        return df

    df = df.withColumn(
        '_motivation_band',
        F.when(F.col('motivation_score').isNull(), F.lit('missing'))
         .when(F.col('motivation_score') <= 20, F.lit('0-20'))
         .when(F.col('motivation_score') <= 40, F.lit('21-40'))
         .when(F.col('motivation_score') <= 60, F.lit('41-60'))
         .when(F.col('motivation_score') <= 80, F.lit('61-80'))
         .otherwise(F.lit('81-100'))
    )

    median_by_motivation = (
        df.filter(F.col('stress_level').isNotNull())
          .groupBy('_motivation_band')
          .agg(F.expr('percentile_approx(stress_level, 0.5)').alias('median_stress_level'))
    )
    df = df.join(median_by_motivation, on='_motivation_band', how='left')
    df = df.withColumn(
        'stress_level',
        F.when(F.col('stress_level').isNull(), F.col('median_stress_level')).otherwise(F.col('stress_level'))
    )
    global_median = df.select(F.expr('percentile_approx(stress_level, 0.5)').alias('global_median')).first()['global_median']
    if global_median is not None:
        df = df.na.fill({'stress_level': float(global_median)})
    return df.drop('_motivation_band', 'median_stress_level')


def drop_business_duplicates(df):
    dedup_columns = [column for column in df.columns if column not in DEDUP_EXCLUDE_COLUMNS]
    if not dedup_columns:
        return df.dropDuplicates()
    return df.dropDuplicates(dedup_columns)


def transform_stream_batch(df):
    df = drop_business_duplicates(df)
    df = normalize_attendance_rate(df)
    df = clamp_numeric_values(df)
    df = fill_family_income(df)
    df = fill_sleep_hours_by_age(df)
    df = fill_stress_level_by_motivation_score(df)

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
    if batch_df.isEmpty():
        print(f'Batch {batch_id}: no records', flush=True)
        return

    converted = parse_kafka_value(batch_df)
    cleaned = transform_stream_batch(converted)

    print(f'Batch {batch_id}: received {cleaned.count()} cleaned records', flush=True)
    print_summary(cleaned)

    output_path = f"{HDFS_OUTPUT_BASE}"
    cleaned.write.mode('append').parquet(output_path)
    print(f'Batch {batch_id}: appended cleaned data to {output_path}', flush=True)


def main():
    ensure_paths()
    spark = create_spark_session(app_name='SparkKafkaConsumer', include_kafka=True)
    spark.sparkContext.setLogLevel("WARN")
    kafka_df = (
        spark.readStream
             .format('kafka')
             .option('kafka.bootstrap.servers', KAFKA_BROKER)
             .option('subscribe', KAFKA_TOPIC)
             .option('startingOffsets', KAFKA_STARTING_OFFSETS)
             .load()
    )

    query = (
        kafka_df.writeStream
                .foreachBatch(process_microbatch)
                .option('checkpointLocation', str(CHECKPOINT_LOCATION))
                .trigger(processingTime='1 seconds')
                .start()
    )

    print('Spark consumer started. Listening to Kafka topic:', KAFKA_TOPIC, flush=True)
    query.awaitTermination()


if __name__ == '__main__':
    main()
