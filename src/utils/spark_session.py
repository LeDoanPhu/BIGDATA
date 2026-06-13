import os
import sys

import findspark
findspark.init()

from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


def create_spark_session(app_name="BigData_Project", include_kafka=False):
    """
    Hàm khởi tạo kết nối Spark dùng chung cho toàn bộ dự án.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.use.daemon", "false")
    )
    if include_kafka:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)

    spark = builder.getOrCreate()
    return spark
