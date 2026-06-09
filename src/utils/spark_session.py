import findspark
findspark.init()

from pyspark.sql import SparkSession

def create_spark_session(app_name="BigData_Project"):
    """
    Hàm khởi tạo kết nối Spark dùng chung cho toàn bộ dự án.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark
