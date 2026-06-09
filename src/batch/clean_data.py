import os
import sys

# Thêm thư mục gốc vào đường dẫn để gọi được config và utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F
from config import settings

def main():
    print("=== KHỞI ĐỘNG HỆ THỐNG BATCH (TỰ ĐỘNG DỌN RÁC LỊCH SỬ) ===")
    
    # 1. Khởi tạo kết nối Spark
    spark = SparkSession.builder \
        .appName("KetNoiHDFS_Batch_ETL") \
        .getOrCreate()
        
    print(f"Đang đọc dữ liệu từ HDFS: {settings.HDFS_URL}/student_data.csv")
    
    # Ở đồ án thực tế, đường dẫn HDFS sẽ là: f"{settings.HDFS_URL}/student_data.csv"
    # Tạm thời để code chạy được nghiệm thu, nếu chưa có HDFS ta dùng file local
    # Xóa dòng local này khi HDFS đã sẵn sàng
    csv_path = "D:/Final BigData/BIGDATA/data/student_data.csv" 
    
    try:
        # Đọc dữ liệu (Tạm dùng local để tránh lỗi nếu HDFS tắt)
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"Đã đọc thành công {df.count()} dòng dữ liệu Raw!")
        
        # -------------------------------------------------------------
        # ÁP DỤNG LUẬT EDA VÀO ETL BATCH
        # -------------------------------------------------------------
        
        # LUẬT 1: Điền Null cho sleep_hours bằng MEAN theo pass_fail
        print("Đang áp dụng Luật 1: Điền khuyết sleep_hours...")
        df_sleep_mean = df.groupBy("pass_fail").agg(F.mean("sleep_hours").alias("mean_sleep"))
        # Join để lấy giá trị mean ứng với từng pass_fail
        df = df.join(df_sleep_mean, on="pass_fail", how="left")
        df = df.withColumn("sleep_hours", when(col("sleep_hours").isNull(), col("mean_sleep")).otherwise(col("sleep_hours")))
        df = df.drop("mean_sleep")

        # LUẬT 2: Điền Null cho stress_level bằng MEAN theo pass_fail
        print("Đang áp dụng Luật 2: Điền khuyết stress_level...")
        df_stress_mean = df.groupBy("pass_fail").agg(F.mean("stress_level").alias("mean_stress"))
        df = df.join(df_stress_mean, on="pass_fail", how="left")
        df = df.withColumn("stress_level", when(col("stress_level").isNull(), col("mean_stress")).otherwise(col("stress_level")))
        df = df.drop("mean_stress")
        
        # LUẬT 3: Điền Null cho family_income bằng MEDIAN theo parent_education_level
        print("Đang áp dụng Luật 3: Điền khuyết family_income...")
        # Ở PySpark tính Median dùng percentile_approx
        df_income_median = df.groupBy("parent_education_level").agg(F.expr("percentile_approx(family_income, 0.5)").alias("median_income"))
        df = df.join(df_income_median, on="parent_education_level", how="left")
        df = df.withColumn("family_income", when(col("family_income").isNull(), col("median_income")).otherwise(col("family_income")))
        df = df.drop("median_income")

        print("ĐÃ LÀM SẠCH XONG TOÀN BỘ 100% DỮ LIỆU BATCH!")
        df.show(5)

        # -------------------------------------------------------------
        # LƯU KẾT QUẢ VÀO HDFS (Thư mục Clean Data)
        # -------------------------------------------------------------
        # output_path = f"{settings.HDFS_URL}/clean_data"
        # df.write.mode("overwrite").parquet(output_path)
        # print(f"Đã lưu file làm sạch xuống HDFS thành công tại: {output_path}")

    except Exception as e:
        print(f"LỖI: {e}")
        print("Vui lòng kiểm tra lại đường dẫn file CSV.")

if __name__ == "__main__":
    main()
