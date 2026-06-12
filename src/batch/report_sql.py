import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types as T

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.chdir(str(ROOT_DIR))


def clean_and_transform_data(df):
    df = df.dropDuplicates()
    current_cols = df.columns

    df_mapped = df.select(
        F.col(current_cols[0]).alias("gender"),
        F.col(current_cols[1]).alias("age"),
        F.col(current_cols[2]).alias("parental_education_level"),
        F.col(current_cols[3]).alias("family_income"),
        F.col(current_cols[4]).alias("sleep_hours"),
        F.col(current_cols[5]).alias("stress_level"),
        F.col(current_cols[6]).alias("attendance_rate"),
        F.col(current_cols[7]).alias("math_score"),
        F.col(current_cols[8]).alias("reading_score"),
        F.col(current_cols[9]).alias("writing_score"),
        F.col(current_cols[10]).alias("daily_study_hours"),
        F.col(current_cols[11]).alias("pass_fail")
    )

    df_mapped = df_mapped.withColumn("age", F.col("age").cast(T.IntegerType()))

    numeric_fields = ['family_income', 'sleep_hours', 'stress_level', 'attendance_rate', 'math_score', 'reading_score',
                      'writing_score', 'daily_study_hours']
    for col_name in numeric_fields:
        df_mapped = df_mapped.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))

    df_mapped = df_mapped.withColumn('pass_fail', F.lower(F.col('pass_fail').cast(T.StringType())))

    if "sleep_hours" in df_mapped.columns and "pass_fail" in df_mapped.columns:
        df_sleep_mean = df_mapped.groupBy("pass_fail").agg(F.mean("sleep_hours").alias("mean_sleep"))
        df_mapped = df_mapped.join(df_sleep_mean, ["pass_fail"], how="left")
        df_mapped = df_mapped.withColumn("sleep_hours",
                                         F.when(F.col("sleep_hours").isNull(), F.col("mean_sleep")).otherwise(
                                             F.col("sleep_hours"))).drop("mean_sleep")

    if "stress_level" in df_mapped.columns and "pass_fail" in df_mapped.columns:
        df_stress_mean = df_mapped.groupBy("pass_fail").agg(F.mean("stress_level").alias("mean_stress"))
        df_mapped = df_mapped.join(df_stress_mean, ["pass_fail"], how="left")
        df_mapped = df_mapped.withColumn("stress_level",
                                         F.when(F.col("stress_level").isNull(), F.col("mean_stress")).otherwise(
                                             F.col("stress_level"))).drop("mean_stress")

    df_final = df_mapped.na.fill({
        'family_income': 50000.0,
        'sleep_hours': 7.0,
        'stress_level': 5.0,
        'math_score': 60.0,
        'reading_score': 60.0,
        'writing_score': 60.0,
        'daily_study_hours': 3.0,
        'attendance_rate': 0.85,
        'pass_fail': 'pass'
    })

    return df_final


def main():
    HDFS_URL = "hdfs://100.109.78.104:9000"
    FILE_HDFS_PATH = f"{HDFS_URL}/stream_output"

    spark = SparkSession.builder \
        .appName("Spark_HDFS_SQL_Report") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        spark.conf.set("spark.sql.caseSensitive", "false")

        # Thêm thông báo hiển thị đường dẫn làm việc và nguồn dữ liệu stream
        print(f"Thu muc lam viec hien tai: {ROOT_DIR}")
        print(f"Dang quet va nap du lieu stream tu HDFS: {FILE_HDFS_PATH}")

        df_raw = spark.read.parquet(FILE_HDFS_PATH)
        df_cleaned = clean_and_transform_data(df_raw)
        df_cleaned.createOrReplaceTempView("student_stream")

        # --- CÂU 1 ---
        print("\n--- BÁO CÁO 1: Điểm trung bình, độ chênh lệch điểm và phân vị toàn trường ---")
        spark.sql("""
            WITH score_variance_table AS (
                SELECT 
                    UPPER(gender) AS gioi_tinh, 
                    age AS tuoi, 
                    attendance_rate AS ti_le_chuyen_can,
                    (math_score + reading_score + writing_score) / 3 AS diem_trung_binh,
                    GREATEST(math_score, reading_score, writing_score) - LEAST(math_score, reading_score, writing_score) AS do_chenh_lech_diem
                FROM student_stream
            )
            SELECT 
                gioi_tinh, 
                tuoi, 
                ROUND(diem_trung_binh, 2) AS diem_trung_binh_3_mon, 
                ROUND(do_chenh_lech_diem, 2) AS chenh_lech_max_min,
                ROUND(ti_le_chuyen_can, 2) AS ti_le_chuyen_can,
                CAST(CUME_DIST() OVER (ORDER BY diem_trung_binh DESC) AS DECIMAL(6, 4)) AS ti_le_phan_vi_toan_truong 
            FROM score_variance_table 
            ORDER BY diem_trung_binh_3_mon DESC, chenh_lech_max_min ASC
            LIMIT 15
        """).show()

        # --- CÂU 2 ---
        print("\n--- BÁO CÁO 2: Điểm toán và mức độ stress theo nhóm thu nhập gia đình ---")
        spark.sql("""
            WITH rank_income_table AS (
                SELECT 
                    family_income, 
                    stress_level, 
                    math_score, 
                    PERCENT_RANK() OVER (ORDER BY family_income DESC) AS xep_hang_thu_nhap 
                FROM student_stream
            ),
            categorized_income_table AS (
                SELECT *,
                       CASE 
                           WHEN xep_hang_thu_nhap <= 0.25 THEN 'Nhóm 1: Thu nhập Cao (Top 25%)'
                           WHEN xep_hang_thu_nhap <= 0.50 THEN 'Nhóm 2: Thu nhập Khá'
                           WHEN xep_hang_thu_nhap <= 0.75 THEN 'Nhóm 3: Thu nhập Trung bình'
                           ELSE 'Nhóm 4: Thu nhập Thấp (Bottom 25%)'
                       END AS nhom_thu_nhap
                FROM rank_income_table
            )
            SELECT 
                nhom_thu_nhap, 
                COUNT(*) AS so_luong_hoc_sinh, 
                ROUND(AVG(math_score), 2) AS trung_binh_diem_toan,
                ROUND(AVG(stress_level), 2) AS muc_stress_binh_quan,
                ROUND(STDDEV(math_score), 2) AS do_lech_chuan_diem_toan
            FROM categorized_income_table
            GROUP BY nhom_thu_nhap 
            ORDER BY nhom_thu_nhap ASC
        """).show()

        # --- CÂU 3 ---
        print("\n--- BÁO CÁO 3: Top 3 học sinh điểm toán cao nhất theo giới tính và kết quả học tập ---")
        spark.sql("""
            WITH ranked_students AS (
                SELECT 
                    CASE 
                        WHEN pass_fail LIKE '1%' OR pass_fail = 'pass' OR pass_fail = '0' THEN 'ĐẠT (PASS)'
                        ELSE 'TRƯỢT (FAIL)'
                    END AS ket_qua_hoc_tap, 
                    INITCAP(gender) AS gioi_tinh, 
                    age AS tuoi, 
                    math_score AS diem_toan, 
                    daily_study_hours AS gio_tu_hoc,
                    DENSE_RANK() OVER (
                        PARTITION BY pass_fail, gender 
                        ORDER BY math_score DESC, daily_study_hours ASC, attendance_rate DESC
                    ) AS xep_hang_nhom 
                FROM student_stream
            )
            SELECT * FROM ranked_students WHERE xep_hang_nhom <= 3
            ORDER BY ket_qua_hoc_tap ASC, gioi_tinh ASC, xep_hang_nhom ASC
        """).show()
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()