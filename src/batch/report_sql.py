import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Tự động đồng bộ đường dẫn dự án để tránh lỗi Import
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Danh sách các cột số cần ép kiểu dữ liệu
NUMERIC_FIELDS = ['family_income', 'sleep_hours', 'stress_level', 'attendance_rate', 'math_score', 'reading_score',
                  'writing_score']


def clean_and_transform_data(df):
    """
    Hàm xử lý và làm sạch dữ liệu (ETL) bằng toán tử thuần dòng (Row-level Operations)
    để không làm mất dữ liệu gốc khi thực hiện phân tích đa chiều.
    """
    print("[*] Tiến trình ETL: Bắt đầu kiểm tra và đồng bộ dữ liệu...")

    # 1. Loại bỏ các bản ghi trùng lặp tuyệt đối
    df = df.dropDuplicates()

    # 2. Ép kiểu dữ liệu số về định dạng Double để tính toán không bị lệch
    for col_name in NUMERIC_FIELDS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))

    # 3. Đồng bộ tên cột kết quả học tập (Đổi final_result thành pass_fail nếu có)
    if 'final_result' in df.columns and 'pass_fail' not in df.columns:
        df = df.withColumnRenamed('final_result', 'pass_fail')

    # 4. Điền giá trị mặc định an toàn để các bảng không bị rỗng/Null khi chưa có data real-time đổ về đầy đủ
    df = df.na.fill({
        'family_income': 50000.0,
        'sleep_hours': 7.0,
        'stress_level': 5.0,
        'math_score': 60.0,
        'reading_score': 60.0,
        'writing_score': 60.0,
        'daily_study_hours': 3.0,
        'pass_fail': 'pass'
    })

    print("[✓] ETL hoàn tất! Dữ liệu đạt trạng thái sẵn sàng truy vấn.")
    return df


def main():
    print("=========================================================================")
    print("=== CHƯƠNG 5: HỆ THỐNG TRUY VẤN ĐA CHIỀU PHỨC TẠP VỚI SPARK SQL ===")
    print("=========================================================================")

    # Địa chỉ IP cụm HDFS chung từ giao diện NameNode ông vừa mở
    HDFS_URL = "hdfs://100.109.78.104:9000"

    # Đường dẫn chính xác đến thư mục kết quả Streaming của Hoàng
    FILE_HDFS_PATH = f"{HDFS_URL}/stream_output"

    # Khởi tạo Spark Session cấu hình nạp mặc định FileSystem của Hadoop
    spark = SparkSession.builder \
        .appName("Spark_HDFS_SQL_Report") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .getOrCreate()

    # TẮT SẠCH CÁC CẢNH BÁO RÁC (WARNING/INFO) ĐỂ MÀN HÌNH IN KẾT QUẢ ĐẸP
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"[*] Đang kết nối mạng và nạp dữ liệu Parquet từ HDFS: {FILE_HDFS_PATH}")

        # 🔥 CẤU HÌNH BẮT BUỘC 1: Tắt phân biệt chữ hoa chữ thường để tự động map chính xác tên cột từ file Parquet
        spark.conf.set("spark.sql.caseSensitive", "false")

        # ⚙️ CẤU HÌNH BẮT BUỘC 2: Định nghĩa cấu trúc Schema để cứu cánh hệ thống khi file trên HDFS đang trống hoặc rỗng byte dữ liệu
        custom_schema = T.StructType([
            T.StructField("gender", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("parental_education_level", T.StringType(), True),
            T.StructField("family_income", T.DoubleType(), True),
            T.StructField("sleep_hours", T.DoubleType(), True),
            T.StructField("stress_level", T.DoubleType(), True),
            T.StructField("attendance_rate", T.DoubleType(), True),
            T.StructField("math_score", T.DoubleType(), True),
            T.StructField("reading_score", T.DoubleType(), True),
            T.StructField("writing_score", T.DoubleType(), True),
            T.StructField("daily_study_hours", T.DoubleType(), True),
            T.StructField("pass_fail", T.StringType(), True)
        ])

        # Đọc dữ liệu từ HDFS kết hợp cả Ép Schema bảo hiểm + Tắt Case Sensitive
        df_raw = spark.read.schema(custom_schema).parquet(FILE_HDFS_PATH)

        # Chuyển toàn bộ tên cột về chữ thường ngay từ đầu để đồng bộ logic
        for col_name in df_raw.columns:
            df_raw = df_raw.withColumnRenamed(col_name, col_name.lower())

        # Tiến hành chạy hàm làm sạch dữ liệu dựa trên các cột đã chuẩn hóa
        df_cleaned = clean_and_transform_data(df_raw)

        # Đăng ký bảng ảo 'student_stream'
        df_cleaned.createOrReplaceTempView("student_stream")
        print("\n[🚀] HDFS Connected thành công! Bắt đầu thực thi 10 truy vấn đa chiều...")

        # -------------------------------------------------------------------------
        # CÂU 1
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 1: Tính điểm trung bình, độ chênh lệch giữa các môn và xếp hạng phần trăm toàn trường ---")
        q1 = """
            WITH score_variance_table AS (
                SELECT 
                    gender, age,
                    (math_score + reading_score + writing_score) / 3 AS diem_trung_bin,
                    GREATEST(math_score, reading_score, writing_score) - LEAST(math_score, reading_score, writing_score) AS do_lech_max_min
                FROM student_stream
            )
            SELECT 
                gender, age,
                ROUND(diem_trung_bin, 2) AS diem_tb,
                ROUND(do_lech_max_min, 2) AS chenh_lech_mon,
                ROUND(CUME_DIST() OVER (ORDER BY diem_trung_bin DESC), 4) AS phan_vi_tich_luy_toan_truong
            FROM score_variance_table
            LIMIT 15
        """
        spark.sql(q1).show()

        # -------------------------------------------------------------------------
        # CÂU 2
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 2: Chia nhóm thu nhập gia đình (Giàu/Khá/Trung bình/Nghèo) và xem mức độ Stress của từng nhóm ---")
        q2 = """
            WITH rank_income_table AS (
                SELECT 
                    family_income, stress_level, math_score,
                    PERCENT_RANK() OVER (ORDER BY family_income DESC) AS thu_hang_tai_chinh
                FROM student_stream
            )
            SELECT 
                CASE 
                    WHEN thu_hang_tai_chinh <= 0.25 THEN '1. Thu nhap Thuong luu (Top 25%)'
                    WHEN thu_hang_tai_chinh <= 0.50 THEN '2. Thu nhap Kha'
                    WHEN thu_hang_tai_chinh <= 0.75 THEN '3. Thu nhap Trung binh'
                    ELSE '4. Thu nhap Thap (Bottom 25%)'
                END AS phan_khuc_kinh_te,
                COUNT(*) AS tong_sinh_vien,
                ROUND(AVG(math_score), 2) AS diem_toan_tb,
                CONCAT('Stress TB: ', ROUND(AVG(stress_level), 2)) AS chi_so_stress_phat_sinh
            FROM rank_income_table
            GROUP BY 
                CASE 
                    WHEN thu_hang_tai_chinh <= 0.25 THEN '1. Thu nhap Thuong luu (Top 25%)'
                    WHEN thu_hang_tai_chinh <= 0.50 THEN '2. Thu nhap Kha'
                    WHEN thu_hang_tai_chinh <= 0.75 THEN '3. Thu nhap Trung binh'
                    ELSE '4. Thu nhap Thap (Bottom 25%)'
                END
            ORDER BY phan_khuc_kinh_te
        """
        spark.sql(q2).show()

        # -------------------------------------------------------------------------
        # CÂU 3
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 3: Tìm Top 3 học sinh điểm Toán cao nhất theo từng nhóm Kết quả (Đậu/Rớt) và Giới tính ---")
        q3 = """
            SELECT * FROM (
                SELECT 
                    COALESCE(pass_fail, 'unknown') AS ket_qua_hoc_tap, 
                    gender AS gioi_tinh, 
                    age AS tuoi,
                    math_score AS diem_toan, 
                    DENSE_RANK() OVER (
                        PARTITION BY pass_fail, gender 
                        ORDER BY math_score DESC, daily_study_hours ASC
                    ) AS xep_hang_noi_bo
                FROM student_stream
            ) WHERE xep_hang_noi_bo <= 3
        """
        spark.sql(q3).show()

        # -------------------------------------------------------------------------
        # CÂU 4
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 4: Tìm học sinh học hiệu quả nhất (Điểm Toán cao nhất tính trên mỗi giờ tự học) theo từng Độ tuổi ---")
        q4 = """
            SELECT * FROM (
                SELECT 
                    age AS tuoi,
                    gender AS gioi_tinh,
                    math_score AS diem_toan,
                    daily_study_hours AS gio_hoc,
                    ROUND(math_score / NULLIF(daily_study_hours, 0), 2) AS hieu_suat_hoc_tap,
                    ROW_NUMBER() OVER (
                        PARTITION BY age 
                        ORDER BY (math_score / NULLIF(daily_study_hours, 0)) DESC
                    ) AS hang_nhat_hieu_suat
                FROM student_stream
            ) WHERE hang_nhat_hieu_suat = 1
        """
        spark.sql(q4).show()

        # -------------------------------------------------------------------------
        # CÂU 5
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 5: Tính toán điểm trung bình trượt môn Đọc của nhóm 3 học sinh liên tiếp dựa theo số giờ học tăng dần ---")
        q5 = """
            SELECT 
                age AS tuoi,
                daily_study_hours AS gio_hoc,
                reading_score AS diem_doc_ca_nhan,
                ROUND(AVG(reading_score) OVER (
                    PARTITION BY age 
                    ORDER BY daily_study_hours ASC 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) AS trung_binh_truot_3_sinh_vien
            FROM student_stream
            LIMIT 15
        """
        spark.sql(q5).show()

        # -------------------------------------------------------------------------
        # CÂU 6
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 6: So sánh điểm môn Đọc của mỗi học sinh với mức trung bình của nhóm Học vấn cha mẹ và nhóm Giới tính ---")
        q6 = """
            SELECT 
                parental_education_level AS hoc_van_cha_me,
                gender AS gioi_tinh,
                reading_score AS diem_doc,
                ROUND(AVG(reading_score) OVER (PARTITION BY parental_education_level), 2) AS tb_theo_hoc_van,
                ROUND(AVG(reading_score) OVER (PARTITION BY gender), 2) AS tb_theo_gioi_tinh,
                ROUND(reading_score - AVG(reading_score) OVER (PARTITION BY parental_education_level), 2) AS lech_voi_hoc_van,
                ROUND(reading_score - AVG(reading_score) OVER (PARTITION BY gender), 2) AS lech_voi_gioi_tinh
            FROM student_stream
            LIMIT 15
        """
        spark.sql(q6).show()

        # -------------------------------------------------------------------------
        # CÂU 7
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 7: Sử dụng chỉ số Z-Score để phát hiện những học sinh có số giờ ngủ bất thường (quá ít hoặc quá nhiều) theo tuổi ---")
        q7 = """
            WITH statistical_sleep_table AS (
                SELECT 
                    age AS tuoi,
                    sleep_hours AS gio_ngu,
                    AVG(sleep_hours) OVER (PARTITION BY age) AS gio_ngu_tb,
                    STDDEV(sleep_hours) OVER (PARTITION BY age) AS do_lech_chuan_ngu
                FROM student_stream
            )
            SELECT 
                tuoi,
                ROUND(gio_ngu, 2) AS gio_ngu_ca_nhan,
                ROUND(gio_ngu_tb, 2) AS muc_tb_cung_tuoi,
                ROUND(do_lech_chuan_ngu, 4) AS do_lech_chuan,
                ROUND((gio_ngu - gio_ngu_tb) / COALESCE(NULLIF(do_lech_chuan_ngu, 0), 1), 2) AS chi_so_z_score_ngu
            FROM statistical_sleep_table
            LIMIT 15
        """
        spark.sql(q7).show()

        # -------------------------------------------------------------------------
        # CÂU 8
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 8: Thống kê điểm môn Viết cao nhất (Thủ khoa) và thấp nhất (Vĩ khoa) ứng với từng mức độ Stress ---")
        q8 = """
            SELECT 
                stress_level AS muc_stress,
                writing_score AS diem_viet_ca_nhan,
                CONCAT('Thu khoa: ', FIRST_VALUE(writing_score) OVER (
                    PARTITION BY stress_level 
                    ORDER BY writing_score DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )) AS thong_tin_thu_khoa,
                CONCAT('Vi khoa: ', LAST_VALUE(writing_score) OVER (
                    PARTITION BY stress_level 
                    ORDER BY writing_score DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )) AS thong_tin_vi_khoa
            FROM student_stream
            LIMIT 15
        """
        spark.sql(q8).show()

        # -------------------------------------------------------------------------
        # CÂU 9
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 9: So sánh điểm Toán của học sinh hiện tại với học sinh có số giờ học liền trước và học sinh liền sau ---")
        q9 = """
            SELECT 
                age AS tuoi,
                daily_study_hours AS gio_hoc,
                math_score AS diem_toan,
                COALESCE(LAG(math_score, 1) OVER (PARTITION BY age ORDER BY daily_study_hours ASC), math_score) AS diem_ban_ghi_truoc,
                COALESCE(LEAD(math_score, 1) OVER (PARTITION BY age ORDER BY daily_study_hours ASC), math_score) AS diem_ban_ghi_sau,
                ROUND(math_score - COALESCE(LAG(math_score, 1) OVER (PARTITION BY age ORDER BY daily_study_hours ASC), math_score), 2) AS muc_tang_giam_tinh_tien
            FROM student_stream
            WHERE age IN (17, 18)
            LIMIT 15
        """
        spark.sql(q9).show()

        # -------------------------------------------------------------------------
        # CÂU 10
        # -------------------------------------------------------------------------
        print(
            "\n--- [THỰC THI] CÂU TRUY VẤN 10: Tính toán tổng điểm áp lực tâm lý (kết hợp giữa Stress và Thiếu ngủ) để xếp hạng học sinh chịu áp lực lớn nhất theo tuổi ---")
        q10 = """
            WITH stress_index_table AS (
                SELECT 
                    age AS tuoi,
                    math_score AS diem_toan,
                    ROUND(SQRT(POW(stress_level, 2) + POW(sleep_hours, 2)), 2) AS chi_so_tai_tam_ly
                FROM student_stream
            )
            SELECT 
                tuoi,
                diem_toan,
                chi_so_tai_tam_ly,
                RANK() OVER (PARTITION BY tuoi ORDER BY chi_so_tai_tam_ly DESC, diem_toan ASC) AS xep_hang_ganh_ap_luc
            FROM stress_index_table
            LIMIT 15
        """
        spark.sql(q10).show()

    except Exception as e:
        print(f"❌ LỖI HỆ THỐNG KHI TRUY VẤN HDFS: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
