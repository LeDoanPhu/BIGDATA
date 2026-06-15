# 🚀 Distributed Big Data Pipeline: Batch & Simulated Real-time Analytics

![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.4.3-yellow?style=for-the-badge&logo=apachehadoop)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-orange?style=for-the-badge&logo=apachespark)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.2-black?style=for-the-badge&logo=apachekafka)
![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python)

## 📌 Giới thiệu dự án (Project Overview)
Dự án này là một hệ thống **Data Pipeline phân tán** hoàn chỉnh từ đầu đến cuối (End-to-End), được thiết kế để lưu trữ và xử lý khối lượng lớn dữ liệu phân tán trên HDFS. 

Hệ thống được thiết kế với 2 luồng xử lý song song mang tính ứng dụng thực tế cao:
1. **Batch Processing (Xử lý theo Lô):** Xử lý lượng dữ liệu khổng lồ tĩnh bằng PySpark SQL để làm sạch (ETL Pipeline) và đẩy vào huấn luyện Machine Learning.
2. **Simulated Real-time Streaming (Xử lý Thời gian thực giả lập):** Áp dụng kỹ thuật dùng kịch bản **Python Producer** đọc file CSV tĩnh và "bắn" từng dòng dữ liệu vào **Apache Kafka** liên tục từng giây. **Spark Streaming** sẽ đóng vai trò Consumer lắng nghe Kafka và tính toán ngay lập tức, biến dữ liệu tĩnh thành một luồng dữ liệu sống động y như trong các tập đoàn lớn.

Tất cả được vận hành trên một **Cụm máy chủ (Distributed Cluster)** gồm 3 máy tính độc lập liên kết qua mạng riêng ảo (Tailscale VPN) và được quản lý tài nguyên chặt chẽ bởi YARN.

## 🏗️ Kiến trúc Hệ thống (Architecture Diagram)

```mermaid
graph LR
    subgraph Data Sources
        A1[Static CSV files]
        A2[Python Script <br> Đọc CSV từng dòng]
    end

    subgraph Data Ingestion
        B1[(HDFS Raw Zone)]
        B2[Apache Kafka <br> Topic: student_stream]
    end

    subgraph Distributed Processing
        C1[PySpark SQL / Batch ETL]
        C2[Spark Streaming <br> Consumer]
    end

    subgraph Storage & Analytics
        D1[(HDFS Clean Zone)]
        E1[Jupyter EDA & BI Reports]
        E2[Machine Learning Models]
        E3[Real-time Console / Dashboard]
    end

    %% Luồng Batch
    A1 --"Lưu trữ thô"--> B1
    B1 --"Extract"--> C1
    C1 --"Transform & Load"--> D1
    
    %% Luồng Streaming
    A2 --"Bắn data từng giây (Producer)"--> B2
    B2 --"Hút data liên tục"--> C2
    C2 --"Xử lý trực tiếp"--> E3
    
    %% Phân tích & AI
    D1 --> E1
    D1 --> E2
```

## 🛠️ Công nghệ sử dụng (Tech Stack)
* **Hạ tầng (Infrastructure):** Cụm 3 Node (1 Master, 2 Workers) liên kết qua Tailscale VPN.
* **Lưu trữ phân tán (Storage):** Hadoop Distributed File System (HDFS).
* **Điều phối tài nguyên (Resource Manager):** Hadoop YARN.
* **Xử lý Dữ liệu Lô (Batch ETL):** Apache Spark Core, Spark SQL, PySpark.
* **Xử lý Luồng (Real-time Streaming):** Apache Kafka & Spark Structured Streaming.
* **Khám phá Dữ liệu & AI:** Jupyter Notebook, Pandas, Scikit-learn (ML).

## 📂 Cấu trúc thư mục (Directory Structure)
```text
📦BIGDATA
 ┣ 📂data/               # Thư mục chứa sample data test cục bộ
 ┣ 📂notebooks/          # Chứa các file Jupyter (Initial EDA, Insight Reports)
 ┣ 📂src/                # MÃ NGUỒN CHÍNH CỦA HỆ THỐNG
 ┃ ┣ 📂batch/            # Nơi chứa luồng ETL
 ┃ ┃ ┣ 📄clean_data.py   # Làm sạch lô lớn
 ┃ ┃ ┗ 📄report_sql.py   # Viết SQL báo cáo tĩnh
 ┃ ┣ 📂config/           # Nơi chứa cấu hình
 ┃ ┃ ┗ 📄settings.py     # Chứa IP của HDFS và Kafka 
 ┃ ┣ 📂utils/            # Nơi chứa công cụ dùng chung
 ┃ ┃ ┗ 📄spark_session.py# Chứa hàm khởi tạo kết nối Spark
 ┃ ┣ 📂ml/               # Nơi chứa Machine Learning
 ┃ ┃ ┣ 📄train_model.py  # Dạy AI
 ┃ ┃ ┗ 📄predict.py      # Dùng AI dự đoán
 ┃ ┗ 📂streaming/        # Nơi chứa luồng Real-time giả lập
 ┃   ┣ 📄kafka_producer.py  # Đọc CSV ném vào Kafka từng giây
 ┃   ┗ 📄spark_consumer.py  # Spark hút data từ Kafka về xử lý trực tiếp
 ┣ 📜.gitignore          # Quy tắc bỏ qua file data lớn khi push Github
 ┣ 📜requirements.txt    # Danh sách thư viện Python
 ┗ 📜README.md
```

## 🚀 Tính năng nổi bật (Key Features)
1. **Khả năng chịu lỗi cao (Fault Tolerance):** Cấu hình `Replication = 3` trên HDFS đảm bảo dữ liệu an toàn tuyệt đối ngay cả khi 1-2 node trong cụm bị sập.
2. **Xử lý song song (Data Locality):** Áp dụng YARN để phân chia công việc xử lý dữ liệu ngay tại máy chứa dữ liệu, tối ưu hóa tốc độ tính toán phân tán.
3. **Automated ETL Pipeline:** Xây dựng luồng PySpark tự động trích xuất dữ liệu rác, làm sạch (Transform) và lưu trữ thành dạng Parquet.
4. **Real-time Kafka Integration:** Đột phá với kịch bản giả lập Real-time Producer bắn dữ liệu liên tục vào Kafka, kết hợp với Spark Consumer để tính toán và phát hiện thông tin tức thời.

## 💻 Hướng dẫn chạy dự án (How to run)

**1. Khởi động Cụm Hadoop & Kafka (Trên máy Master):**
```bash
start-dfs.cmd
start-yarn.cmd
# Bật Kafka (Zookeeper & Server)
```

**2. Tham gia Cụm (Trên máy Worker):**
```bash
hdfs datanode
yarn nodemanager
```

**3. Chạy luồng Batch (Làm sạch Data & Học máy):**
```bash
spark-submit src/batch/clean_data.py
spark-submit src/ml/train_model.py
```

**4. Chạy luồng Streaming (Giả lập Real-time):**
```bash
# Bật Terminal 1: Khởi động Spark Consumer để chờ hút dữ liệu
spark-submit src/streaming/spark_consumer.py

# Bật Terminal 2: Kích hoạt Máy bắn bóng (Producer) bắn file CSV
python src/streaming/kafka_producer.py
```

---
*Dự án được phát triển trong khuôn khổ Đồ án môn học Big Data - [06/2026].*
*Tác giả: Nhóm 7 - Lê Doãn Phú,
                   Nguyễn Kiều Minh Trí,
                   Nguyễn Khánh Hoàng*
