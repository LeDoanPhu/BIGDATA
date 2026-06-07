🚀 Distributed Big Data Pipeline & Real-time Analytics
HadoopSparkKafkaPython

📌 Giới thiệu dự án (Project Overview)
Dự án này là một hệ thống Data Pipeline phân tán hoàn chỉnh từ đầu đến cuối (End-to-End), được thiết kế để thu thập, xử lý, làm sạch và phân tích khối lượng lớn dữ liệu. Hệ thống hỗ trợ cả xử lý theo lô (Batch Processing) và xử lý luồng thời gian thực (Real-time Streaming).

Điểm nổi bật của dự án là việc thiết lập thành công một Cụm máy chủ phân tán (Distributed Cluster) gồm 3 máy tính vật lý độc lập kết nối với nhau thông qua mạng riêng ảo (Tailscale VPN), mô phỏng hoàn hảo môi trường làm việc thực tế tại các doanh nghiệp.

🏗️ Kiến trúc Hệ thống (Architecture)
Mermaid diagram
🛠️ Công nghệ sử dụng (Tech Stack)
Hạ tầng (Infrastructure): Cụm 3 Node (1 Master, 2 Workers) liên kết qua Tailscale VPN.
Lưu trữ phân tán (Storage): Hadoop Distributed File System (HDFS).
Điều phối tài nguyên (Resource Manager): Hadoop YARN.
Xử lý Dữ liệu Lô (Batch Processing): Apache Spark Core, Spark SQL, PySpark.
Luồng Dữ liệu Thực (Streaming): Apache Kafka, Spark Streaming.
Khám phá Dữ liệu & AI: Jupyter Notebook, Pandas, Scikit-learn (ML).
📂 Cấu trúc thư mục (Directory Structure)
text

📦bigdata-analytics-pipeline
 ┣ 📂data/               # Thư mục chứa sample data test cục bộ
 ┣ 📂notebooks/          # Chứa các file Jupyter (Initial EDA, Insight Reports)
 ┣ 📂src/                # Mã nguồn chính của hệ thống
 ┃ ┣ 📂batch/            # ETL Scripts: spark-submit jobs làm sạch dữ liệu
 ┃ ┣ 📂streaming/        # Realtime Scripts: Kafka producer & Spark Streaming
 ┃ ┣ 📂ml/               # Machine Learning: Traning models & Predictions
 ┃ ┣ 📂config/           # Cấu hình IP cluster, Kafka topics
 ┃ ┗ 📂utils/            # Helper functions (Spark session builder)
 ┣ 📂scripts/            # Các file .cmd/.sh tự động hóa quá trình chạy jobs
 ┣ 📜.gitignore          # Quy tắc bỏ qua file data lớn khi push Github
 ┣ 📜requirements.txt    # Danh sách thư viện Python
 ┗ 📜README.md
🚀 Tính năng nổi bật (Key Features)
Khả năng chịu lỗi cao (Fault Tolerance): Cấu hình Replication = 3 trên HDFS đảm bảo dữ liệu an toàn tuyệt đối ngay cả khi 1-2 node trong cụm bị sập nguồn.
Xử lý song song (Data Locality): Áp dụng YARN để phân chia công việc xử lý dữ liệu ngay tại máy chứa dữ liệu, loại bỏ độ trễ truyền tải mạng.
Automated ETL Pipeline: Kịch bản Python tự động trích xuất dữ liệu rác (Raw Zone), làm sạch (Transform) và lưu trữ dưới định dạng nén Parquet (Clean Zone).
Real-time Analytics: Tích hợp Kafka hứng dữ liệu sống và Spark Streaming để phân tích, cho phép phát hiện bất thường ngay thời gian thực.
💻 Hướng dẫn chạy dự án (How to run)
1. Khởi động Cụm Hadoop & Kafka (Trên máy Master):

bash

# Khởi động HDFS và YARN
start-dfs.cmd
start-yarn.cmd
# Khởi động Kafka Zookeeper và Server
zookeeper-server-start.bat config\zookeeper.properties
kafka-server-start.bat config\server.properties
2. Tham gia Cụm (Trên máy Worker):

bash

hdfs datanode
yarn nodemanager
3. Chạy Pipeline Làm sạch Dữ liệu (ETL):

bash

spark-submit src/batch/clean_data.py
4. Xem báo cáo EDA: Mở thư mục notebooks/ và khởi chạy jupyter notebook để xem các báo cáo phân tích trực quan.

Dự án được phát triển trong khuôn khổ Đồ án môn học Big Data - [2026]. Tác giả: Nhóm 5 - BigData bao gồm 3 thành viên:Lê Doãn Phú, Nguyễn Kiều Minh Trí, Nguyễn Khánh Hoàng 
