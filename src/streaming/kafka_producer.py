
import time
import json
import subprocess
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# ==========================================
# 🛠️ CẤU HÌNH KAFKA VÀ ĐƯỜNG DẪN HDFS
# ==========================================
# Đường dẫn chính xác của file CSV đang nằm TRÊN HDFS
HDFS_SOURCE_CSV = "/user/hadoop/data/raw_large_data.csv" 
KAFKA_TOPIC = "streaming_data_topic"

# Thay thế bằng IP Tailscale của máy Master (máy chạy Kafka Broker)
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] 

CHUNK_SIZE = 500  # Đọc mỗi lần 500 dòng từ luồng HDFS để tránh tràn RAM
SLEEP_TIME = 2    # Tần suất bắn dữ liệu sang Kafka: 2 giây

def json_serializer(data):
    """Chuyển đổi dữ liệu sang định dạng JSON byte để gửi qua Kafka"""
    return json.dumps(data).encode("utf-8")

def run_kafka_producer():
    print(f"🚀 Bắt đầu đọc stream dữ liệu từ HDFS: '{HDFS_SOURCE_CSV}'")
    print(f"📡 Tiến hành bắn dữ liệu lên Kafka topic: '{KAFKA_TOPIC}'")
    
    try:
        # 1. Khởi tạo Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}. Bạn đã khởi động Kafka Server chưa?")
        return

    # 2. Sử dụng lệnh 'hdfs dfs -cat' để truyền luồng dữ liệu từ HDFS vào Python
    hdfs_cmd = ["hdfs", "dfs", "-cat", HDFS_SOURCE_CSV]
    
    try:
        # Chạy lệnh hdfs và bắt lấy đầu ra (stdout) của nó
        process = subprocess.Popen(hdfs_cmd, stdout=subprocess.PIPE, text=True, encoding='utf-8')
        
        # 3. Đọc trực tiếp từ luồng stdout của HDFS bằng Pandas chunksize
        for i, chunk in enumerate(pd.read_csv(process.stdout, chunksize=CHUNK_SIZE)):
            
            # Chuyển đổi DataFrame chunk thành list các dict (từng dòng dữ liệu)
            records = chunk.to_dict(orient="records")
            
            # Bắn từng dòng lên Kafka topic
            for record in records:
                record['producer_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                producer.send(KAFKA_TOPIC, value=record)
            
            # Ép Kafka đẩy hết dữ liệu trong buffer đi
            producer.flush()
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Đã đọc từ HDFS & bắn {len(records)} tin nhắn (chunk {i}) lên Kafka.")
            
            # Nghỉ 2 giây trước khi đọc chunk tiếp theo
            time.sleep(SLEEP_TIME)
            
        print("🎉 Đã hoàn thành việc truyền toàn bộ dữ liệu từ HDFS sang Kafka!")

    except KeyboardInterrupt:
        print("\n🛑 Đã chủ động dừng Kafka Producer.")
    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}")
    finally:
        # Dọn dẹp tiến trình và đóng kết nối
        if 'process' in locals():
            process.kill()
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    run_kafka_producer()