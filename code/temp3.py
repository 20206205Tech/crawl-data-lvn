from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# 1. Thông tin cấu hình
DATABASE_URL = 'postgresql://neondb_owner:**********@ep-odd-snow-a1cq8l8g-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'





def update_crawl_time():
    # 2. Tính toán mốc thời gian (10 ngày trước)
    ten_days_ago = datetime.now() - timedelta(days=10)
    
    # 3. Tạo kết nối
    engine = create_engine(DATABASE_URL)
    
    # 4. Thực thi câu lệnh SQL
    query = text("""
        UPDATE "raw"
        SET crawl_detail_started_at = :new_date
    """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"new_date": ten_days_ago})
            connection.commit()
            print(f"Thành công! Đã cập nhật {result.rowcount} dòng.")
            print(f"Thời gian đã đặt: {ten_days_ago}")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

if __name__ == "__main__":
    update_crawl_time()