import json
import os
from loguru import logger
from sqlalchemy import select

# Import các thành phần từ project của bạn
from crawler.database.config import session_scope
from crawler.database.service import RawService
from crawler.database.models import Raw

# Đường dẫn file theo yêu cầu của bạn
PATH_FILE_JSONL = r"C:\Users\Admin\Desktop\20206205\crawl-data-luatvietnam\data\document_list.jsonl"

def main():
    # 1. Kiểm tra file tồn tại
    if not os.path.exists(PATH_FILE_JSONL):
        logger.error(f"❌ File không tồn tại: {PATH_FILE_JSONL}")
        return

    # 2. Đọc và trích xuất dữ liệu từ file JSONL
    new_data_map = {} # Dùng dict để map item_id -> full_item nhằm tránh trùng trong file
    try:
        logger.info(f"📂 Đang đọc dữ liệu từ: {PATH_FILE_JSONL}")
        with open(PATH_FILE_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    item_id = item.get("DocId")
                    if item_id:
                        # Chuẩn hóa về Integer cho Database
                        new_data_map[int(item_id)] = {
                            "item_id": int(item_id),
                            "url": item.get("DocUrl"),
                            # "meta_data": item
                        }
                except (json.JSONDecodeError, ValueError) as e:
                    continue
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file JSONL: {e}")
        return

    # 3. Kết nối Database để so sánh và lọc dữ liệu mới
    try:
        with session_scope() as db_session:
            # Lấy danh sách ID đã có trong DB
            existing_ids_query = db_session.execute(select(Raw.item_id))
            existing_ids = set(existing_ids_query.scalars().all())

            # Lọc ra những bản ghi thực sự mới
            to_insert = [
                val for vid, val in new_data_map.items() 
                if vid not in existing_ids
            ]
            
            already_exists_count = len(new_data_map) - len(to_insert)

            # 4. In báo cáo so sánh
            print("\n" + "="*50)
            print("📊 BÁO CÁO PHÂN TÍCH DỮ LIỆU")
            print("="*50)
            print(f"Tổng số bản ghi trong file:    {len(new_data_map)}")
            print(f"Số bản ghi đã có trong DB:     {already_exists_count}")
            print(f"Số bản ghi mới cần nạp:        {len(to_insert)}")
            print("-" * 50)

            if not to_insert:
                logger.info("✅ Không có dữ liệu mới để nạp.")
                return

            # Hiển thị 5 bản ghi mẫu
            print("🆕 5 bản ghi mới tiêu biểu:")
            for item in to_insert[:5]:
                print(f" + ID: {item['item_id']} | URL: {item['url']}")

            # 5. Xác nhận và nạp dữ liệu
            confirm = input("\nBạn có muốn nạp các bản ghi mới này vào Database không? (y/n): ")
            if confirm.lower() == 'y':
                service = RawService(db_session)
                # Sử dụng hàm insert_raw_items của service.py đã viết sẵn
                service.insert_raw_items(to_insert)
                logger.success(f"🎉 Thành công! Đã nạp {len(to_insert)} bản ghi mới.")
            else:
                logger.info("🚫 Đã hủy quá trình nạp dữ liệu.")

    except Exception as e:
        logger.error(f"❌ Lỗi xử lý database: {e}")

if __name__ == "__main__":
    main()