# load_document_list.py

import json
import os
from loguru import logger
from crawler.env import PATH_FILE_DOCUMENT_LIST
from crawler.database.config import session_scope
from crawler.database.service import RawService

def import_from_jsonl():
    # 1. Kiểm tra file tồn tại
    if not os.path.exists(PATH_FILE_DOCUMENT_LIST):
        logger.error(f"Không tìm thấy file JSONL tại: {PATH_FILE_DOCUMENT_LIST}")
        return

    data_to_import = []
    
    # 2. Đọc và parse file JSONL
    try:
        with open(PATH_FILE_DOCUMENT_LIST, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    doc_item = json.loads(line)
                    
                    # Lấy các trường tương ứng với Model Raw
                    # Dựa trên file mẫu: DocId là khóa chính
                    item_id = doc_item.get("DocId")
                    url = doc_item.get("DocUrl")
                    
                    if item_id:
                        data_to_import.append({
                            "item_id": int(item_id),
                            "url": url,
                            # "meta_data": doc_item  # Lưu toàn bộ để sau này cần thì lấy
                        })
                        
                except json.JSONDecodeError:
                    logger.warning(f"Bỏ qua dòng lỗi định dạng: {line[:50]}...")
                    continue
    except Exception as e:
        logger.error(f"Lỗi khi đọc file: {e}")
        return

    if not data_to_import:
        logger.warning("Không có dữ liệu hợp lệ để nạp.")
        return

    # 3. Nạp vào Database thông qua Service
    logger.info(f"Bắt đầu nạp {len(data_to_import)} văn bản vào database...")
    try:
        with session_scope() as db_session:
            service = RawService(db_session)
            service.insert_raw_items(data_to_import)
    except Exception as e:
        logger.error(f"Quy trình nạp dữ liệu thất bại: {e}")

if __name__ == "__main__":
    import_from_jsonl()