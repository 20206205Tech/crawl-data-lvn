# import json
# import os
# import shutil
# import psycopg2
# from psycopg2.extras import DictCursor

# # 1. Cấu hình đường dẫn
# file_jsonl = r"C:\Users\Admin\Desktop\20206205\crawl-data-luatvietnam\data - Copy\detail.jsonl"
# folder_old = r"C:\Users\Admin\Desktop\pdf"
# folder_new = r"C:\Users\Admin\Desktop\new"
# db_url = "postgresql://neondb_owner:**********@ep-odd-snow-a1cq8l8g-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# # Tạo thư mục mới nếu chưa tồn tại
# if not os.path.exists(folder_new):
#     os.makedirs(folder_new)

# def process_files():
#     conn = None
#     try:
#         # 2. Kết nối Database
#         conn = psycopg2.connect(db_url)
#         cur = conn.cursor(cursor_factory=DictCursor)
#         print("--- Đang kết nối Database thành công ---")

#         # 3. Đọc tệp JSONL
#         with open(file_jsonl, 'r', encoding='utf-8') as f:
#             for line in f:
#                 data = json.loads(line.strip())
#                 url_path = data.get("url_path")
#                 old_filename = data.get("pdf_filename")

#                 if not url_path or not old_filename:
#                     continue

#                 # 4. Truy vấn item_id từ bảng raw dựa trên url_path
#                 # Lưu ý: url trong DB khớp với url_path từ file
#                 query = 'SELECT item_id FROM "public"."raw" WHERE "url" = %s LIMIT 1;'
#                 cur.execute(query, (url_path,))
#                 result = cur.fetchone()

#                 if result:
#                     item_id = result['item_id']
#                     new_filename = f"{item_id}.pdf"
                    
#                     path_old_file = os.path.join(folder_old, old_filename)
#                     path_new_file = os.path.join(folder_new, new_filename)

#                     # 5. Thực hiện Copy và đổi tên
#                     if os.path.exists(path_old_file):
#                         shutil.copy2(path_old_file, path_new_file)
#                         print(f"Thành công: {old_filename} -> {new_filename} (ID: {item_id})")
#                     else:
#                         print(f"Cảnh báo: Không tìm thấy file gốc {old_filename}")
#                 else:
#                     print(f"Lỗi: Không tìm thấy URL trong DB: {url_path}")

#     except Exception as e:
#         print(f"Đã xảy ra lỗi: {e}")
#     finally:
#         if conn:
#             cur.close()
#             conn.close()
#             print("--- Đã đóng kết nối Database ---")

# if __name__ == "__main__":
#     process_files()