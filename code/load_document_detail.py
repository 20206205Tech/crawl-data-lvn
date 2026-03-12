import os
from loguru import logger
from crawler.database.config import session_scope
from crawler.database.service import RawService
from crawler.env import PATH_FOLDER_DATA
from crawler.utils.google_drive import get_drive_service, upload_to_drive

# ID thư mục trên Google Drive để chứa file PDF/HTML
CRAWL_DATA_LVN_GOOGLE_DRIVE_PDF_FOLDER_ID = os.getenv(
    'CRAWL_DATA_LVN_GOOGLE_DRIVE_PDF_FOLDER_ID')


def main():
    # 1. Lấy danh sách file đã tải về (ví dụ: 428114.pdf hoặc 428114.html)
    if not os.path.exists(PATH_FOLDER_DATA):
        logger.error(f"Thư mục dữ liệu không tồn tại: {PATH_FOLDER_DATA}")
        return

    # Lấy các file có định dạng .pdf hoặc .html
    files_to_process = [
        f for f in os.listdir(PATH_FOLDER_DATA)
        if (f.endswith('.html') or f.endswith('.pdf')) and not f.startswith('menu_')
    ]

    if not files_to_process:
        logger.warning("Không tìm thấy file nào trong thư mục data để xử lý.")
        return

    try:
        # 2. Khởi tạo dịch vụ Google Drive
        service_drive = get_drive_service()
        logger.info(
            f"Tìm thấy {len(files_to_process)} file. Bắt đầu tải lên Drive...")

        uploaded_ids = []
        for f in files_to_process:
            full_path = os.path.join(PATH_FOLDER_DATA, f)

            # Tải lên Drive
            # Lưu ý: upload_to_drive cần nhận diện đúng mimetype nếu là PDF
            if upload_to_drive(service_drive, full_path, CRAWL_DATA_LVN_GOOGLE_DRIVE_PDF_FOLDER_ID):
                # Tách ID từ tên file (ví dụ '428114.pdf' -> '428114')
                item_id = os.path.splitext(f)[0]
                uploaded_ids.append(item_id)

                # (Tùy chọn) Xóa file local sau khi upload thành công để tiết kiệm bộ nhớ
                # os.remove(full_path)
            else:
                logger.error(f"Thất bại khi upload file: {f}")

        # 3. Cập nhật trạng thái vào Database
        if uploaded_ids:
            with session_scope() as db_session:
                service_db = RawService(db_session)
                service_db.mark_details_completed(uploaded_ids)

            logger.success(
                f"🎉 Đã upload và cập nhật trạng thái cho {len(uploaded_ids)} bản ghi.")
        else:
            logger.warning("Không có file nào được nạp lên Drive thành công.")

    except Exception as e:
        logger.error(f"Quy trình nạp dữ liệu lên Drive thất bại: {e}")


if __name__ == "__main__":
    main()
