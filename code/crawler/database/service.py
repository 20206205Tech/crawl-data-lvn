from sqlalchemy.orm import Session
from sqlalchemy import select, update
from datetime import datetime, timedelta
from loguru import logger
from .models import Raw
from sqlalchemy.dialects.postgresql import insert


class RawService:
    def __init__(self, db: Session):
        self.db = db

    def insert_raw_items(self, items_data: list):
        """
        items_data: List các dict chứa {'item_id': ..., 'url': ..., 'meta_data': ...}
        """
        if not items_data:
            return

        try:
            # Tạo câu lệnh INSERT với ON CONFLICT DO NOTHING dựa trên item_id
            stmt = insert(Raw).values(items_data)
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=['item_id']
            )

            self.db.execute(on_conflict_stmt)
            # Session commit sẽ được xử lý bởi session_scope hoặc tại đây
            self.db.commit()
            logger.success(
                f"✅ Đã nạp thành công {len(items_data)} bản ghi vào DB.")
        except Exception as e:
            logger.error(f"❌ Lỗi insert batch: {e}")
            self.db.rollback()

    def fetch_pending_details(self, limit=10):
        """Lấy danh sách các item chưa crawl và đánh dấu thời gian bắt đầu (Atomic)"""
        try:
            # Subquery tìm các item chưa có crawl_detail_started_at
            subquery = (
                select(Raw.item_id)
                .where(Raw.crawl_detail_started_at.is_(None))
                .limit(limit)
                .with_for_update(skip_locked=True)
                .scalar_subquery()
            )

            # Update thời gian bắt đầu và trả về item_id kèm url
            stmt = (
                update(Raw)
                .where(Raw.item_id.in_(subquery))
                .values(crawl_detail_started_at=datetime.now())
                .returning(Raw.item_id, Raw.url)
            )

            result = self.db.execute(stmt)
            rows = result.all()  # Trả về list các Row(item_id, url)
            self.db.commit()
            return rows
        except Exception as e:
            logger.error(f"Lỗi fetch_pending_details: {e}")
            self.db.rollback()
            return []

    def mark_details_completed(self, item_ids):
        try:
            if not item_ids:
                return

            # Nếu truyền vào 1 string/int đơn lẻ, bọc nó vào list
            if not isinstance(item_ids, (list, tuple)):
                item_ids = [item_ids]

            self.db.execute(
                update(Raw)
                .where(Raw.item_id.in_(item_ids))
                .values(crawl_detail_ended_at=datetime.now())
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"Lỗi cập nhật trạng thái: {e}")
            self.db.rollback() 




    # def release_stuck_tasks(self, hours_stuck: int = 12):
    #     """
    #     Giải phóng các bản ghi bị kẹt (đã start nhưng chưa end sau `hours_stuck` giờ).
    #     Đưa crawl_detail_started_at về None để quét lại.
    #     """
    #     try:
    #         threshold_time = datetime.now() - timedelta(hours=hours_stuck)
            
    #         stmt = (
    #             update(Raw)
    #             .where(
    #                 Raw.crawl_detail_started_at.isnot(None),
    #                 Raw.crawl_detail_ended_at.is_(None),
    #                 Raw.crawl_detail_started_at <= threshold_time
    #             )
    #             .values(crawl_detail_started_at=None)
    #         )
            
    #         result = self.db.execute(stmt)
    #         self.db.commit()
            
    #         released_count = result.rowcount
    #         if released_count > 0:
    #             logger.success(f"🔄 Đã giải phóng {released_count} bản ghi bị kẹt trên {hours_stuck} giờ.")
    #         else:
    #             logger.info(f"✨ Không có bản ghi nào bị kẹt quá {hours_stuck} giờ.")
                
    #         return released_count
            
    #     except Exception as e:
    #         logger.error(f"❌ Lỗi khi giải phóng task bị kẹt: {e}")
    #         self.db.rollback()
    #         return 0