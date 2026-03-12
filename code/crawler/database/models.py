from sqlalchemy import Column, String, DateTime, Index, Integer, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Raw(Base):
    __tablename__ = 'raw'

    # Sử dụng DocId làm primary key (item_id)
    item_id = Column(Integer, primary_key=True) 
    
    # Thêm cột url riêng biệt
    url = Column(String, nullable=True)
    
    # Lưu toàn bộ dữ liệu thô dưới dạng JSONB
    # meta_data = Column(JSONB, nullable=True) 

    # Tracking thời gian crawl detail
    crawl_detail_started_at = Column(DateTime, nullable=True)
    crawl_detail_ended_at = Column(DateTime, nullable=True)

    __table_args__ = (
        # 1. Index cho các item đang chờ crawl Detail
        Index(
            'ix_raw_pending_detail',
            'item_id',
            postgresql_where=(crawl_detail_started_at.is_(None))
        ),
        # 2. Index cho các item đang trong quá trình crawl
        Index(
            'ix_raw_in_progress_detail',
            'crawl_detail_started_at',
            postgresql_where=(
                (crawl_detail_started_at.isnot(None)) & 
                (crawl_detail_ended_at.is_(None))
            )
        ),
    )