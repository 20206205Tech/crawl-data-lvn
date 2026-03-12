import scrapy
from scrapy.utils.response import open_in_browser
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from loguru import logger
from crawler.env import CRAWL_DATA_ENV_DEV, PATH_FOLDER_DATA
from crawler.database.config import session_scope
from crawler.database.service import RawService
from crawler.utils.auth import get_authenticated_session


class DocumentDetailSpider(scrapy.Spider):
    name = "document_detail"
    allowed_domains = ["luatvietnam.vn"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        os.makedirs(PATH_FOLDER_DATA, exist_ok=True)

        self.auth_session = get_authenticated_session()
        self.cookies = self.auth_session.cookies.get_dict()
        logger.info(f"🔑 Session khởi tạo với {len(self.cookies)} cookies.")

    def start_requests(self):
        limit = 2 if CRAWL_DATA_ENV_DEV else 20
        # limit = 2 if CRAWL_DATA_ENV_DEV else 2

        with session_scope() as db_session:
            service = RawService(db_session)
            pending_items = service.fetch_pending_details(limit=limit)

        if not pending_items:
            logger.info("🏁 Không còn bản ghi nào cần crawl.")
            return

        for item in pending_items:
            # item là Row(item_id, url)
            full_url = urljoin("https://luatvietnam.vn", item.url)

            yield scrapy.Request(
                url=full_url,
                callback=self.parse_detail,
                cookies=self.cookies,
                meta={'item_id': item.item_id}
            )

    def parse_detail(self, response):

        if CRAWL_DATA_ENV_DEV:
            open_in_browser(response)

        item_id = response.meta['item_id']

        if "đăng nhập" in response.text.lower() and "customername" in response.text.lower():
            logger.error(f"❌ Session hết hạn khi crawl ItemID: {item_id}")
            return

        try:
            # soup = BeautifulSoup(response.text, 'html.parser')
            # # Tìm link PDF trong div list-download (theo cấu trúc trang)
            # download_div = soup.find('div', class_='list-download')
            # pdf_link_tag = None

            # if download_div:
            #     pdf_link_tag = download_div.find('a', href=lambda x: x and '.pdf' in x.lower())

            # if pdf_link_tag:
            #     pdf_url = urljoin(response.url, pdf_link_tag.get('href'))
            #     logger.info(f"📄 Tìm thấy PDF cho {item_id}: {pdf_url}")

            soup = BeautifulSoup(response.text, 'html.parser')
            download_div = soup.find('div', class_='list-download')

            if download_div:
                pdf_link_tag = download_div.find(
                    'a', title=lambda x: x and 'pdf' in x.lower())
                if not pdf_link_tag:
                    pdf_link_tag = download_div.find(
                        'a', href=lambda x: x and '.pdf' in x.lower())

                if pdf_link_tag:
                    relative_pdf_url = pdf_link_tag.get('href')
                    full_pdf_url = urljoin(response.url, relative_pdf_url)

                    logger.info(f"📄 Tìm thấy PDF mới: {full_pdf_url}")

                yield scrapy.Request(
                    url=full_pdf_url,
                    callback=self.save_pdf,
                    cookies=self.cookies,
                    meta={'item_id': item_id}
                )
            else:
                logger.warning(
                    f"⚠️ Không tìm thấy link PDF trên trang của ItemID: {item_id}")
                # Vẫn đánh dấu hoàn thành hoặc xử lý log lỗi tùy bạn

        except Exception as e:
            logger.error(f"❌ Lỗi parse trang chi tiết {item_id}: {e}")

    def save_pdf(self, response):
        item_id = response.meta['item_id']
        # Đặt tên file là ID của document
        file_name = f"{item_id}.pdf"
        file_path = os.path.join(PATH_FOLDER_DATA, file_name)

        try:
            # Kiểm tra nội dung có phải PDF không
            if b'%PDF' not in response.body[:10]:
                logger.warning(
                    f"⚠️ File tải về cho {item_id} không đúng định dạng PDF.")
                return

            with open(file_path, 'wb') as f:
                f.write(response.body)

        except Exception as e:
            logger.error(f"❌ Lỗi khi lưu file PDF {item_id}: {e}")
