import scrapy
from scrapy.http import JsonRequest
import math
import scrapy
from scrapy.utils.response import open_in_browser
from urllib.parse import parse_qs, urlparse, urlencode
from crawler.env import CRAWL_DATA_ENV_DEV


class DocumentListSpider(scrapy.Spider):
    name = "document_list"
    api_url = "https://apidemo.luatvietnam.vn/api/docs/Search?time={YOUR_TIME}&sig={YOUR_SIG}"
    psize = 20 if CRAWL_DATA_ENV_DEV else 200

    def start_requests(self):
        payload = {"page": 1, "PSize": self.psize}
        yield JsonRequest(
            url=self.api_url,
            data=payload,
            callback=self.parse,
            meta={'payload': payload}
        )

    # def parse_first_page(self, response):
    #     data = response.json()
    #     row_count = data.get("RowCount", 0)
    #     self.logger.info(f"🚀 TỔNG SỐ BẢN GHI: {row_count}")

    #     # Tính tổng số trang cần crawl
    #     total_pages = math.ceil(row_count / self.psize)
    #     self.logger.info(f"📂 Tổng số trang cần xử lý: {total_pages}")

    #     # Xử lý dữ liệu trang 1 trước
    #     yield from self.parse(response)

    #     # Sau đó tạo request cho tất cả các trang còn lại (từ trang 2)
    #     for page in range(2, total_pages + 1):
    #         new_payload = {
    #             "page": page,
    #             "PSize": self.psize
    #         }
    #         yield JsonRequest(
    #             url=self.api_url,
    #             data=new_payload,
    #             callback=self.parse,
    #             meta={'payload': new_payload}
    #         )

    def parse(self, response):

        # if CRAWL_DATA_ENV_DEV:
        #     open_in_browser(response)

        data = response.json()
        docs = data.get("docsModels", [])

        payload = response.meta.get('payload')

        current_page = payload.get("page", 1)

        self.logger.info(f"Tìm thấy {len(docs)} văn bản trong trang này.")

        # 1. Kiểm tra nếu không có dữ liệu thì dừng
        if not docs:
            self.logger.info(
                f"Không còn dữ liệu. Kết thúc tại trang {current_page - 1}.")
            return

        self.logger.info(
            f"--- Đang xử lý trang {current_page} (Tìm thấy {len(docs)} văn bản) ---")

        for doc in docs:
            yield {
                "LanguageId": doc.get("LanguageId"),
                "DocId": doc.get("DocId"),
                "DocName": doc.get("DocName"),
                "DocSummary": doc.get("DocSummary"),
                "DocIdentity": doc.get("DocIdentity"),
                "IssueDate": doc.get("IssueDate"),
                "EffectDate": doc.get("EffectDate"),
                "ExpireDate": doc.get("ExpireDate"),
                "GazetteNumber": doc.get("GazetteNumber"),
                "GazetteDate": doc.get("GazetteDate"),
                "EffectStatusId": doc.get("EffectStatusId"),
                "DocUrl": doc.get("DocUrl"),
                "DocGroupId": doc.get("DocGroupId"),
                "DocTypeId": doc.get("DocTypeId"),
                "IssueYear": doc.get("IssueYear"),
                "EffectStatusName": doc.get("EffectStatusName"),
                "DocTypeName": doc.get("DocTypeName"),
                "OrganName": doc.get("OrganName"),
                "SignerName": doc.get("SignerName"),
                "FieldName": doc.get("FieldName"),
                "CrDateTime": doc.get("CrDateTime"),
                "UpdDateTime": doc.get("UpdDateTime"),
            }

        if not CRAWL_DATA_ENV_DEV:

            row_count = data.get("RowCount", 0)
            total_pages_possible = math.ceil(row_count / self.psize)

            next_page = current_page + 1

            if next_page <= total_pages_possible:
                new_payload = {"page": next_page, "PSize": self.psize}
                yield JsonRequest(
                    url=self.api_url,
                    data=new_payload,
                    callback=self.parse,
                    meta={'payload': new_payload}
                )
