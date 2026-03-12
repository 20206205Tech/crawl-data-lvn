from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from crawler.spiders.document_list_spider import DocumentListSpider


def main():
    settings = get_project_settings()
    settings.set('FEEDS', {
        '../data/document_list.jsonl': {'format': 'jsonl', 'overwrite': True}
    })

    process = CrawlerProcess(settings)
    process.crawl(DocumentListSpider)
    process.start()


if __name__ == "__main__":
    main()