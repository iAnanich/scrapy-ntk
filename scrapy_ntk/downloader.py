from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy import signals

from .tools.proxy import ProxyManager
from .spider import BaseArticleSpider, WorkerSpider


class ProxyManagerDM:

    def __init__(self, enable: bool=False, mode: str=None):
        self.enable = enable
        if self.enable:
            self.proxy_manager = ProxyManager(mode)

    @classmethod
    def from_crawler(cls, crawler):
        new = cls()
        crawler.signals.connect(
            new.spider_opened, signal=signals.spider_opened)
        return new

    def spider_opened(self, spider: BaseArticleSpider):
        if not isinstance(spider, WorkerSpider):
            self.enable = spider.enable_proxy
            if self.enable:
                self.proxy_manager = ProxyManager(spider.proxy_mode)
        else:
            self.enable = False

    def process_request(self, request: Request, spider: BaseArticleSpider) \
            -> None or Request or Response:
        if self.enable:
            self.proxy_manager.process(request)
