import logging

from scrapy.exceptions import NotConfigured
from scrapy import signals

from .spider import NewsArticleSpider, TestingSpider, WorkerSpider
from .config import cfg
from .tools.cloud import SHub, SHubFetcher
from .item import FINGERPRINT
from .utils.check import has_any_type

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _to_boolean(option: str) -> bool:
    if option in ['True', '1']:
        return True
    elif option in ['False', '0']:
        return False
    else:
        raise RuntimeError('Cannot recognise argument value: {}'
                           .format(option))


class SHubConnector:

    def __init__(self, enable):
        self.enabled = enable
        logger.info(
            f'ScrapingHub connector is {"enabled" if enable else "disabled"}.')

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(_to_boolean(cfg.enable_shub))
        crawler.signals.connect(ext.spider_opened,
                                signal=signals.spider_opened)
        return ext

    def spider_opened(self, spider):
        if has_any_type(spider, TestingSpider, WorkerSpider):
            pass
        elif self.enabled and isinstance(spider, NewsArticleSpider):
            shub = SHub(
                default_conf={
                    'api_key': str(cfg.api_key),
                    'project_id': int(cfg.current_project_id),
                    'spider_id': int(cfg.current_spider_id),
                },
            )
            spider.connect_cloud(shub)
