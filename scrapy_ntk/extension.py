import logging

from scrapy.exceptions import NotConfigured
from scrapy import signals

from .spider import SingleSpider, TestingSpider, WorkerSpider
from .config import cfg
from .tools.cloud import SHubInterface

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


def is_any_instance(obj, *types):
    return any(isinstance(obj, type_) for type_ in types)


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
        if is_any_instance(spider, TestingSpider, WorkerSpider):
            pass
        elif self.enabled and isinstance(spider, SingleSpider):
            spider.connect_cloud(SHubInterface())
