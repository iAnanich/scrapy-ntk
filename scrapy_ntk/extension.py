import logging
import warnings

from scrapy import signals

from .config import cfg
from .scraping_hub.manager import ScrapinghubManager, ManagerDefaults
from .spider import NewsArticleSpider, TestingSpider, WorkerSpider
from .utils.args import to_bool, to_str, to_int
from .utils.check import has_any_type

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ScrapinghubManagerConnector:

    def __init__(self, enable):
        self.enabled = enable
        logger.info(
            f'ScrapingHub connector is {"enabled" if enable else "disabled"}.')

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(to_bool(cfg.enable_shub))
        crawler.signals.connect(ext.spider_opened,
                                signal=signals.spider_opened)
        return ext

    def spider_opened(self, spider):
        if has_any_type(spider, TestingSpider, WorkerSpider):
            pass
        elif self.enabled and isinstance(spider, NewsArticleSpider):
            defaults = ManagerDefaults(
                api_key=to_str(cfg.api_key, 32),
                project_id=to_int(cfg.current_project_id),
                spider_id=to_int(cfg.current_spider_id),
            )
            shub = ScrapinghubManager(defaults=defaults)
            spider.connect_cloud(shub)


class SHubConnector(ScrapinghubManagerConnector):

    def __init__(self, *args, **kwargs):
        warnings.warn(f'Use "ScrapinghubManagerConnector" instead.')

        super().__init__(*args, **kwargs)
