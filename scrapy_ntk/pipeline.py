import logging

from .item import ArticleItem
from .config import cfg
from .cloud import SHubInterface
from .spider import SingleSpider, TestingSpider, BaseSpider, WorkerSpider
from .storage import GSpreadMaster
from .exporter import GSpreadItemExporter


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


ENABLE_GSPREAD = _to_boolean(cfg.enable_gspread)
ENABLE_SHUB = _to_boolean(cfg.enable_shub)


class StoragePipeline(object):

    def __init__(self):
        self.gspread_master = None
        self.gspread_exporter = None
        self.cloud_interface = None

    def open_spider(self, spider: BaseSpider):

        if is_any_instance(spider, TestingSpider, WorkerSpider):
            pass
        elif ENABLE_SHUB and isinstance(spider, SingleSpider):
            self.cloud_interface = SHubInterface()
            spider.connect_cloud(self.cloud_interface)

        if ENABLE_GSPREAD and not isinstance(spider, WorkerSpider):
            self.gspread_master = GSpreadMaster()
            self.gspread_exporter = GSpreadItemExporter(
                worksheet=self.gspread_master.get_worksheet_by_spider(spider),
                backup_worksheet=self.gspread_master.get_backup_worksheet_by_spider(spider),
                spider=spider,
            )
            self.gspread_exporter.start_exporting()

    def close_spider(self, spider: BaseSpider):
        if self.gspread_exporter is not None:
            self.gspread_exporter.finish_exporting()

    def process_item(self, item: ArticleItem, spider: BaseSpider):
        if isinstance(item, dict):
            return item
        elif isinstance(item, ArticleItem):
            if ENABLE_GSPREAD:
                self.gspread_exporter.export_item(item)
            return item
        else:
            logging.warning(
                'Unknown item type: {}. Item will not be saved into exporters.'
                .format(repr(item)))
            return item
