import abc

from scrapy.exporters import BaseItemExporter

from .item import ArticleItem
from .config import cfg
from .tools.cloud import SHubInterface
from .spider import SingleSpider, TestingSpider, BaseSpider, WorkerSpider
from .exporting import GSpreadMaster, GSpreadItemExporter
from .exporting.g_spread import GSpreadWriter, GSpreadRow, BackupGSpreadRow


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


class ArticlePipeline(abc.ABC):

    def __init__(self):
        self.exporter: BaseItemExporter = None
        self.master = None

    @abc.abstractmethod
    def open_spider(self, spider: BaseSpider):
        pass

    def close_spider(self, spider):
        if self.exporter is not None:
            self.exporter.finish_exporting()

    def process_item(self, item: ArticleItem, spider) -> ArticleItem:
        if isinstance(item, ArticleItem):
            if self.exporter is not None:
                self.exporter.export_item(item)
        else:
            pass
        return item


class GSpreadPipeline(ArticlePipeline):

    def open_spider(self, spider: BaseSpider):
        # FIXME: move to standalone exporter or extension
        if is_any_instance(spider, TestingSpider, WorkerSpider):
            pass
        elif ENABLE_SHUB and isinstance(spider, SingleSpider):
            spider.connect_cloud(SHubInterface())

        if ENABLE_GSPREAD:
            self.master = GSpreadMaster(cfg.spreadsheet_title)
            self.exporter = GSpreadItemExporter(
                enable_postpone_mode=True,
                spider=spider,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=GSpreadRow,
                    name="main"
                )
            )
            self.exporter.start_exporting()


class BackupGSpreadPipeline(ArticlePipeline):

    def open_spider(self, spider: BaseSpider):
        if ENABLE_GSPREAD:
            self.master = GSpreadMaster(cfg.backup_spreadsheet_title)
            self.exporter = GSpreadItemExporter(
                enable_postpone_mode=False,
                spider=spider,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=BackupGSpreadRow,
                    name="backup"
                )
            )
            self.exporter.start_exporting()
