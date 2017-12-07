import abc
import logging

from scrapy.exporters import BaseItemExporter

from .item import ArticleItem
from .config import cfg
from .tools.cloud import SHubInterface
from .spider import SingleSpider, TestingSpider, BaseSpider, WorkerSpider
from .exporting import (
    GSpreadMaster,
    GSpreadItemExporter,
    SQLAlchemyItemExporter,
    SQLAlchemyMaster,
    SQLAlchemyWriter,
    GSpreadWriter,
    GSpreadRow,
    BackupGSpreadRow,
)

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


ENABLE_GSPREAD = _to_boolean(cfg.enable_gspread)
ENABLE_SHUB = _to_boolean(cfg.enable_shub)
ENABLE_DATABASE = _to_boolean(cfg.enable_database)


class ArticlePipeline(abc.ABC):

    def __init__(self):
        self._exporter: BaseItemExporter = None
        self._master = None

        self._state = None

    @abc.abstractmethod
    def setup_exporter(self, spider: BaseSpider):
        pass

    def __repr__(self):
        return f'<{self.name} :: {self.exporter} state: {self._state}>'

    @property
    def name(self):
        return f'{self.__class__.__name__}'

    @property
    def is_active(self) -> bool:
        return bool(self._state)

    @is_active.setter
    def is_active(self, val: bool):
        state = bool(val)
        self._state = state
        logger.info(f'{self.name} | state update: {self._state}')

    @property
    def exporter(self) -> BaseItemExporter:
        return self._exporter

    @exporter.setter
    def exporter(self, new: BaseItemExporter):
        if not isinstance(new, BaseItemExporter):
            exporter_type_msg = \
                f'{self.name} | You are trying to set "exporter" ' \
                f'with wrong type: {type(new)}'
            logger.error(exporter_type_msg)
            raise TypeError(exporter_type_msg)
        else:
            logger.debug(f'{self.name} | {new} exporter settled up.')
            self._exporter = new

    @property
    def master(self):
        return self._master

    @master.setter
    def master(self, new):
        self._master = new

    def open_spider(self, spider: BaseSpider):
        try:
            self.setup_exporter(spider)
        except Exception as exc:
            logger.exception(f'{self.name} | Error while setting up {self.name}: {exc}')
            self.is_active = False
        else:
            if self.exporter is None:
                logger.error(f'{self.name} | "exporter" is not set up.')
                self.is_active = False
            else:
                self.is_active = True
            if self.master is None:
                logger.warning(f'{self.name} | "master" attribute is not set.')

        if self._state:
            try:
                self.exporter.start_exporting()
            except Exception as exc:
                logger.exception(f'{self.name} | Error while starting exporting with {self.exporter}: {exc}')
                self.is_active = False

    def close_spider(self, spider):
        if self._state:
            self.exporter.finish_exporting()
            logger.info(f'{self.name} | Successfully finished {self.exporter} exporter.')

    def process_item(self, item: ArticleItem, spider) -> ArticleItem:
        if isinstance(item, ArticleItem):
            if self._state:
                try:
                    self.exporter.export_item(item)
                except Exception as exc:
                    logger.exception(
                        f'{self.name} | Error while exporting '
                        f'<{item["fingerprint"]}> item: {exc}')
        else:
            pass
        return item


class GSpreadPipeline(ArticlePipeline):

    def setup_exporter(self, spider: BaseSpider):
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


class BackupGSpreadPipeline(ArticlePipeline):

    def setup_exporter(self, spider: BaseSpider):
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


class SQLAlchemyPipeline(ArticlePipeline):

    def setup_exporter(self, spider: BaseSpider):
        if ENABLE_DATABASE:
            self.master = SQLAlchemyMaster(cfg.database_url, cfg.database_table_name)
            self.exporter = SQLAlchemyItemExporter(
                enable_postpone_mode=True,
                writer=SQLAlchemyWriter(
                    session=self.master.session,
                    Model=self.master.Model,
                )
            )
