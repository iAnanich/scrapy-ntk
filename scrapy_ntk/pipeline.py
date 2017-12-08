import logging

from .base import BaseArticlePipeline
from .config import cfg
from .exporting import (
    GSpreadMaster,
    GSpreadAIE,
    SQLAlchemyAIE,
    SQLAlchemyMaster,
    SQLAlchemyWriter,
    GSpreadWriter,
    GSpreadRow,
    BackupGSpreadRow,
)
from .spider import BaseArticleSpider


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


class GSpreadPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if ENABLE_GSPREAD:
            self.master = GSpreadMaster(cfg.spreadsheet_title)
            self.exporter = GSpreadAIE(
                enable_postpone_mode=True,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=GSpreadRow,
                    name="main"
                )
            )


class BackupGSpreadPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if ENABLE_GSPREAD:
            self.master = GSpreadMaster(cfg.backup_spreadsheet_title)
            self.exporter = GSpreadAIE(
                enable_postpone_mode=False,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=BackupGSpreadRow,
                    name="backup"
                )
            )


class SQLAlchemyPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if ENABLE_DATABASE:
            self.master = SQLAlchemyMaster(cfg.database_url, cfg.database_table_name)
            self.exporter = SQLAlchemyAIE(
                enable_postpone_mode=True,
                writer=SQLAlchemyWriter(
                    session=self.master.session,
                    Model=self.master.Model,
                )
            )
