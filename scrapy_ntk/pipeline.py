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
from .utils.args import to_bool, to_str


class GSpreadPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if to_bool(cfg.enable_gspread):
            self.master = GSpreadMaster(to_str(cfg.spreadsheet_title))
            self.exporter = GSpreadAIE(
                spider=spider,
                enable_postpone_mode=True,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=GSpreadRow,
                    name="main"
                )
            )


class BackupGSpreadPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if to_bool(cfg.enable_gspread):
            self.master = GSpreadMaster(to_str(cfg.backup_spreadsheet_title))
            self.exporter = GSpreadAIE(
                spider=spider,
                enable_postpone_mode=False,
                writer=GSpreadWriter(
                    worksheet=self.master.get_worksheet_by_spider(spider),
                    row=BackupGSpreadRow,
                    name="backup"
                )
            )


class SQLAlchemyPipeline(BaseArticlePipeline):

    def setup_exporter(self, spider: BaseArticleSpider):
        if to_bool(cfg.enable_database):
            self.master = SQLAlchemyMaster(
                database_url=to_str(cfg.database_url),
                table_name=to_str(cfg.database_table_name), )
            self.exporter = SQLAlchemyAIE(
                enable_postpone_mode=True,
                writer=SQLAlchemyWriter(
                    session=self.master.session,
                    declarative_model_class=self.master.Model,
                )
            )
