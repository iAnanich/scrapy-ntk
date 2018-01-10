import logging
from datetime import datetime

from scrapy import Spider

from ..base import BaseArticleItemExporter, BaseArticleItemWriter
from .g_spread import GSpreadWriter
from .sql_alchemy import SQLAlchemyWriter
from ..config import cfg
from ..utils.args import to_bool, to_str


class GSpreadAIE(BaseArticleItemExporter):

    empty_cell = '-----'
    default_spider_name = '-- NOT PROVIDED --'

    _writer_type = GSpreadWriter

    def __init__(self, *, writer: BaseArticleItemWriter, spider: Spider,
                 enable_postpone_mode: bool =True,
                 logger: logging.Logger =None, **kwargs):
        super().__init__(
            writer=writer,
            enable_postpone_mode=enable_postpone_mode,
            logger=logger,
            **kwargs)
        if spider is None:
            self.logger.debug('`spider` key-word argument was not provided.')
        self.spider = spider

    @property
    def job_url(self):
        return f'https://app.scrapinghub.com/p' \
               f'/{cfg.current_project_id}' \
               f'/{cfg.current_spider_id}' \
               f'/{cfg.current_job_id}'

    @property
    def _start_row(self):
        if self.spider:
            spider_name = self.spider.name
        else:
            self.logger.warning(
                f'`spider` key-word argument was not provided. '
                f'Using `{self.default_spider_name}` string instead it\' name.')
            spider_name = self.default_spider_name

        return dict(
            url=self.empty_cell,
            header=to_str(cfg.gspread_prefixfmt).format(
                date=datetime.now(),
                name=spider_name,
            ),
            tags=self.job_url,
            text=self.empty_cell,
            date=self.empty_cell,
            index=self.empty_cell,
        )

    @property
    def _close_row(self):
        return dict(
            url=self.empty_cell,
            header=to_str(cfg.gspread_suffixfmt).format(
                date=datetime.now(),
                count=str(len(self._items)),
            ),
            tags=self.job_url,
            text=self.empty_cell,
            date=self.empty_cell,
            index=self.empty_cell,
        )

    def _incapsulate_items(self, items: list) -> list:
        res = []
        if to_bool(cfg.gspread_enable_prefix):
            res.append(self._start_row)
        res += items
        if to_bool(cfg.gspread_enable_suffix):
            res.append(self._close_row)
        return res

    def _finish_postpone(self):
        items = self._incapsulate_items(self._items)
        self._writer.write(*items)


class SQLAlchemyAIE(BaseArticleItemExporter):

    _writer_type = SQLAlchemyWriter
