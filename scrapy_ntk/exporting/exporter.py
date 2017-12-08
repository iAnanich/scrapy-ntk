import logging
from datetime import datetime

from .base import BaseArticleItemExporter
from .g_spread import GSpreadWriter
from .sql_alchemy import SQLAlchemyWriter
from ..config import cfg


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _to_bool(string: str) -> bool:
    if string in ['True', '1']:
        return True
    elif string in ['False', '0']:
        return False
    else:
        raise ValueError('Unknown string value: ' + string)


class GSpreadAIE(BaseArticleItemExporter):

    empty_cell = '-----'

    _writer_type = GSpreadWriter

    @property
    def job_url(self):
        return f'https://app.scrapinghub.com/p' \
               f'/{cfg.current_project_id}' \
               f'/{cfg.current_spider_id}' \
               f'/{cfg.current_job_id}'

    @property
    def _start_row(self):
        return dict(
            url=self.empty_cell,
            header=cfg.gspread_prefixfmt.format(
                date=datetime.now(),
                name=self._spider.name,
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
            header=cfg.gspread_suffixfmt.format(
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
        if _to_bool(cfg.gspread_enable_prefix):
            res.append(self._start_row)
        res += items
        if _to_bool(cfg.gspread_enable_suffix):
            res.append(self._close_row)
        return res

    def _finish_postpone(self):
        items = self._incapsulate_items(self._items)
        self._writer.write(*items)


class SQLAlchemyAIE(BaseArticleItemExporter):

    _writer_type = SQLAlchemyWriter
