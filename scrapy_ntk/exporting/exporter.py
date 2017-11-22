import logging
from datetime import datetime

from scrapy.exporters import BaseItemExporter

from ..config import cfg
from ..item import ArticleItem
from .g_spread import GSpreadWriter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _to_bool(string: str) -> bool:
    if string in ['True', '1']:
        return True
    elif string in ['False', '0']:
        return False
    else:
        raise ValueError('Unknown string value: ' + string)


class GSpreadItemExporter(BaseItemExporter):

    empty_cell = '-----'

    @property
    def job_url(self):
        if self._job_url is None:
            self._job_url = f'https://app.scrapinghub.com/p' \
                            f'/{cfg.current_project_id}' \
                            f'/{cfg.current_spider_id}' \
                            f'/{cfg.current_job_id}'
        return str(self._job_url)

    def __init__(self, *, spider, writer: GSpreadWriter,
                 enable_postpone_mode: bool =True, **kwargs):
        self._spider = spider
        self._writer = writer
        self._postpone_mode_enabled = enable_postpone_mode

        # define
        self._items = []
        self._is_active = None
        self._job_url = None

        # log info
        logger.info(f'Writer initialised = {self._writer}')

        super().__init__(**kwargs)

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

    @property
    def items(self):
        return self._items

    @property
    def status(self):
        if self._is_active is None:
            return 'idle'
        elif self._is_active is True:
            return 'active'
        elif self._is_active is False:
            return 'closed'
        else:
            raise TypeError('Can not recognise status.')

    # exporter methods
    def start_exporting(self):
        self._is_active = True

    def finish_exporting(self):
        if self._postpone_mode_enabled:
            items = self._incapsulate_items(self._items)
            self._writer.write(*items)
        self._is_active = False

    def export_item(self, item):
        if self._is_active is not True:
            raise RuntimeError(
                'Can not append item when session have "{}" status'
                .format(self.status))
        if not isinstance(item, ArticleItem):
            raise TypeError('Can not export item that is not {}'
                            .format(ArticleItem.__name__))
        self._export(item)

    def _export(self, item):
        if self._postpone_mode_enabled:
            self._items.append(item)
        else:
            self._writer.write(item)

    def __repr__(self):
        return f'<{self.__class__.__name__} :: ' \
               f'status: "{self.status}", items: {len(self._items)}>'
