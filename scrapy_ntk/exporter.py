import logging
from datetime import datetime

from scrapy.exporters import BaseItemExporter

from .config import cfg
from .item import ArticleItem
from .storage import GSpreadRow

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

    def __init__(self, **kwargs):
        self._spider = kwargs.pop('spider')
        self._worksheet = kwargs.pop('worksheet')
        self._job_url = None
        # define
        self._items = []
        self._is_active = None
        # log info
        self._log_config()

        super().__init__(**kwargs)

    def _write_rows(self, rows: list or tuple) -> None:
        for row in rows:
            self._worksheet.append_row(row)

    @property
    def _starting_row(self):
        return GSpreadRow.to_tuple(
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
        return GSpreadRow.to_tuple(
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

    def convert_to_rows(self, items: list) -> list:
        res = []
        if _to_bool(cfg.gspread_enable_prefix):
            res.append(self._starting_row)
        for item in items:
            res.append(GSpreadRow.to_tuple(item=item))
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
        rows = self.convert_to_rows(self._items)
        self._log_rows(rows)
        self._write_rows(rows)
        self._is_active = False

    def export_item(self, item):
        if self._is_active is not True:
            raise RuntimeError(
                'Can not append item when session have "{}" status'
                .format(self.status))
        if not isinstance(item, ArticleItem):
            raise TypeError('Can not export item that is not {}'
                            .format(ArticleItem.__name__))
        self._items.append(item)

    # logging methods
    def _log_config(self):
        logger.info(
            'GSpread exporting configuration ::\n'
            '\tspider = "{name}" (id= {id})\n'
            '\tspreadsheet = "{ss}"\n'
            '\tworksheet = "{ws}"'
            .format(
                name=self._spider.name,
                id=cfg.current_spider_id,
                ss=self._worksheet.spreadsheet.title,
                ws=self._worksheet.title,
            )
        )

    def _log_rows(self, rows: list or tuple):
        string = 'Rows to write:['
        for row in rows:
            string += '\n\t("' + '", "'.join(row) + '")'
        logger.debug(string + '\n]')

    def __repr__(self):
        return '<{name} "{status}" items: {i}>'.format(
            name=self.__name__,
            status=self.status,
            i=len(self._items),
        )
