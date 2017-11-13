import logging
import abc
from datetime import datetime
from typing import List, Tuple

import gspread
import scrapy
from oauth2client.service_account import \
    ServiceAccountCredentials as Credentials

from . import config
from .item import ArticleItem, DATE, URL

cfg = config.cfg
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GSpreadMaster:

    def __init__(self):
        self._credentials = self._get_credentials()
        self._client = self._get_client()
        self.spreadsheet = self._client.open(cfg.spreadsheet_title)
        self.backup_spreadsheet = None

        backup_spreadsheet_title = cfg.backup_spreadsheet_title
        if backup_spreadsheet_title is not None:
            self.backup_spreadsheet = self._client.open(backup_spreadsheet_title)

    @staticmethod
    def _get_credentials() -> Credentials:
        return Credentials.from_json_keyfile_name(
            cfg.client_secret_path, ['https://spreadsheets.google.com/feeds'])

    @staticmethod
    def _get_index(spider):
        try:
            return cfg.get_worksheet_id(spider.name)
        except KeyError:
            raise RuntimeError(
                f'No worksheet configured for this spider: {spider.name}')

    @classmethod
    def _get_worksheet(cls, spider, spreadsheet: gspread.Spreadsheet =None):
        if spreadsheet is None:
            return None

        index = cls._get_index(spider)
        try:
            worksheet = spreadsheet.get_worksheet(index)
            assert worksheet is not None
        except AssertionError:
            raise RuntimeError(
                f'No worksheet exist for this spider: {spider.name}/{index}')
        return worksheet

    def _get_client(self) -> gspread.Client:
        return gspread.authorize(self._credentials)

    def get_worksheet_by_spider(self, spider: scrapy.spiders.Spider) \
            -> gspread.Worksheet:
        return self._get_worksheet(spider, self.spreadsheet)

    def get_backup_worksheet_by_spider(self, spider: scrapy.spiders.Spider) \
            -> gspread.Worksheet:
        return self._get_worksheet(spider, self.backup_spreadsheet)

    @property
    def secret_file_name(self):
        return 'client-secret.json'  # library_depend


class Row(abc.ABC):
    """ Place to configure fields order in a table"""

    empty_cell = '- - -'

    def __init__(self, item: ArticleItem or dict = None, **fields):
        if item is not None:
            self.item_dict = dict(item)
        else:
            self.item_dict = fields

        self.serialized = self.serialize(self.item_dict)

    def serialize(self, item_dict: dict) -> dict:
        for key, value in item_dict.items():
            if key == DATE and isinstance(value, datetime):
                new_value = value.strftime(cfg.item_datefmt)
            elif not isinstance(value, str):
                logger.warning(
                    'For "{}" key value ("{}") is not `str` instance'
                    .format(key, value))
                new_value = str(value)
            else:
                new_value = value
            item_dict[key] = new_value
        return item_dict

    def __iter__(self):
        for column in self.columns_order:
            yield self.serialized[column]

    @classmethod
    def to_tuple(cls, **kwargs) -> tuple:
        return tuple(cls(**kwargs))

    @property
    @abc.abstractmethod
    def columns_order(self) -> tuple:
        pass


class GSpreadRow(Row):

    @property
    def columns_order(self):
        return cfg.columns


class BackupGSpreadRow(Row):

    @property
    def columns_order(self):
        return [DATE, URL]


class BaseGSpreadWriter(abc.ABC):

    def __init__(self, worksheet: gspread.Worksheet=None):
        self._worksheet = worksheet

    @property
    @abc.abstractmethod
    def Row(self) -> Row:
        pass

    @property
    def disabled(self):
        return self._worksheet is None

    def write(self, *items: List[ArticleItem]):
        if self.disabled:
            return

        rows = self._convert_items(*items)
        if len(items) == 0:
            return
        elif len(rows) == 1:
            row = rows[0]
            logger.debug(f'{self.writer_name} :: Writing into '
                         f'"{self._worksheet.spreadsheet.title}/'
                         f'{self._worksheet.title}":\n\t{row}')

            self._write_row(row)
            logger.info(f'{self.writer_name} :: Successfully writen row '
                        f'into {self.worksheet_name}')
        else:
            msg = f'Writing {len(rows)} rows into ' \
                  f'"{self._worksheet.spreadsheet.title}/' \
                  f'{self._worksheet.title}":'
            for i, row in enumerate(rows):
                msg += f'\n {i:s3}. {row}'
            logger.debug(msg)

            for row in rows:
                self._write_row(row)
            logger.info(f'{self.writer_name} :: Successfully writen '
                        f'{len(rows)} rows into {self.worksheet_name}')

    def _write_row(self, row: tuple):
        self._worksheet.append_row(row)

    def _convert_items(self, *items) -> Tuple[tuple, ...]:
        return tuple(self.Row.to_tuple(item=item) for item in items)

    @property
    def worksheet_name(self):
        return f'"{self._worksheet.spreadsheet.title}"/"{self._worksheet.title}"'

    @property
    def writer_name(self):
        return self.__class__.__name__

    def __repr__(self):
        return f'<{self.writer_name} :: disabled: {self.disabled}, Row: {self.Row}>'


class GSpreadWriter(BaseGSpreadWriter):

    @property
    def Row(self):
        return GSpreadRow


class BackupGSpreadWriter(BaseGSpreadWriter):

    @property
    def Row(self):
        return BackupGSpreadRow
