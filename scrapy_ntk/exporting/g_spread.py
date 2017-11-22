import logging
import abc
from datetime import datetime
from typing import List, Tuple

import gspread
import scrapy
from oauth2client.service_account import \
    ServiceAccountCredentials as Credentials

from ..config import cfg
from ..item import ArticleItem, DATE, URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GSpreadMaster:

    def __init__(self, spreadsheet_title: str):
        self._credentials = self._get_credentials()
        self._client = self._get_client()
        self.spreadsheet = self._client.open(spreadsheet_title)

    @staticmethod
    def _get_credentials() -> Credentials:
        return Credentials.from_json_keyfile_name(
            cfg.client_secret_path, ['https://spreadsheets.google.com/feeds'])

    def _get_client(self) -> gspread.Client:
        return gspread.authorize(self._credentials)

    def get_worksheet_by_spider(self, spider: scrapy.spiders.Spider) \
            -> gspread.Worksheet:
        try:
            index = cfg.get_worksheet_id(spider.name)
            worksheet = self.spreadsheet.get_worksheet(index)
            assert worksheet is not None
        except KeyError:
            raise RuntimeError(
                f'No worksheet configured for this spider: {spider.name}')
        except AssertionError:
            raise RuntimeError(
                f'No worksheet exist for this spider: {spider.name}/{index}')
        return worksheet

    @property
    def secret_file_name(self):
        return 'client-secret.json'  # library_depend


class BaseGSpreadRow(abc.ABC):
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
            elif value is None:
                new_value = ''
            else:
                new_value = str(value)
            item_dict[key] = new_value
        return item_dict

    def __iter__(self):
        for column in self.columns_order:
            yield self.serialized[column]

    def __repr__(self):
        return f'<{self.__class__.__name__} columns: {self.columns_order}>'

    def __str__(self):
        return str(self.serialized)

    @classmethod
    def to_tuple(cls, **kwargs) -> tuple:
        return tuple(cls(**kwargs))

    @property
    @abc.abstractmethod
    def columns_order(self) -> tuple:
        pass


class GSpreadRow(BaseGSpreadRow):

    @property
    def columns_order(self):
        return cfg.columns


class BackupGSpreadRow(BaseGSpreadRow):

    @property
    def columns_order(self):
        return [DATE, URL]


class GSpreadWriter(abc.ABC):

    _names = set()

    def __init__(self, worksheet: gspread.Worksheet, row: type, name: str =None):
        self._worksheet = worksheet
        self._row = row

        if name is not None and name not in self._names:
            self._name = name
        else:
            self._name = f'{self._writer_name}_{len(self._names)}'
        self._names.add(self._name)

    def write(self, *items: List[ArticleItem]):
        rows = self._convert_items(*items)
        prefix = f'{self.name} ::'
        if len(items) == 0:
            return
        elif len(rows) == 1:
            row = rows[0]
            logger.debug(f'{prefix} Writing into '
                         f'"{self._worksheet.spreadsheet.title}/'
                         f'{self._worksheet.title}":\n\t{row}')

            self._write_row(row)
            logger.info(f'{prefix} Successfully writen row '
                        f'into {self.worksheet_name}')
        else:
            msg = f'{prefix} Writing {len(rows)} rows into ' \
                  f'"{self._worksheet.spreadsheet.title}/' \
                  f'{self._worksheet.title}":'
            for i, row in enumerate(rows):
                msg += f'\n{i:4}. {row}'
            logger.debug(msg)

            for row in rows:
                self._write_row(row)
            logger.info(f'{prefix} Successfully writen '
                        f'{len(rows)} rows into {self.worksheet_name}')

    def _write_row(self, row: tuple):
        self._worksheet.append_row(row)

    def _convert_items(self, *items) -> Tuple[tuple, ...]:
        return tuple(self.Row.to_tuple(item=item) for item in items)

    @property
    def Row(self) -> BaseGSpreadRow:
        return self._row

    @property
    def worksheet(self):
        return self._worksheet

    @property
    def worksheet_name(self):
        return f'"{self._worksheet.spreadsheet.title}"/"{self._worksheet.title}"'

    @property
    def name(self):
        return self._name

    @property
    def _writer_name(self):
        return self.__class__.__name__

    def __repr__(self):
        return f'<{self._writer_name} :: Row: "{self.Row}"; name: {self.name}; destination: {self.worksheet_name}>'
