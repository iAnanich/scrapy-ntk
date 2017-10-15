import logging
from datetime import datetime

import gspread
import scrapy
from oauth2client.service_account import ServiceAccountCredentials as Creds

from . import config
from .item import (
    ArticleItem,
    URL, INDEX, TEXT, TAGS, DATE, HEADER,
)

cfg = config.cfg
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


COLUMNS_TUPLE = (URL, HEADER, TAGS, TEXT, DATE, INDEX)


class GSpreadMaster:

    _secret_file_name = 'client-secret.json'  # library_depend

    sheet_name = cfg.spreadsheet_title
    spider_to_worksheet_dict = cfg.spider_to_worksheet_dict

    def __init__(self):
        self._path_to_secret = cfg.path_to_config_file(self._secret_file_name)
        self._credentials = self._get_credentials()
        self._client = self._get_client()
        self.spreadsheet = self._client.open(self.sheet_name)

    def _get_credentials(self) -> Creds:
        return Creds.from_json_keyfile_name(
            self._path_to_secret, ['https://spreadsheets.google.com/feeds'])

    def _get_client(self) -> gspread.Client:
        return gspread.authorize(self._credentials)

    def get_worksheet_by_spider(self, spider: scrapy.spiders.Spider) -> gspread.Worksheet:
        try:
            index = self.spider_to_worksheet_dict[spider.name]
        except KeyError:
            raise RuntimeError('No worksheet configured for this spider: {}'.format(spider.name))
        try:
            worksheet = self.spreadsheet.get_worksheet(index)
            assert worksheet is not None
        except AssertionError:
            raise RuntimeError('No worksheet exist for this spider: {}/{}'.format(spider.name, index))
        return worksheet

    @property
    def secret_file_name(self):
        return self._secret_file_name


class GSpreadRow:
    """ Place to configure fields order in a table"""

    columns_order = COLUMNS_TUPLE
    empty_cell = '- - -'

    def __init__(self, item: ArticleItem or dict = None,
                 url: str = empty_cell,
                 header: str = empty_cell,
                 tags: str = empty_cell,
                 text: str = empty_cell,
                 date: str = empty_cell,
                 index: str = empty_cell):
        if item is not None:
            self.item_dict = dict(item)
        else:
            self.item_dict = dict(url=url, header=header, tags=tags,
                                  text=text, date=date, index=index, )
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
    def to_tuple(cls, **kwargs):
        return tuple(cls(**kwargs))
