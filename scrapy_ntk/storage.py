import logging
from datetime import datetime

import gspread
import scrapy
from oauth2client.service_account import \
    ServiceAccountCredentials as Credentials

from . import config
from .item import (
    ArticleItem, FIELDS,
    URL, FINGERPRINT, TEXT, TAGS, DATE, HEADER, MEDIA, ERRORS
)

cfg = config.cfg
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


COLUMNS_TUPLE = (URL, HEADER, TAGS, TEXT, DATE, FINGERPRINT)


class GSpreadMaster:

    _secret_file_name = 'client-secret.json'  # library_depend

    def __init__(self):
        self._credentials = self._get_credentials()
        self._client = self._get_client()
        self.spreadsheet = self._client.open(cfg.spreadsheet_title)

    def _get_credentials(self) -> Credentials:
        return Credentials.from_json_keyfile_name(
            cfg.client_secret_path, ['https://spreadsheets.google.com/feeds'])

    def _get_client(self) -> gspread.Client:
        return gspread.authorize(self._credentials)

    def get_worksheet_by_spider(self, spider: scrapy.spiders.Spider) \
            -> gspread.Worksheet:
        try:
            index = cfg.get_worksheet_id(spider.name)
        except KeyError:
            raise RuntimeError(
                f'No worksheet configured for this spider: {spider.name}')
        try:
            worksheet = self.spreadsheet.get_worksheet(index)
            assert worksheet is not None
        except AssertionError:
            raise RuntimeError(
                f'No worksheet exist for this spider: {spider.name}/{index}')
        return worksheet

    @property
    def secret_file_name(self):
        return self._secret_file_name


class GSpreadRow:
    """ Place to configure fields order in a table"""

    columns_order = COLUMNS_TUPLE
    empty_cell = '- - -'

    def __init__(self, item: ArticleItem or dict = None, **fields):
        if item is not None:
            self.item_dict = dict(item)
        else:
            self.item_dict = fields

        for k in self.item_dict:
            if k not in self.columns_order:
                raise KeyError

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
