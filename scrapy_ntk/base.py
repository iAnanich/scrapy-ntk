import abc
import logging
import random
import string
from datetime import datetime
from typing import List, FrozenSet, Dict, NoReturn

from scrapy import Spider
from scrapy.exporters import BaseItemExporter
from scrapy.http import Response, Request

from .config import cfg
from .item import (
    ArticleItem,
    URL, FINGERPRINT, DATE
)
from .utils.args import to_bool, to_str, from_set
from .utils.check import check_obj_type
from .proxy.modes import PROXY_MODES
from .utils.helpers import collect_kwargs


class BaseArticleSpider(abc.ABC, Spider):

    _enable_proxy = False
    _proxy_mode = None

    _article_item_class = ArticleItem

    _meta_fingerprint_key = f'article__{FINGERPRINT}'
    _meta_datetime_key = f'article__{DATE}'

    _default_request_meta: dict = None

    name: str = None

    def __init__(self, *args, **kwargs):
        # check proxy
        if self._enable_proxy or to_bool(cfg.enable_proxy):
            self._enable_proxy = True
            if self._proxy_mode is None:
                proxy_mode = to_str(cfg.proxy_mode)
                if from_set(proxy_mode, PROXY_MODES, raise_=True):
                    self._proxy_mode = proxy_mode
            self.logger.info('Spider set `_enable_proxy=True`.')
            self.logger.info(f'Spider set `_proxy_mode={self._proxy_mode}`.')

        super().__init__(*args, **kwargs)

    def new_article_item(self, response: Response, **kwargs):
        """
        Yields `ArticleItem` instances with `url` and `fingerprint` arguments
        extracted from given `response` object.
        :param response: `scrapy.http.Response` from "article page"
        :param kwargs: fields for `ArticleItem`
        :return: yields `ArticleItem` instance
        """
        meta = dict(response.meta)
        fingerprint = meta.get(self._meta_fingerprint_key, self.get_random_fingerprint())
        date_time = meta.get(self._meta_datetime_key, datetime.utcnow())

        # initialise default values
        dict_item = {
            URL: response.url,
            FINGERPRINT: fingerprint,
            DATE: date_time,
        }
        # override default values
        dict_item.update(kwargs)
        return self._article_item_class(**dict_item)

    def new_request(self, url, callback=None, method='GET', headers=None,
                    body=None, cookies=None, meta=None, encoding='utf-8',
                    priority=0, dont_filter=False, errback=None, flags=None):
        kwargs = collect_kwargs(locals())
        return Request(**kwargs)

    @property
    def request_meta(self):
        if self._default_request_meta:
            meta = self._default_request_meta.copy()
            return meta
        return {}

    @staticmethod
    def get_random_fingerprint():
        length = 8
        return ''.join(random.SystemRandom().choice(
            string.ascii_uppercase + string.digits) for _ in range(length))

    @property
    def enable_proxy(self):
        return self._enable_proxy

    @property
    def proxy_mode(self):
        return self._proxy_mode


class LoggableBase(abc.ABC):

    def create_logger(self, name=None) -> logging.Logger:
        if name is None:
            name = self.name
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        return logger

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass


class BaseArticleItemWriter(LoggableBase, abc.ABC):

    _names = set()

    def __init__(self, *, name: str=None, logger: logging.Logger =None):

        default_name = f'#{len(self._names) + 1}'
        if name is not None:
            if name not in self._names:
                self._name = name
            else:
                self.create_logger(
                    f'<{self.__class__.__name__}> Initialisator'
                ).warning(
                    f'Passed "{name}" is already in use, '
                    f'"{default_name}" will be used instead.')
                self._name = default_name
        else:
            self._name = default_name
        self._names.add(self._name)

        if logger is None:
            logger = self.create_logger()
        self.logger = logger

    @abc.abstractmethod
    def write(self, *items: List[ArticleItem]):
        pass

    @property
    def name(self) -> str:
        return f'{self.__class__.__name__}<{self._name}>'

    def __repr__(self):
        return f'<{self.name}>'


class BaseArticleItemExporter(LoggableBase, BaseItemExporter, abc.ABC):

    _writer_type = None

    def __init__(self, *, writer: BaseArticleItemWriter, enable_postpone_mode: bool =True,
                 logger: logging.Logger =None, **kwargs):
        if logger is None:
            logger = self.create_logger()
        self.logger = logger

        if not isinstance(writer, self._writer_type):
            msg = f'Writer with wrong type passed: {writer} with ' \
                  f'{type(writer)} type while {self._writer_type} expected.'
            self.logger.error(msg)
            raise TypeError(msg)

        self._writer = writer
        self._postpone_mode_enabled = enable_postpone_mode

        # define
        self._items = []
        self._counter = 0
        self._is_active = None

        # log info
        self.logger.info(f'Writer initialised = {self._writer}')

        super().__init__(**kwargs)

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def items(self):
        return self._items

    @property
    def count(self):
        return self._counter

    @property
    def status(self):
        if self._is_active is None:
            return 'idle'
        elif self._is_active is True:
            return 'active'
        elif self._is_active is False:
            return 'closed'
        else:
            msg = f'Can not recognise status: `_is_active == {self._is_active}'
            self.logger.error(msg)
            raise TypeError(msg)

    def start_exporting(self):
        self._is_active = True
        self._start()

    def finish_exporting(self):
        if self._postpone_mode_enabled:
            self._finish_postpone()
        self._is_active = False
        self._finish()

    def export_item(self, item):
        if self._is_active is not True:
            raise RuntimeError(
                'Can not append item when session have "{}" status'
                .format(self.status))
        if not isinstance(item, ArticleItem):
            raise TypeError('Can not export item that is not {}'
                            .format(ArticleItem.__name__))
        self._export(item)

    def _start(self):
        pass

    def _finish_postpone(self):
        self._writer.write(*self._items)

    def _finish(self):
        pass

    def _export(self, item):
        self._counter += 1
        if self._postpone_mode_enabled:
            self._items.append(item)
        else:
            self._writer.write(item)

    def __repr__(self):
        return f'<{self.name} :: ' \
               f'status: "{self.status}", items: {len(self._items)}>'


class BaseArticlePipeline(LoggableBase, abc.ABC):

    def __init__(self, logger: logging.Logger =None):
        if logger is None:
            logger = self.create_logger()
        self.logger = logger

        self._exporter: BaseArticleItemExporter = None
        self._master = None

        self._state = None

    @abc.abstractmethod
    def setup_exporter(self, spider: BaseArticleSpider):
        pass

    def __repr__(self):
        return f'<{self.name} :: {self.exporter} state: {self._state}>'

    @property
    def name(self):
        return f'{self.__class__.__name__}'

    @property
    def is_active(self) -> bool:
        return bool(self._state)

    @is_active.setter
    def is_active(self, val: bool):
        state = bool(val)
        self._state = state
        self.logger.info(f'state update: {self._state}')

    @property
    def exporter(self) -> BaseArticleItemExporter:
        return self._exporter

    @exporter.setter
    def exporter(self, new: BaseArticleItemExporter):
        if not isinstance(new, BaseArticleItemExporter):
            exporter_type_msg = \
                f'You are trying to set "exporter" ' \
                f'with wrong type: {type(new)}'
            self.logger.error(exporter_type_msg)
            raise TypeError(exporter_type_msg)
        else:
            self.logger.debug(f'{new} exporter settled up.')
            self._exporter = new

    @property
    def master(self):
        return self._master

    @master.setter
    def master(self, new):
        self._master = new

    def open_spider(self, spider: BaseArticleSpider):
        try:
            self.setup_exporter(spider)
        except Exception as exc:
            self.logger.exception(f'Error while setting up {self.name}: {exc}')
            self.is_active = False
        else:
            if self.exporter is None:
                self.logger.warning(f'"exporter" is not set up.')
                self.is_active = False
            else:
                self.is_active = True
            if self.master is None:
                self.logger.warning(f'"master" attribute is not set.')

        if self._state:
            try:
                self.exporter.start_exporting()
            except Exception as exc:
                self.logger.exception(f'Error while starting exporting with {self.exporter}: {exc}')
                self.is_active = False

    def close_spider(self, spider):
        if self._state:
            self.exporter.finish_exporting()
            self.logger.info(
                f'Successfully finished <{self.exporter.name}> '
                f'exporter with {self.exporter.count} items exported.')

    def process_item(self, item: ArticleItem, spider) -> ArticleItem:
        if isinstance(item, ArticleItem):
            if self._state:
                try:
                    self.exporter.export_item(item)
                except Exception as exc:
                    self.logger.exception(
                        f'Error while exporting '
                        f'<{item["fingerprint"]}> item: {exc}')
        else:
            pass
        return item


# TODO: move to separate file
class FieldsStorageABC(abc.ABC):
    """
    Defines interface for `FieldsStorage` classes.
    """

    FieldNameType: type = object
    FieldValueType: type = object
    FieldsDictType: type = Dict[FieldNameType, FieldValueType]

    @abc.abstractmethod
    def __init__(self, field_names: FrozenSet[FieldNameType]):
        self._all_field_names: frozenset = frozenset(field_names)
        self._filled_field_names: set = set()

    @abc.abstractmethod
    def update(self, dictionary: FieldsDictType) -> NoReturn:
        """
        Behaves as ``dict.update`` method.
        :param dictionary:
        :return:
        """
        pass

    @abc.abstractmethod
    def dict_copy(self) -> FieldsDictType:
        """
        Returns dictionary copy
        :return:
        """
        pass

    @abc.abstractmethod
    def set(self, field_name: FieldNameType, field_value: FieldValueType) -> NoReturn:
        """
        Validates field and value, then sets field's value if given ``field``
        is allowed by all fields set, and updates set of filled fields.
        :param field_name: ``str``
        :param field_value: ``str``
        :return: ``None``
        """
        pass

    @abc.abstractmethod
    def get(self, field_name: FieldNameType) -> FieldValueType:
        """
        Validates field and returns it's value.
        :param field_name: field's name
        :raise KeyError: if no such ``field_name`` was found.
        :return: related value
        """
        pass

    @abc.abstractmethod
    def reset(self) -> NoReturn:
        """
        Drops fields' values, and clears set of filled fields.
        :return: ``None``
        """
        pass

    @abc.abstractmethod
    def release(self) -> FieldsDictType:
        """
        Returns dict copy and clears storage.
        :return: field name to it's value mapping.
        """
        pass

    @abc.abstractmethod
    def is_full(self) -> bool:
        """
        Checks whether or not all fields were filled wis valid values.
        :return: ``True`` if all fields has their values.
        """
        pass

    @abc.abstractmethod
    def has_value(self, field_name: FieldNameType) -> bool:
        """
        Checks whether or net given ``field`` filed has it's value.
        :param field_name: one of the allowed fields.
        :return: ``True`` if field has value.
        """

    @abc.abstractmethod
    def validate_field_name(self, field_name) -> FieldNameType:
        """
        Validates field name, by just searching for it in set of allowed fields.
        :param field_name: field name to validate
        :raise KeyError: if no such ``field_name`` was found.
        :return: validated field name
        """
        pass

    @abc.abstractmethod
    def validate_field_value(self, field_value, *,
                             valid_field_name: FieldNameType) -> FieldValueType:
        """
        Validates value. Uses ``valid_field_name`` for error messages.
        :param field_value: value to validate
        :param valid_field_name: valid field name.
        :raise ValueError: no such case yet
        :raise TypeError: if ``field_value`` is not an instance of ``FieldValueType``
        :return: validated value
        """
        pass


class BaseStringFieldsStorage(FieldsStorageABC, metaclass=abc.ABCMeta):

    FieldNameType = str
    FieldValueType = str
    FieldsDictType = Dict[FieldNameType, FieldValueType]

    def __init__(self, field_names: FrozenSet[FieldNameType]):
        for f in field_names:
            check_obj_type(f, self.FieldNameType, f'Field')

        self._all_field_names: frozenset = frozenset(field_names)
        self._filled_field_names: set = set()

        # do nothing
        super().__init__(field_names)

    def validate_field_name(self, field_name) -> FieldNameType:
        if field_name not in self._all_field_names:
            raise KeyError(
                f'No such "{field_name}" field name found.'
            )
        return field_name

    def validate_field_value(self, field_value, *,
                             valid_field_name: FieldNameType) -> FieldValueType:
        check_obj_type(field_value, str, f'Value of the "{valid_field_name}" field.')
        return field_value

    def set(self, field_name: FieldNameType, field_value: FieldValueType) -> NoReturn:
        valid_field_name = self.validate_field_name(field_name)
        valid_field_value = self.validate_field_value(
            field_value, valid_field_name=valid_field_name)
        self._set(valid_field_name=valid_field_name, valid_field_value=valid_field_value)
        self._filled_field_names.add(valid_field_name)

    def get(self, field: FieldNameType) -> FieldValueType:
        valid_field_name = self.validate_field_name(field)
        return self._get(valid_field_name=valid_field_name)

    def update(self, dictionary: FieldsDictType) -> NoReturn:
        for key, value in dictionary.items():
            self.set(field_name=key, field_value=value)

    def reset(self) -> NoReturn:
        self._reset()
        self._filled_field_names.clear()

    def release(self) -> FieldsDictType:
        res = self.dict_copy()
        self.reset()
        return res

    def has_value(self, field_name: FieldNameType) -> bool:
        if field_name in self._filled_field_names:
            return True
        elif field_name in self._all_field_names:
            return False
        else:
            raise KeyError(f'Unknown field: "{field_name}.')

    def is_full(self) -> bool:
        return self._filled_field_names == self._all_field_names

    def dict_copy(self) -> dict:
        return {field_name: self.get(field_name)
                for field_name in self._filled_field_names}

    @abc.abstractmethod
    def _set(self, valid_field_name: FieldNameType, valid_field_value: FieldValueType) -> NoReturn:
        """
        Abstraction over implementation details for setting ``field``'s
        ``value`` without overriding it.
        :param valid_field_name: validated field
        :param valid_field_value: validated value
        :return:
        """
        pass

    @abc.abstractmethod
    def _reset(self) -> NoReturn:
        """
        Drops fields' values.
        :return: ``None``
        """
        pass

    @abc.abstractmethod
    def _get(self, valid_field_name: FieldNameType) -> FieldValueType:
        """
        Returns value related to given ``field`` field.
        :raise KeyError: if no such ``field_name`` was found.
        :return: related value
        """
        pass


class ExtractorABC(abc.ABC):

    name: str

    @abc.abstractmethod
    def extract_from(self, obj: object) -> object:
        """
        Extracts object from given object.
        :param obj: object to extract from
        :return: extracted string
        """
        pass

    @abc.abstractmethod
    def safe_extract_from(self, obj: object) -> object:
        """
        Same as ``extract_from`` but catches all errors and resets fields
        storage id any.
        :param obj: object to extract from
        :return: extracted object
        """
        pass

    @abc.abstractmethod
    def get_dict(self) -> dict:
        """
        Returns collected field values.
        :return: ``field`` to ``value`` dictionary
        """
        pass

    # properties
    @property
    @abc.abstractmethod
    def fields(self) -> frozenset:
        """
        Returns set of fields.
        :return: ``frozenset`` object
        """
        pass

    @property
    @abc.abstractmethod
    def ready(self):
        """
        ``True`` means that field values were extracted.
        :return: boolean
        """
        pass

    @ready.setter
    @abc.abstractmethod
    def ready(self, val: bool):
        pass
