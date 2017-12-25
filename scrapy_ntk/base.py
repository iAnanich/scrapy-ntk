import abc
import logging
import random
import string
from datetime import datetime
from typing import List

from scrapy import Spider
from scrapy.exporters import BaseItemExporter
from scrapy.http import Response

from .config import cfg
from .item import (
    ArticleItem,
    URL, FINGERPRINT, DATE
)


def _to_bool(string: str) -> bool:
    if string in ['True', '1']:
        return True
    elif string in ['False', '0']:
        return False
    else:
        raise ValueError('Unknown string value: ' + string)


class BaseArticleSpider(abc.ABC, Spider):

    _enable_proxy = False
    _proxy_mode = None

    _article_item_class = ArticleItem

    name: str = None

    def __init__(self, *args, **kwargs):
        # check proxy
        if self._enable_proxy or _to_bool(cfg.enable_proxy):
            self._enable_proxy = True
            self._proxy_mode = self._proxy_mode or cfg.proxy_mode
            self.logger.info('Spider set `_enable_proxy=True`.')
            self.logger.info('Spider set `_proxy_mode={}`.'
                        .format(self._proxy_mode))

        super().__init__(*args, **kwargs)

    def _yield_article_item(self, response: Response, **kwargs):
        """
        Yields `ArticleItem` instances with `url` and `fingerprint` arguments
        extracted from given `response` object.
        :param response: `scrapy.http.Response` from "article page"
        :param kwargs: fields for `ArticleItem`
        :return: yields `ArticleItem` instance
        """
        try:
            fingerprint = response.meta['fingerprint']
        except KeyError:
            # case when used with `crawl` command
            fingerprint = self.get_random_fingerprint()
        kwargs.update({
            URL: response.url,
            FINGERPRINT: fingerprint,
            DATE: datetime.now()
        })
        yield self._article_item_class(**kwargs)

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
    def name(self):
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
    def klass(self) -> str:
        return self.__class__.__name__

    @property
    def name(self):
        return f'{self.klass}[{self._name}]'

    def __repr__(self):
        return f'<{self.name}>'


class BaseArticleItemExporter(LoggableBase, BaseItemExporter, abc.ABC):

    _writer_type = None

    def __init__(self, *, writer, enable_postpone_mode: bool =True,
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
        self.logger.info(f'{self.name} | state update: {self._state}')

    @property
    def exporter(self) -> BaseArticleItemExporter:
        return self._exporter

    @exporter.setter
    def exporter(self, new: BaseArticleItemExporter):
        if not isinstance(new, BaseArticleItemExporter):
            exporter_type_msg = \
                f'{self.name} | You are trying to set "exporter" ' \
                f'with wrong type: {type(new)}'
            self.logger.error(exporter_type_msg)
            raise TypeError(exporter_type_msg)
        else:
            self.logger.debug(f'{self.name} | {new} exporter settled up.')
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
            self.logger.exception(f'{self.name} | Error while setting up {self.name}: {exc}')
            self.is_active = False
        else:
            if self.exporter is None:
                self.logger.warning(f'{self.name} | "exporter" is not set up.')
                self.is_active = False
            else:
                self.is_active = True
            if self.master is None:
                self.logger.warning(f'{self.name} | "master" attribute is not set.')

        if self._state:
            try:
                self.exporter.start_exporting()
            except Exception as exc:
                self.logger.exception(f'{self.name} | Error while starting exporting with {self.exporter}: {exc}')
                self.is_active = False

    def close_spider(self, spider):
        if self._state:
            self.exporter.finish_exporting()
            self.logger.info(
                f'{self.name} | Successfully finished <{self.exporter.name}> '
                f'exporter with {self.exporter.count} items exported.')

    def process_item(self, item: ArticleItem, spider) -> ArticleItem:
        if isinstance(item, ArticleItem):
            if self._state:
                try:
                    self.exporter.export_item(item)
                except Exception as exc:
                    self.logger.exception(
                        f'{self.name} | Error while exporting '
                        f'<{item["fingerprint"]}> item: {exc}')
        else:
            pass
        return item


class BaseExtractor(LoggableBase, abc.ABC):

    name: str = None

    exception_template = '!!! {type}: {message} !!!'

    def __init__(self, logger: logging.Logger =None):
        if logger is None:
            logger = self.create_logger()
        self.logger = logger

        self._fields_storage = {field: None for field in self.fields}
        self._is_ready = False

    def create_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)
        return logger

    def _format_exception(self, exception: Exception):
        self.logger.exception(str(exception))
        return self.exception_template.format(
            type=type(exception), message=exception.args)

    def _attr_checker(self, name, converter: type, default=None):
        if hasattr(self, name):
            return converter(getattr(self, name))
        elif default is not None:
            return default
        else:
            raise NotImplementedError(f"Please, implement attribute `{name}`.")

    def _save_result(self, result: str, field: str = None):
        if field is None:
            field = self.name
        if field not in self.fields:
            raise ValueError
        self._fields_storage[field] = str(result)

    @abc.abstractmethod
    def extract_from(self, obj: object) -> str:
        pass

    def safe_extract_from(self, obj: object) -> str:
        try:
            string = self.extract_from(obj)
            self._save_result(string)
            self.ready = True
        except Exception as exc:
            string = self._format_exception(exc)
        return string

    def get_dict(self):
        return self._fields_storage.copy()

    @property
    def fields(self):
        return self._attr_checker('_fields', set, {self.name})

    @property
    def ready(self):
        return self._is_ready

    @ready.setter
    def ready(self, val: bool):
        self._is_ready = bool(val)

    def __repr__(self):
        return f'<{self.__class__.__name__} : "{self.name}">'
