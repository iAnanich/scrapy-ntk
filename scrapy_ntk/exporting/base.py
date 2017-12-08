import abc
import logging
from typing import List

from scrapy.exporters import BaseItemExporter

from ..item import ArticleItem


class BaseArticleItemWriter(abc.ABC):

    _names = set()

    def __init__(self, *, name: str=None):
        if name is not None and name not in self._names:
            self._name = name
        else:
            self._name = f'#{len(self._names) + 1}'
        self._names.add(self._name)

        self.logger = self.create_logger()

    def create_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)
        return logger

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


class BaseArticleItemExporter(BaseItemExporter, abc.ABC):

    _writer_type = None

    def __init__(self, *, writer, enable_postpone_mode: bool =True, **kwargs):
        self.logger = self.create_logger()

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

    def create_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)
        return logger

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
