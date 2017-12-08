import abc
import logging
from typing import List

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
