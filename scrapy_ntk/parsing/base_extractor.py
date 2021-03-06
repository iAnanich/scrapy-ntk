import abc
import logging
import typing

from scrapy.selector import SelectorList

from .parser import ESCAPE_CHAR_PAIRS
from ..base import ExtractorABC, LoggableBase, FieldsStorageABC
from ..item import TAGS, TEXT, HEADER


POSSIBLE_EXTRACTOR_NAMES = frozenset({TEXT, HEADER, TAGS})


class DictFieldsStorage(FieldsStorageABC):

    def __init__(self, fields: typing.Sequence[str]):
        super().__init__(fields)

        self._storage: dict = self._new_storage()

    def _new_storage(self) -> dict:
        return {k: None for k in self._fields}

    def reset(self):
        self._storage = self._new_storage()

    def set(self, field: str, value: str):
        if field not in self._fields:
            raise ValueError
        self._storage[field] = value

    def dict_copy(self) -> typing.Dict[str, str]:
        return self._storage.copy()


DefaultFieldsStorage = DictFieldsStorage


class BaseExtractor(ExtractorABC, LoggableBase, abc.ABC):

    exception_template = '!!! {type}: {message} !!!'
    fields_storage_type = DefaultFieldsStorage

    def __init__(self, logger: logging.Logger =None):
        if logger is None:
            logger = self.create_logger()
        self.logger = logger

        self.fields_storage = self.fields_storage_type(self.fields)
        self._is_ready = False

    def create_logger(self, name=None):
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
        self.fields_storage.set(field, str(result))

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
            self.fields_storage.reset()
        return string

    def get_dict(self):
        return self.fields_storage.dict_copy()

    # properties
    @property
    def fields(self):
        return self._attr_checker('_fields', frozenset, {self.name})

    @property
    def ready(self):
        return self._is_ready

    @ready.setter
    def ready(self, val: bool):
        self._is_ready = bool(val)

    def __repr__(self):
        return f'<{self.__class__.__name__} : "{self.name}">'


class CSSExtractor(BaseExtractor, abc.ABC):
    """
    Extracts data from HTML using `scrapy.selector.SelectorList.css` method.
    It's more useful than old `ParseMixin` class implementation because
    allows you to you inheritance between extractors and customize them for
    every spider.
    """

    replace_with = ESCAPE_CHAR_PAIRS
    allowed_ends = []
    raise_on_missed = True

    def __init__(self, string_css_selector: str =None):
        self._check_string_selector(string_css_selector)
        self.string_selector = string_css_selector

        super().__init__()

    def select_from(self, selector: SelectorList) -> SelectorList:
        selected = selector.css(self.string_selector)
        if not selected:
            msg = 'Not found any "{}" containers.'.format(self.name)
            if self.raise_on_missed:
                raise RuntimeError(msg)
            else:
                self.logger.warning(msg)
                return SelectorList([])
        return selected

    def _check_string_selector(self, string_selector: str):
        """
        Checks if given `string_selector` ends with strings defined in
        `allowed_ends` class field.
        :param string_selector: string CSS selector
        :exception ValueError: raises if non of strings in `self.allowed_ends`
        list are at the end of given `string_selector` string
        :return: None
        """
        if not isinstance(string_selector, str):
            raise TypeError('Given `string_selector` argument is not `str`.')
        for end in self.allowed_ends:
            if string_selector.endswith(end):
                break
        else:
            raise ValueError(
                'Given `string_selector` (="{}") argument is not valid.'
                .format(string_selector))

    def _format(self, text: str) -> str:
        """
        Formats given `text` string be iterating over tuples from
        `replace_with` field list's pairs in format
        `(what_to_replace, replace_with)`
        :param text: `str` object
        :return: `str` object
        """
        for before, after in self.replace_with:
            text = text.replace(before, after)
        return text


class JoinableCSSExtractor(CSSExtractor, abc.ABC):

    default = ''
    separator = ', '

    def join(self, lst: list) -> str:
        """
        Converts given list of strings to string by formatting every item in
        the `lst` list and joining them with `self.separator`
        :param lst: list of `str` objects
        :return: `str` object. If given list is empty returns `self.default`
        """
        if len(lst) == 0:
            return self.default
        string = self._format(lst[0])
        for item in lst[1:]:
            formatted = self._format(item)
            if formatted:
                string += self.separator + self._format(item)
        return string


class VoidExtractor(BaseExtractor):
    """
    Extractor that returns "void".
    Returns `''` string for `extract_from` method.
    Returns `SelectorList()` object for `select_from` method.
    """

    name: str = None
    _is_ready = True

    def __init__(self, name: str =None):
        if hasattr(self, 'name') and name is None:
            pass
        elif name is not None:
            self.name = name
        elif name not in POSSIBLE_EXTRACTOR_NAMES:
            msg = f'Wrong `name` passed: "{name}"'
            self.logger.error(msg)
            raise RuntimeError(msg)

        super().__init__()

    def extract_from(self, selector: SelectorList) -> str:
        return ''
