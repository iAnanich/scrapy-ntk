import abc

from scrapy.selector import SelectorList

from .parser import ESCAPE_CHAR_PAIRS
from ..base import BaseExtractor
from ..item import (
    TAGS,
    TEXT,
    HEADER,
    ERRORS,
    MEDIA,
)


# names
LINK = 'link'
NAMES = frozenset({TEXT, HEADER, TAGS, LINK})


class CSSExtractor(BaseExtractor, abc.ABC):
    """
    Extracts data from HTML using `scrapy.selector.SelectorList.css` method.
    It's more useful than old `ParseMixin` class implementation because
    allows you to you inheritance between extractors and customize them for
    every spider.
    """

    replace_with = ESCAPE_CHAR_PAIRS
    allowed_ends = []

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
            raise TypeError('Given `string_selector` argument isn\'t `str`.')
        for end in self.allowed_ends:
            if string_selector.endswith(end):
                break
        else:
            raise ValueError(
                'Given `string_selector` (="{}") argument isn\' valid.'
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

    def select_from(self, selector: SelectorList) -> SelectorList:
        """
        Uses `list_of_string_selectors` attribute to create new selector from
        the given one.
        :param selector: `SelectorList` object from what method selects using
        initial arguments
        :return: selected `SelectorList` object
        """
        raise NotImplementedError


class JoinableExtractor(CSSExtractor, abc.ABC):

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


class MultipleCSSExtractor(CSSExtractor, abc.ABC):

    def __init__(self, list_of_string_css_selectors: list):
        for string_selector in list_of_string_css_selectors:
            self._check_string_selector(string_selector)
        self.list_of_string_selectors = list_of_string_css_selectors

        super().__init__()


class SingleCSSExtractor(CSSExtractor, abc.ABC):

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


class GeneratorCSSExtractor(CSSExtractor, abc.ABC):

    def select_from(self, selector: SelectorList):
        """
        :param selector: `SelectorList` object from what we selects data
        :exception RuntimeError: if even one string selector in
        `self.list_of_string_selectors` fails
        :return: generator of `Selector` objects
        """
        if hasattr(self, 'list_of_string_selectors'):
            list_of_string_selectors = self.list_of_string_selectors
        elif hasattr(self, 'string_selector'):
            list_of_string_selectors = [self.string_selector]
        else:
            raise AttributeError(
                'Can not found any attributes with string selectors.')
        for string_selector in list_of_string_selectors:
            selected = selector.css(string_selector)
            if selected:
                for item in selected:
                    yield item
            else:
                raise RuntimeError(
                    '`{}` selector failed'.format(string_selector))

    def extract_from(self, selector: SelectorList):
        for selected in self.select_from(selector):
            yield self._format(selected.extract())


class VoidExtractor(BaseExtractor):
    """
    Extractor that returns "void".
    Returns `''` string for `extract_from` method.
    Returns `SelectorList()` object for `select_from` method.
    """

    def __init__(self, name: str =None):
        if hasattr(self, 'name') and name is None:
            pass
        elif name is not None:
            self.name = name
        elif name not in NAMES:
            msg = f'Wrong `name` passed: "{name}"'
            self.logger.error(msg)
            raise RuntimeError(msg)

        super().__init__()

    def extract_from(self, selector: SelectorList) -> str:
        return ''

    _is_ready = True


class GeneratorCSSVoidExtractor(GeneratorCSSExtractor, VoidExtractor):

    def select_from(self, selector: SelectorList):
        yield SelectorList()

    def extract_from(self, selector: SelectorList):
        yield ''

    _is_ready = True
