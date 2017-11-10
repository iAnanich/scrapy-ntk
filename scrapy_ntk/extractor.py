import logging
import abc

from scrapy.selector import SelectorList

from .parser import ESCAPE_CHAR_PAIRS, Parser, MediaCounter, ElementsChain
from .middleware import HTMLMiddleware, MiddlewareContainer, select
from .item import (
    ERRORS,
    MEDIA,
    TAGS,
    TEXT,
    HEADER,
)


logger = logging.getLogger(__name__)


# names
LINK = 'link'
NAMES = frozenset({TEXT, HEADER, TAGS, LINK})


class Extractor(abc.ABC):

    name: str = None

    exception_template = '!!! {type}: {message} !!!'

    def __init__(self):
        self._fields_storage = {field: None for field in self.fields}
        self._is_ready = False

    def _format_exception(self, exception: Exception):
        logger.exception(str(exception))
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


class CSSExtractor(Extractor, abc.ABC):
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
                logger.warning(msg)
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


class VoidExtractor(Extractor):
    """
    Extractor that returns "void".
    Returns `''` string for `extract_from` method.
    Returns `SelectorList()` object for `select_from` method.
    """

    def __init__(self, name: str):
        if name not in NAMES:
            raise RuntimeError('Wrong `name` passed: "{}"'.format(name))
        self.name = name

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


# ===================
#  actual extractors
# ===================
VOID_TAGS = VoidExtractor(TAGS)
VOID_TEXT = VoidExtractor(TEXT)
VOID_HEADER = VoidExtractor(HEADER)
VOID_LINK = GeneratorCSSVoidExtractor(LINK)


class LinkExtractor(MultipleCSSExtractor, GeneratorCSSExtractor):

    name = LINK

    replace_with = []
    allowed_ends = ['a::attr(href)']


class HeaderExtractor(SingleCSSExtractor):

    name = HEADER

    allowed_ends = ['::text']

    def select_from(self, selector: SelectorList) -> SelectorList:
        selected = selector.css(self.string_selector)
        if not selected:
            raise RuntimeError('Failed to select.')
        return selected

    def extract_from(self, selector: SelectorList) -> str:
        extracted_list = self.select_from(selector).extract()
        # `extracted_list` list must not be empty
        if len(extracted_list) == 1:
            extracted = extracted_list[0]
        else:
            raise RuntimeError('Too many items to extract.')
        formatted = self._format(extracted)
        return formatted


class TagsExtractor(SingleCSSExtractor, JoinableExtractor):

    name = TAGS
    allowed_ends = ['::text']
    raise_on_missed = False

    def extract_from(self, selector: SelectorList) -> str:
        extracted = self.select_from(selector).extract()
        converted = self.join(extracted)
        return converted


class TextExtractor(JoinableExtractor):

    name = TEXT

    separator = '\n'

    _fields = {TEXT, ERRORS, MEDIA}

    def __init__(self, string_css_selector: str,
                 parser_class: type,
                 middleware_list: list=None,
                 media_counter_class: type=None,
                 elements_chain_class: type=None, ):
        self.string_selector = string_css_selector

        # Parser stuff
        if not isinstance(parser_class(), Parser):
            raise RuntimeError('Given `parser_class` is not inherited from '
                               '`parser.Parser` class.')
        if not media_counter_class:
            media_counter_class = MediaCounter
        if not elements_chain_class:
            elements_chain_class = ElementsChain
        if not isinstance(elements_chain_class(), ElementsChain):
            raise RuntimeError('Given `elements_chain` is not inherited from '
                               '`parser.ElementsChain` class.')
        if not isinstance(media_counter_class(), MediaCounter):
            raise RuntimeError('Given `media_counter` is not inherited from '
                               '`parser.MediaCounter` class.')
        self.middleware_container = MiddlewareContainer([
            HTMLMiddleware(select, args=(string_css_selector,), )
        ])
        if middleware_list:
            for middleware in middleware_list:
                    self.middleware_container.append(middleware)
        self.elements_chain_class = elements_chain_class
        self.media_counter_class = media_counter_class
        self.parser_class = parser_class

        super().__init__()

    def select_from(self, selector: SelectorList):
        return self.middleware_container.process(selector)

    def extract_from(self, selector: SelectorList) -> str:
        selected = self.select_from(selector)
        elements: list = self.parse_selected(selected)
        formatted = self.join(elements)
        return formatted

    def parse_selected(self, selector_list: SelectorList) -> list:
        parser = self._open_parser(
            parser_class=self.parser_class,
            media_counter_class=self.media_counter_class,
            elements_chain_class=self.elements_chain_class,
        )
        for selector in iter(selector_list):
            parser.safe_parse(selector)

        self._save_result(self.join(parser.errors_list), ERRORS)
        self._save_result(parser.summary, MEDIA)

        return self._close_parser(parser)

    @staticmethod
    def _open_parser(parser_class: type, media_counter_class: type,
                     elements_chain_class: type) -> Parser:
        return parser_class(
            media_counter=media_counter_class(),
            elements_chain=elements_chain_class(),
        )

    @staticmethod
    def _close_parser(parser: Parser) -> list:
        return parser.close()


# ============
#  management
# ============
class ExtractManager:

    def __init__(self, link_extractor: LinkExtractor =None,
                 header_extractor: HeaderExtractor =None,
                 text_extractor: TextExtractor =None,
                 tags_extractor: TagsExtractor =None, ):
        self._check_type('link_extractor', link_extractor, LinkExtractor)
        self._check_type('header_extractor', header_extractor, HeaderExtractor)
        self._check_type('text_extractor', text_extractor, TextExtractor)
        self._check_type('tags_extractor', tags_extractor, TagsExtractor)

        self.item_extractors = frozenset([
            text_extractor,
            tags_extractor,
            header_extractor
        ])
        self._fields_dict = {k: None for k in self.fields}
        self._item_names_dict = {e.name: False for e in self.item_extractors}
        self._name_to_field_dict = {e.name: tuple(e.fields)
                                    for e in self.item_extractors}

    def _check_type(self, name: str, variable, parent: type):
        if isinstance(variable, VoidExtractor) or \
                isinstance(variable, GeneratorCSSVoidExtractor):
            logger.warning(
                '`{}` initialised with `extractor.VoidExtractor` '
                'class and will not return any useful data.'.format(name))
        elif not isinstance(variable, parent):
            raise TypeError('Passed `{}` variable must be `{}` type.'
                            .format(name, parent.__class__))
        self.__setattr__(name, variable)

    def get_dict(self):
        def raise_error():
            raise ValueError('Not all extractors are ready yet.')
        return {k: v if v is not None else raise_error()
                for k, v in self._fields_dict.items()}

    def extract(self, obj, name: str=None, field: str=None) -> dict:
        if name is None and field is None:
            raise ValueError('You must specify `name` or `field` argument.')
        elif name is None and field is not None:
            raise ValueError('You must pass in only one argument.')
        elif name is not None:
            extractor = self._get_extractor_by_name(name)
            return_field = False
        else:  # field is not None
            extractor = self._get_extractor_by_field(field)
            return_field = True
        extractor.safe_extract_from(obj)
        dictionary = extractor.get_dict()
        self._save_result(
            dictionary,
            name=self._field_to_name(field) if return_field else name)
        if return_field:
            return {field: dictionary[field]}
        else:
            return dictionary

    def extract_all(self, obj) -> dict:
        for name in self.names:
            self.extract(obj, name)
        return self.get_dict()

    def _get_extractor_by_name(self, name: str):
        for extractor in self.item_extractors:
            if extractor.name == name:
                return extractor
        else:
            raise ValueError('No extractors with this name')

    def _get_extractor_by_field(self, field: str):
        for extractor in self.item_extractors:
            if field in extractor.fields:
                return extractor
        else:
            raise ValueError('No extractors with this name')

    def _save_result(self, dictionary: dict, name: str):
        if name not in self._item_names_dict.keys():
            raise ValueError('No extractors with this name.')
        self._fields_dict.update(dictionary)
        self._item_names_dict[name] = True

    def _field_to_name(self, field):
        for name, fields in self._name_to_field_dict.items():
            if field in fields:
                return name
        else:
            raise ValueError('Can not find this field.')

    @property
    def fields(self):
        for extractor in self.item_extractors:
            for field in extractor.fields:
                yield field

    @property
    def names(self):
        yield from (e.name for e in self.item_extractors)
