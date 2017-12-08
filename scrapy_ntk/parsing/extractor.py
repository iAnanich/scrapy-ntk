from scrapy.selector import SelectorList

from .parser import Parser, MediaCounter, ElementsChain
from .middleware import HTMLMiddleware, MiddlewareContainer, select
from .base_extractor import (
    LINK, TEXT, ERRORS, MEDIA, TAGS, HEADER,
    VoidExtractor, GeneratorCSSExtractor, JoinableExtractor,
    MultipleCSSExtractor, SingleCSSExtractor, GeneratorCSSVoidExtractor,
)


# --- field mixins ---
class TextExtractorMixin:
    name = TEXT
    _fields = {TEXT, ERRORS, MEDIA}


class TagsExtractorMixin:
    name = TAGS
    _fields = {TAGS}


class HeaderExtractorMixin:
    name = HEADER
    _fields = {HEADER}


class LinkExtractorMixin:
    name = LINK


# --- Void Extractors ---
class TextVoidExtractor(TextExtractorMixin, VoidExtractor):

    def extract_from(self, obj: object):
        self._save_result('', ERRORS)
        self._save_result('', MEDIA)
        return ''


class TagsVoidExtractor(TagsExtractorMixin, VoidExtractor):
    pass


class HeaderVoidExtractor(HeaderExtractorMixin, VoidExtractor):
    pass


class LinkVoidExtractor(LinkExtractorMixin, GeneratorCSSVoidExtractor):
    pass


# ===================
#  actual extractors
# ===================
VOID_TAGS = TagsVoidExtractor()
VOID_TEXT = TextVoidExtractor()
VOID_HEADER = HeaderVoidExtractor()
VOID_LINK = LinkVoidExtractor()


class LinkExtractor(LinkExtractorMixin, MultipleCSSExtractor, GeneratorCSSExtractor):

    replace_with = []
    allowed_ends = ['a::attr(href)']


class HeaderExtractor(HeaderExtractorMixin, SingleCSSExtractor):

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


class TagsExtractor(TagsExtractorMixin, SingleCSSExtractor, JoinableExtractor):

    allowed_ends = ['::text']
    raise_on_missed = False

    def extract_from(self, selector: SelectorList) -> str:
        extracted = self.select_from(selector).extract()
        converted = self.join(extracted)
        return converted


class TextExtractor(TextExtractorMixin, JoinableExtractor):

    separator = '\n'

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


# - - - = ========== = - - -
# -       management       -
# - - - = ========== = - - -
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

    def _check_type(self, name: str, extractor, klass: type):
        if isinstance(extractor, VoidExtractor) or \
                isinstance(extractor, GeneratorCSSVoidExtractor):
            extractor.logger.warning(
                '`{}` initialised with `extractor.VoidExtractor` '
                'class and will not return any useful data.'.format(name))
        elif not isinstance(extractor, klass):
            raise TypeError('Passed `{}` variable must be `{}` type.'
                            .format(name, klass.__class__))
        self.__setattr__(name, extractor)

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
        result = self.get_dict()
        return result

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
