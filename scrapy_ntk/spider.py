# -*- coding: utf-8 -*-
""" Module for spider class templates.

    Entities:
    * news-list page - page on the web-site that have HTML tag (e. g.
    "news-list tag") with multiple childes, where every child HTML tag
    contains a link to an "article page"
    * news-list tag - HTML tag  with multiple childes, where every child HTML
    tag contains a link to an "article page"
    * article page - page on the same web-site that have HTML tag (e. g.
    "article tag") with childes that have all needed data as header, tags etc.
    * article tag - HTML tag  with childes that have all data for scraping
    * fingerprint - part of the article page URL that can be used to identify
    the article page to not scrape it twice
    * callback - method which takes request and yields another request or item
"""

import abc
from typing import Iterable, Iterator, Tuple
from urllib.parse import urlparse, urlunparse

from scrapy import Spider
from scrapy.http import Response, Request

from .base import BaseArticleSpider
from .item import (
    FINGERPRINT, TAGS, TEXT, HEADER, MEDIA, ERRORS, URL,
)
from .parsing import ExtractManager, LinkExtractor
from .tools.cloud import SHub, SHubFetcher, IterManager, Context


def _get_item(lst: list, fingerprint: int, default=None):
    try:
        return lst[fingerprint]
    except IndexError:
        return default


class NewsArticleSpider(BaseArticleSpider, abc.ABC):

    # Just a spider name used by Scrapy to identify it.
    # Must be a string.
    name: str = None

    # URL path to the "news-list page". Used for `start_urls` field.
    # Must be a string. Minimal value: ''
    _start_path = None

    # URL host of the web-site. Used for `allowed_domains` field.
    # Must be a string. Example: 'www.example.com'
    _start_domain = None

    # URL scheme. Allowed values: 'http', 'https'
    _scheme = None

    # Extractors used to extract needed data from HTML
    # Must be `Extractor` instances.
    _link_extractor: LinkExtractor = None
    _header_extractor = None
    _tags_extractor = None
    _text_extractor = None

    _item_extractors: set = None

    _extract_manager = None

    _use_proxy = False
    _default_request_meta = {}

    def __init__(self, *args, **kwargs):
        self.cloud: SHub = None
        # call it to check
        self.extract_manager = self.setup_extract_manager()
        self._item_extractors = self.extract_manager.item_extractors

        super().__init__(*args, **kwargs)

    def connect_cloud(self, cloud: SHub):
        self.cloud = cloud

    # =================
    #  "parse" methods
    # =================
    # there are "callbacks" that scrapes data from page (response)
    def parse(self, response: Response):
        """
        "callback" for "news-list page" that yields requests to "article pages"
        with `parse_article` "callback".
        :param response: `scrapy.http.Response` from "news-list page"
        :return: yields requests to "article pages"
        """
        # parse response and yield requests with `parse_article` "callback"
        urls_iterator = self._yield_urls_from_response(response)
        for url, path in self._get_urls_iterator(urls_iterator):
            meta = self.request_meta
            fingerprint = self._convert_path_to_fingerprint(path)
            meta.update({FINGERPRINT: fingerprint})
            yield Request(url=url,
                          callback=self.parse_article,
                          meta=meta)

    def parse_article(self, response: Response):
        self.logger.info('Started extracting from {}'.format(response.url))
        # produce item
        yield from self._yield_article_item(
            response, **self.extract_manager.extract_all(response))

    def _get_urls_iterator(self, urls_iterator) -> Iterator[Tuple[str, str]]:
        fetcher = SHubFetcher.from_shub_defaults(self.cloud)
        scraped_urls_iterator = (
            (url, urlparse(url)[2]) for url in
            (item[URL] for item in fetcher.fetch_items()))

        def context_processor(value: Tuple[str, str]) -> Context:
            url, path = value
            ctx = Context(value=value, exclude_value=url)
            ctx['path'] = path
            return ctx

        iter_manager = IterManager(
            general_iterator=urls_iterator,
            value_type=tuple,
            return_type=tuple,
            exclude_value_type=str,
            exclude_iterator=scraped_urls_iterator,
            max_exclude_matches=2,
            context_processor=context_processor,
        )
        return iter(iter_manager)

    def _yield_urls_from_response(self, response: Response):
        """
        Parses response from "news-list page" and yields requests to
        "article pages" that aren't scraped yet.
        :param response: `scrapy.http.Response` from "news-list page"
        :return: yield `scrapy.http.Request` instance
        """
        for path_or_url in self._link_extractor.safe_extract_from(response):
            if '://' in path_or_url:
                url = path_or_url
                path = urlparse(url)[2]
            else:
                path = path_or_url
                url = urlunparse([self._scheme, self._start_domain, path,
                                  None, None, None])
            yield url, path

    # ============
    #  properties
    # ============
    # these properties checks if child class has implemented all needed fields
    @property
    def allowed_domains(self):
        return [self._check_field_implementation('_start_domain'), ]

    def start_requests(self):
        url = '{}://{}/{}'.format(
            self._check_field_implementation('_scheme'),
            self._check_field_implementation('_start_domain'),
            self._check_field_implementation('_start_path'))
        request = Request(url, callback=self.parse, meta=self.request_meta)
        yield request

    @property
    def request_meta(self):
        meta = self._default_request_meta.copy()
        return meta

    def setup_extract_manager(self) -> ExtractManager:
        extractors = [
            self._text_extractor,
            self._tags_extractor,
            self._header_extractor,
            self._link_extractor, ]
        if isinstance(self._extract_manager, ExtractManager):
            return self._extract_manager
        elif all(extractors):
            return ExtractManager(
                link_extractor=self._link_extractor,
                header_extractor=self._header_extractor,
                tags_extractor=self._tags_extractor,
                text_extractor=self._text_extractor,
            )
        else:
            raise RuntimeError('nor `_extract_manager` nor `_*_extractor` '
                               'are not defined.')

    # =========
    #  helpers
    # =========

    def _convert_path_to_fingerprint(self, path: str) -> str:
        raise NotImplementedError

    def _check_field_implementation(self, field_name: str):
        """
        Checks if class have implemented field (attribute) with given name
        (string value of `field_name`)
        :param field_name: string that matches class field name
        :raises NotImplementedError: if class doesn't have implemented field
        with `field_name` name
        :return: value if field value isn't `None`, else raises exception
        """
        value = self.__getattribute__(field_name)
        if value is not None:
            return value
        else:
            raise NotImplementedError('Need to define "{}" field.'
                                      .format(field_name))


class TestingSpider(BaseArticleSpider, abc.ABC):
    def parse(self, response: Response):
        yield from self._yield_article_item(
            response, **{
                TAGS: '--',
                TEXT: 'Testing where and how spider exports data.',
                HEADER: '-',
                ERRORS: '----',
                MEDIA: '---',
            }
        )


class WorkerSpider(Spider, abc.ABC):

    _dummy_request_url = 'http://httpbin.org/anything'

    _setup_methods = None

    name: str = None

    def __init__(self, *args, **kwargs):

        for collection in self.setup_methods:
            try:
                method = _get_item(collection, 0)
                if method is None:
                    continue
                args = _get_item(collection, 1)
                if args is None:
                    method(self)
                    continue
                kwargs = _get_item(collection, 2)
                if kwargs is None:
                    method(self, *args)
                    continue
                method(self, *args, **kwargs)
            except Exception as exc:
                raise RuntimeError(
                    f'Error while trying to execute {collection}') from exc

        super().__init__(*args, **kwargs)

    @property
    def setup_methods(self):
        if self._setup_methods is None:
            return ()
        return tuple(
            tuple([collection]) if callable(collection) else tuple(collection)
            for collection in self._setup_methods)

    def start_requests(self):
        """ Make dummy request. """
        yield Request(self._dummy_request_url, callback=self.do_not_parse)

    @abc.abstractmethod
    def do_not_parse(self, _):
        """ Do what you want here. """
        pass
