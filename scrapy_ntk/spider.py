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
import warnings
from typing import Iterator, Tuple
from urllib.parse import urlparse, urlunparse

from scrapy import Spider, Request
from scrapy.selector import Selector
from scrapy.http import HtmlResponse

from .base import BaseArticleSpider
from .item import (
    TAGS, TEXT, HEADER, MEDIA, ERRORS, URL,
)
from .parsing import ExtractManager, LinkExplorer
from .scraping_hub.manager import ScrapinghubManager
from .scraping_hub.fetcher import SHubFetcher


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
    # Must be a string. Minimum value: ''
    _start_path: str = None

    # URL host of the web-site. Used for `allowed_domains` field.
    # Must be a string. Example: 'www.example.com'
    _start_domain: str = None

    # URL scheme. Allowed values: 'http', 'https'
    _scheme: str = None

    # Extractors used to extract needed data from HTML
    # Must be `Extractor` instances.
    _header_extractor = None
    _tags_extractor = None
    _text_extractor = None

    _extract_manager: ExtractManager = None

    _link_explorer: LinkExplorer

    _max_exclude_strike: int or None = None

    def __init__(self, *args, **kwargs):
        self.cloud: ScrapinghubManager = None
        # call it to check
        # TODO: move to another class
        self.extract_manager = self.setup_extract_manager()

        super().__init__(*args, **kwargs)

    def connect_cloud(self, cloud: ScrapinghubManager):
        self.cloud = cloud
        self.logger.info(f'{type(cloud)} connected.')

    # =================
    #  "parse" methods
    # =================
    # there are "callbacks" that scrapes data from page (response)
    def parse(self, response: HtmlResponse):
        """
        "callback" for "news-list page" that yields requests to "article pages"
        with `parse_article` "callback".
        :param response: `scrapy.http.Response` from "news-list page"
        :return: yields requests to "article pages"
        """
        # parse news root page
        url_path_iterator = self._yield_urls_from_selector(response.selector)

        for url, path in self._handle_url_path_iterator(url_path_iterator):
            fingerprint = self._convert_path_to_fingerprint(path)
            meta = self.request_meta
            meta.update({self._meta_fingerprint_key: fingerprint})
            yield self.new_request(
                url=url,
                callback=self.parse_article,
                meta=meta, )

    def parse_article(self, response: HtmlResponse):
        self.logger.info('Started extracting from {}'.format(response.url))
        # produce item
        extracted_fields = self.extract_manager.extract_all(response.selector)
        yield self.new_article_item(response, **extracted_fields)

    def _handle_url_path_iterator(self, urls_iterator) -> Iterator[Tuple[str, str]]:
        if self.cloud is None:
            # pass all incoming URLs
            yield from urls_iterator

        fetcher = SHubFetcher.from_shub_defaults(self.cloud)
        already_scraped_urls = frozenset(item[URL] for item in fetcher.fetch_items())
        for url, path in urls_iterator:
            if url in already_scraped_urls:
                self.logger.debug(
                    f'Skipping article with following URL path because it has '
                    f'been scraped in the past and can be found on ScrapingHub: '
                    f'{path}')
            else:
                yield url, path

    def _yield_urls_from_selector(self, selector: Selector):
        """
        Parses response from "news-list page" and yields requests to
        "article pages" that aren't scraped yet.
        :param selector: selector from "news-list page"
        :return: yield `scrapy.http.Request` instance
        """
        for path_or_url in self.link_explorer.yield_links(selector):
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

    @property
    def link_explorer(self):
        if hasattr(self, '_link_explorer'):
            pass
        elif hasattr(self, '_link_extractor'):
            warnings.warn(
                'Assign `LinkExplorer` instance to `_link_explorer` attribute.')
            self._link_explorer = self._link_extractor
        else:
            raise AttributeError('Missing attribute with `LinkExplorer` instance.')
        return self._link_explorer

    def start_requests(self):
        news_root_url = self.news_root_url
        self.logger.info(f'News root URL: "{news_root_url}".')
        yield self.new_request(
            url=news_root_url, callback=self.parse, meta=self.request_meta)

    @property
    def news_root_url(self):
        return '{scheme}://{domain}/{path}'.format(
            scheme=self._check_field_implementation('_scheme'),
            domain=self._check_field_implementation('_start_domain'),
            path=self._check_field_implementation('_start_path'))

    def setup_extract_manager(self) -> ExtractManager:
        extractors = [
            self._text_extractor,
            self._tags_extractor,
            self._header_extractor,]
        if isinstance(self._extract_manager, ExtractManager):
            return self._extract_manager
        elif all(extractors):
            return ExtractManager(
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

    # TODO: rename to alias
    # TODO: handle query too
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

    def parse(self, response: HtmlResponse):
        yield self.new_article_item(
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
