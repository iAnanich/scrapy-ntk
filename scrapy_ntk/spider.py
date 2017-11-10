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

import logging
import abc

from datetime import datetime
from urllib.parse import urlparse, urlunparse

from scrapy import Spider
from scrapy.http import Response, Request

from .cloud import SHubInterface
from .config import cfg
from .extractor import ExtractManager
from .item import (
    ArticleItem,
    URL, FINGERPRINT, TAGS, TEXT, HEADER, DATE, MEDIA, ERRORS
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOCAL_EMPTY_FINGERPRINT = 'LocalEmptyFingerprint'


def _to_bool(string: str) -> bool:
    if string in ['True', '1']:
        return True
    elif string in ['False', '0']:
        return False
    else:
        raise ValueError('Unknown string value: ' + string)


def _get_item(lst: list, fingerprint: int, default=None):
    try:
        return lst[fingerprint]
    except IndexError:
        return default


class FingerprintsContainer(frozenset):

    def __repr__(self):
        return '<Fingerprints: {}>'.format(self)

    def __str__(self):
        return ', '.join(self)


class BaseSpider(abc.ABC, Spider):

    _enable_proxy = False
    _proxy_mode = None

    _article_item_class = ArticleItem
    _default_fingerprint = LOCAL_EMPTY_FINGERPRINT

    name: str = None

    def __init__(self, *args, **kwargs):
        # check proxy
        if self._enable_proxy or _to_bool(cfg.enable_proxy):
            self._enable_proxy = True
            self._proxy_mode = self._proxy_mode or cfg.proxy_mode
            logger.info('Spider set `_enable_proxy=True`.')
            logger.info('Spider set `_proxy_mode={}`.'
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
            fingerprint = self._default_fingerprint
        kwargs.update({
            URL: response.url,
            FINGERPRINT: fingerprint,
            DATE: datetime.now()
        })
        yield self._article_item_class(**kwargs)

    @property
    def enable_proxy(self):
        return self._enable_proxy

    @property
    def proxy_mode(self):
        return self._proxy_mode


class SingleSpider(BaseSpider, abc.ABC):

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
    _link_extractor = None
    _header_extractor = None
    _tags_extractor = None
    _text_extractor = None

    _item_extractors = set()

    _extract_manager = None

    _use_proxy = False
    _default_request_meta = {}

    def __init__(self, *args, **kwargs):
        self.cloud = None
        self._scraped_fingerprints = frozenset()
        # call it to check
        self.extract_manager = self.setup_extract_manager()
        self._item_extractors = self.extract_manager.item_extractors

        super().__init__(*args, **kwargs)

    def connect_cloud(self, cloud: SHubInterface):
        self.cloud = cloud
        # use `frozenset` here because we will iterate over
        # `self._scraped_fingerprints` many times and iterating right now might
        # reduce the traffic
        self._scraped_fingerprints = FingerprintsContainer(
            cloud.fetch_week_fingerprints())
        # log it
        log_msg = 'Scraped fingerprints:'
        for i in self._scraped_fingerprints:
            log_msg += f'\n\t{i}'
        logger.info(log_msg)

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
        yield from self._yield_requests_from_response(response)

    def parse_article(self, response: Response):
        logger.info('Started extracting from {}'.format(response.url))
        # produce item
        yield from self._yield_article_item(
            response, **self.extract_manager.extract_all(response))

    # ============
    #  generators
    # ============
    # these methods are used to yield requests of items
    def _yield_request(self, path_or_url: str):
        if '://' in path_or_url:
            url = path_or_url
            path = urlparse(url)[2]
        else:
            path = path_or_url
            url = urlunparse([self._scheme, self._start_domain, path,
                              None, None, None])
        fingerprint = self._convert_path_to_fingerprint(path)
        if fingerprint not in self._scraped_fingerprints:
            meta = self.request_meta
            meta.update({FINGERPRINT: fingerprint})
            yield Request(url=url,
                          callback=self.parse_article,
                          meta=meta)

    def _yield_requests_from_response(self, response: Response):
        """
        Parses response from "news-list page" and yields requests to
        "article pages" that aren't scraped yet.
        :param response: `scrapy.http.Response` from "news-list page"
        :return: yield `scrapy.http.Request` instance
        """
        for link in self._link_extractor.safe_extract_from(response):
            yield from self._yield_request(link)

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


class TestingSpider(BaseSpider, abc.ABC):
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
