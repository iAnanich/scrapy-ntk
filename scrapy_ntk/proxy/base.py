import requests
import random
import logging

from scrapy.http.request import Request


REQUEST_META_KEY_PROXY = 'proxy'
REQUEST_META_KEY_RETRY_TIMES = 'retry_times'


class BaseProxy:

    _default_encoding = 'utf-8'

    def __init__(self):
        self.text = self.load_text()
        self.proxies = self.parse()

    def parse(self, text=None) -> tuple:
        if text is None:
            text = self.text
        return tuple(self._parse(text))

    @classmethod
    def decode(cls, content: bytes, encoding: str=None):
        if encoding is None:
            encoding = cls._default_encoding
        try:
            return content.decode(encoding=encoding)
        except Exception as exc:
            raise RuntimeError('Error while decoding.') from exc

    @classmethod
    def load_text(cls, encoding: str=None) -> str:
        return cls.decode(cls._get_content(), encoding)

    @classmethod
    def _get_content(cls):
        raise NotImplementedError

    @classmethod
    def _parse(cls, text):
        raise NotImplementedError

    def get(self, request: Request, **kwargs) -> str:
        raise NotImplementedError


class BaseRandomProxy(BaseProxy):

    url_to_proxy = {}
    _meta_key_retry_times = REQUEST_META_KEY_RETRY_TIMES

    def random(self, request, no_repeat=False) -> str:
        if not no_repeat or \
                request.meta.get(self._meta_key_retry_times, 0) < 1:
            return random.choice(self.proxies)
        elif no_repeat:
            new_proxy_url = random.choice(self.proxies)
            self.url_to_proxy[request.url] = {new_proxy_url}
            return new_proxy_url
        # case: request has one or more retries and we must use another proxy
        proxies = set(self.proxies)
        request_url = request.url
        used_proxies = self.url_to_proxy[request_url]
        # choosing from non-crossing set
        new_proxy_url = random.choice(proxies - used_proxies)
        # adding new url to set associated with this request url
        self.url_to_proxy[request_url] += new_proxy_url
        return new_proxy_url

    def get(self, request: Request, **kwargs) -> str:
        return self.random(request, **kwargs)
