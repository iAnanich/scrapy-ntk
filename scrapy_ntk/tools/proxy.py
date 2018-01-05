import requests
import random
import logging

from scrapy.http.request import Request

WEBRANDOMPROXY_URL = 'https://proxy-spider.com/api/proxies.example.txt'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


_REQUEST_META_KEY_PROXY = 'proxy'
_REQUEST_META_KEY_RETRY_TIMES = 'retry_times'


class _Proxy:

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


class _RandomProxy(_Proxy):

    url_to_proxy = {}
    _meta_key_retry_times = _REQUEST_META_KEY_RETRY_TIMES

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


class RandomWebProxy(_RandomProxy):

    list_of_proxies_url = WEBRANDOMPROXY_URL

    @classmethod
    def _get_content(cls):
        response = requests.get(cls.list_of_proxies_url)
        if response.status_code != 200:
            raise RuntimeError('Can not retrieve list of proxies')
        return response.content

    @classmethod
    def _parse(cls, text):
        yield from text.split('\n')


# =========
#   modes
# =========
RANDOM_WEB_PROXY_MODE = 'random_web'
PROXY_MODES = {
    RANDOM_WEB_PROXY_MODE: RandomWebProxy,
}
DEFAULT_MODE = RANDOM_WEB_PROXY_MODE


class ProxyManager:

    modes = PROXY_MODES
    default_mode = DEFAULT_MODE

    _meta_key_proxy = _REQUEST_META_KEY_PROXY

    def __init__(self, mode: str=None):
        if mode is None:
            mode = self.default_mode
        # retrieve Proxy class
        try:
            self.proxy_class = self.modes[mode]
        except KeyError:
            raise RuntimeError('Unknown mode.')
        self.proxy_mode = mode
        try:
            self.proxy = self.proxy_class()
        except Exception as exc:
            logger.error('Error while setting up {}.'
                         .format(self.proxy_class.__name__))
            raise RuntimeError('Cannot set up ProxyManager') from exc
        self._logger = logger
        self._logger.info('Proxy manager initialized.')

    @classmethod
    def new_proxy_instance(cls, *args, **kwargs):
        return cls(*args, **kwargs).proxy

    def new_proxy_url(self, request):
        new_proxy_url = self.proxy.get(request)
        self._log_new_proxy(new_proxy_url, request.url)
        return new_proxy_url

    def process(self, request):
        new_url = self.new_proxy_url(request)
        request.meta[self._meta_key_proxy] = new_url

    def _log_new_proxy(self, proxy_url, request_url):
        self._logger.debug('Proxy URL "{}" used for "{}" request URL.'
                           .format(proxy_url, request_url))
