import logging

from .base import REQUEST_META_KEY_PROXY
from .modes import PROXY_MODES, PROXY_DEFAULT_MODE


class ProxyManager:

    modes = PROXY_MODES
    default_mode = PROXY_DEFAULT_MODE

    _meta_key_proxy = REQUEST_META_KEY_PROXY

    def __init__(self, mode: str=None, logger=None):
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
        if logger is None:
            logger = logging.getLogger(f'ProxyManager<{self.name}>')
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

    @property
    def name(self) -> str:
        return str(self.proxy_mode)

    def __repr__(self):
        return f'<ProxyManager mode: "{self.proxy_mode}">'
