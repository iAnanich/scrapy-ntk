import requests

from .base import BaseRandomProxy


WEBRANDOMPROXY_URL = 'https://proxy-spider.com/api/proxies.example.txt'
RANDOM_WEB_PROXY_MODE = 'random_web'


class RandomWebProxy(BaseRandomProxy):

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