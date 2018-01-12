import logging
from typing import Dict

from scrapinghub import ScrapinghubClient as Client
from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from .utils import shortcut_api_key, spider_id_to_name


class SHub:

    def __init__(self, *, lazy_mode: bool =False,
                 default_conf: dict or None =None,
                 initial_conf: dict or None =None,
                 logger: logging.Logger = None):
        """
        :param lazy_mode: if turned on, lets object to have unset entities. They
        will be set only when needed.
        :param initial_conf: dictionary for `switch` method.
        """
        if logger is None:
            logger = logging.getLogger('ScrapingHub interface')
            logger.setLevel(logging.DEBUG)
        self.logger = logger

        self.defaults = self.Defaults(self, self.logger, default_conf)

        self._is_lazy = lazy_mode

        # reset client, project and spider to `unset` value
        self.reset_client(stateless=True)

        if initial_conf:
            self.switch(**initial_conf)
        elif not lazy_mode:
            # call below must start chain of `switch_` calls
            self.switch_client()

    class Defaults:

        @classmethod
        def key_type_dict(cls) -> Dict[str, type]:
            # TODO: make it possible to pass spider's ID (for example, by using tuples)
            return {
                'api_key': str,
                'project_id': int,
                'spider_id': int,
            }

        @classmethod
        def keys_tuple(cls) -> tuple:
            return tuple(cls.key_type_dict().keys())

        def __init__(self, shub, logger, config: dict or None):
            self.shub = shub
            self.logger = logger

            if config is None:
                self.config = dict()
            else:
                self.config = self.check_conf(config)

        def __getitem__(self, item: str):
            if item in self.keys_tuple():
                try:
                    return self.config[item]
                except KeyError:
                    raise KeyError(
                        f''
                    ) from None
            else:
                raise KeyError(
                    f'{item} defaults key is not supported.'
                ) from None

        def check_conf(self, config: dict) -> dict:
            processed = dict()

            for key, var_type in self.key_type_dict().items():
                try:
                    var = config[key]
                except KeyError:
                    break
                if not isinstance(var, var_type):
                    msg = str(
                        f'Config var with {key} has not valid type.'
                        f'{var_type} expected, got {type(var)}')
                    self.logger.error(msg)
                    raise TypeError(msg)
                processed[key] = var

            return processed

    shortcut_api_key = staticmethod(shortcut_api_key)

    @property
    def unset(self):
        return None

    @property
    def is_lazy(self) -> bool:
        return self._is_lazy

    """
    Entity properties, that returns instances of `scrapinghub` library's
    `Spider`, `Project`, `Client` classes. 
    """
    @property
    def spider(self) -> Spider:
        spider = self._spider
        if spider is not self.unset:
            return spider
        elif not self._is_lazy:
            return self.switch_spider()
        else:
            raise ValueError('`spider` is not set yet.')

    @property
    def project(self) -> Project:
        project = self._project
        if project is not self.unset:
            return project
        elif not self._is_lazy:
            return self.switch_project()
        else:
            raise ValueError('`project` is not set yet.')

    @property
    def client(self) -> Client:
        client = self._client
        if client is not self.unset:
            return client
        elif not self._is_lazy:
            return self.switch_client()
        else:
            raise ValueError('`client` is not set yet.')

    """
    `_switch_*` methods calls `get_*` method, assigns value and logs it.
    """
    def _switch_spider(self, spider_name: str) -> Spider:
        spider = self.get_spider(spider_name)
        self._spider = spider
        self.logger.info(
            f'Spider switched to "{spider_name}" ({spider.key}).')
        return spider

    def _switch_project(self, project_id: int) -> Project:
        project = self.get_project(project_id)
        self._project = project
        self.logger.info(
            f'Project switched to #{project_id}.')
        return project

    def _switch_client(self, api_key: str) -> Client:
        client = self.get_client(api_key)
        self._client = client
        self.logger.info(
            f'Client switched by {self.shortcut_api_key(api_key)} API key.')
        return client

    """
    `switch_*` methods checks if requirement is unset and if so - raises
    ValueError else - checks if given argument is `None` and if soc - uses
    default key, but in each case they calls `_switch_*` method with that key
    """
    def switch_spider(self, spider_name: str or None =None) -> Spider:
        if self.project is self.unset:
            raise ValueError(f'Can not change `spider` while '
                             f'`project` is not set (=`{self.unset}`)')
        if spider_name is None:
            spider_id = self.defaults['spider_id']
            spider_name = spider_id_to_name(spider_id, self.project)
        spider = self._switch_spider(spider_name)
        return spider

    def switch_project(self, project_id: int or None =None) -> Project:
        if self.client is self.unset:
            raise ValueError(f'Can not change `project` while '
                             f'`client` is not set (=`{self.unset}`)')
        if project_id is None:
            project_id = self.defaults['project_id']
        project = self._switch_project(project_id)
        self.reset_spider()
        return project

    def switch_client(self, api_key: str or None =None) -> Client:
        if api_key is None:
            api_key = self.defaults['api_key']
        client = self._switch_client(api_key)
        self.reset_project()
        return client

    def switch(self, **kwargs):
        if 'api_key' in kwargs:
            self.switch_client(kwargs['api_key'])
        if 'project_id' in kwargs:
            self.switch_project(kwargs['project_id'])
        if 'spider_name' in kwargs:
            self.switch_spider(kwargs['spider_name'])

    """
    `reset_*` methods checks `stateless` mode and if so - calls `drop_*` method
    else - calls `switch_` methods with `None` as only argument, which means
    to switch to default value.
    """
    def reset_spider(self, stateless: bool =False):
        if self._is_lazy or stateless:
            self.drop_spider()
        else:
            self.switch_spider(None)

    def reset_project(self, stateless: bool =False):
        if self._is_lazy or stateless:
            self.drop_project()
        else:
            self.switch_project(None)

    def reset_client(self, stateless: bool =False):
        if self._is_lazy or stateless:
            self.drop_client()
        else:
            self.switch_client(None)

    """
    `_drop_*` methods sets entity to `_unset_value` and logs it.
    """
    def _drop_spider(self):
        self._spider = self.unset
        self.logger.info(f'Spider dropped.')

    def _drop_project(self):
        self._project = self.unset
        self.logger.info(f'Project dropped.')

    def _drop_client(self):
        self._client = self.unset
        self.logger.info(f'Client dropped.')

    """
    `drop_*` methods must call `_drop_*` method and reset entities
     that depends on it.
    """
    def drop_spider(self):
        self._drop_spider()

    def drop_project(self):
        self._drop_project()
        self.reset_spider(stateless=True)

    def drop_client(self):
        self._drop_client()
        self.reset_project(stateless=True)

    """
    `get_*` methods must take an identifier of the entity, get it, and return.
    Nothing else, but they are normal methods. 
    """
    def get_spider(self, spider_name: str) -> Spider:
        return self.project.spiders.get(str(spider_name))

    def get_project(self, project_id: int) -> Project:
        return self.client.get_project(int(project_id))

    def get_client(self, api_key: str) -> Client:
        return Client(str(api_key))
