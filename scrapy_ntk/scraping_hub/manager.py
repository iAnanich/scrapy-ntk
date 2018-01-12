import logging
from typing import Dict, Tuple
from functools import singledispatch, partial

from scrapinghub import ScrapinghubClient as Client
from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from .utils import shortcut_api_key, spider_id_to_name
from ..utils.check import check_obj_type, raise_or_none

_logger = logging.getLogger('ScrapingHub interface')
_logger.setLevel(logging.DEBUG)


class ManagerDefaults:

    API_KEY = 'api_key'
    PROJECT_ID = 'project_id'
    SPIDER_ID = 'spider_id'
    SPIDER_NAME = 'spider_name'

    @classmethod
    def key_type_dict(cls) -> Tuple[Dict[str, type], ...]:
        return (
            {
                cls.API_KEY: str,
            },
            {
                cls.PROJECT_ID: int,
            },
            {
                cls.SPIDER_ID: int,
                cls.SPIDER_NAME: str,
            },
        )

    @classmethod
    def keys_tuple(cls) -> tuple:
        keys = []
        for d in cls.key_type_dict():
            keys += list(d.keys())
        return tuple(keys)

    def __init__(self, config: dict =..., logger: logging.Logger =None):
        if logger is None:
            logger = _logger
        self.logger = logger

        if config is Ellipsis:
            self._config = dict()
        else:
            check_obj_type(config, dict, 'Configuration dictionary')
            self._config = self.check_conf(config)

    def __getitem__(self, item: str):
        if item in self.keys_tuple():
            try:
                return self._config[item]
            except KeyError:
                raise KeyError(
                    f'Given {item} key not found in defaults.'
                ) from None
        else:
            raise KeyError(
                f'{item} defaults key is not supported.'
            ) from None

    def check_conf(self, config: dict) -> dict:
        processed = dict()

        for type_dict in self.key_type_dict():
            raise_: dict = None
            break_ = True
            for key, expected_type in type_dict.items():
                try:
                    value = config[key]
                    break_ = False
                except KeyError:
                    break_ = True
                else:
                    if not isinstance(value, expected_type):
                        raise_ = {
                            'key': key,
                            'value': value,
                            'value_type': type(value),
                            'expected_type': expected_type,
                        }
                        break
                    processed[key] = value

            if raise_:
                msg = str(
                    f'Config var with {raise_["key"]} has not valid type.'
                    f'{raise_["expected_type"]} expected, got {raise_["value_type"]}')
                self.logger.error(msg)
                raise TypeError(msg)
            if break_:
                break

        return processed

    @raise_or_none(KeyError)
    def client(self, api_key: bool =True) -> str:
        if api_key:
            return self[self.API_KEY]
        else:
            raise ValueError

    @raise_or_none(KeyError)
    def project(self, id_: bool =True) -> int:
        if id_:
            return self[self.PROJECT_ID]
        else:
            raise ValueError

    @raise_or_none(KeyError)
    def spider(self, *, id_: bool =True, name: bool =False) -> int or str:
        if id_ and name:
            raise ValueError(
                f'Only spider\'s name or ID can be returned.'
            )
        elif id_:
            return self[self.SPIDER_ID]
        elif name:
            return self[self.SPIDER_NAME]
        else:
            raise ValueError(
                f'`id_` or `name` key-word arguments must be `True`.'
            )

    api_key = property(partial(client, api_key=True, raise_=False))
    project_id = property(partial(project, id_=True, raise_=False))
    spider_id = property(partial(spider, id_=True, name=False, raise_=False))
    spider_name = property(partial(spider, id_=False, name=True, raise_=False))


class SHub:

    shortcut_api_key = staticmethod(shortcut_api_key)

    def __init__(self, *, lazy_mode: bool =False,
                 defaults: ManagerDefaults or None =None,
                 default_conf: dict or None =None,
                 initial_conf: dict or None =None,
                 logger: logging.Logger = None):
        """
        :param lazy_mode: if turned on, lets object to have unset entities. They
        will be set only when needed.
        :param initial_conf: dictionary for `switch` method.
        """
        if logger is None:
            logger = _logger
        self.logger = logger

        if defaults is None and default_conf is not None:
            defaults = ManagerDefaults(default_conf, logger=self.logger)
        self.defaults = defaults

        self._is_lazy = lazy_mode

        # reset client, project and spider to `unset` value
        self.reset_client(stateless=True)

        if initial_conf:
            self.switch(**initial_conf)
        elif not lazy_mode:
            # call below must start chain of `switch_` calls
            self.switch_client()

    @property
    def unset(self):
        return None

    @property
    def is_lazy(self) -> bool:
        return self._is_lazy

    def set_defaults(self, defaults: ManagerDefaults):
        check_obj_type(defaults, ManagerDefaults, 'Manager defaults')
        self.defaults = defaults

    def __repr__(self):
        return f''

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
            spider_name = self.defaults.spider_name
            if spider_name is None:
                spider_id = self.defaults.spider_id
                if spider_id is None:
                    msg = str(
                        f'Trying to switch to default spider, '
                        f'but no spider-related data found in defaults.'
                    )
                    self.logger.error(msg)
                    raise RuntimeError(msg)
                spider_name = spider_id_to_name(spider_id, self.project)
        spider = self._switch_spider(spider_name)
        return spider

    def switch_project(self, project_id: int or None =None) -> Project:
        if self.client is self.unset:
            raise ValueError(f'Can not change `project` while '
                             f'`client` is not set (=`{self.unset}`)')
        if project_id is None:
            project_id = self.defaults.project_id
        project = self._switch_project(project_id)
        self.reset_spider()
        return project

    def switch_client(self, api_key: str or None =None) -> Client:
        if api_key is None:
            api_key = self.defaults.api_key
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
