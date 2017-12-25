import datetime
import logging
import time
from typing import Iterator, Union, Dict, Tuple

from scrapinghub import ScrapinghubClient
from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from ..config import cfg, SCRAPINGHUB_JOBKEY_SEPARATOR


class SHub:
    """
    Uses `scrapinghub.ScrapinghubClient` class to use Scrapy CLoud API.
    It is expected that it is used only for one spider current running spider,
    and uses only it's resources (job history).
    """

    def __init__(self, *, lazy_mode: bool =False, settings: dict or None =None,
                 logger: logging.Logger = None):
        """
        :param lazy_mode: if turned on, lets object to have unset entities. They
        will be set only when needed.
        :param settings: dictionary for `switch` method.
        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger
        self._is_lazy = lazy_mode

        # reset client, project and spider to `unset` value
        self.reset_client(stateless=True)

        if settings:
            self.switch(**settings)
        elif not lazy_mode:
            # call below must start chain of `switch_` calls
            self.switch_client()

    @classmethod
    def shortcut_api_key(cls, api_key: str) -> str:
        return f'{api_key[:4]}\u2026{api_key[-4:]}'

    @property
    def unset(self):
        return None

    @property
    def is_lazy(self) -> bool:
        return self._is_lazy

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
    def client(self) -> ScrapinghubClient:
        client = self._client
        if client is not self.unset:
            return client
        elif not self._is_lazy:
            return self.switch_client()
        else:
            raise ValueError('`client` is not set yet.')

    """
    `default_*` properties returns default key for `get_` method
    """
    @property
    def default_api_key(self) -> str:
        return cfg.api_key

    @property
    def default_project_id(self) -> str:
        return cfg.current_project_id

    @property
    def default_spider_name(self) -> str:
        for spider_dict in self.project.spiders.list():
            spider = self.project.spiders.get(spider_dict['id'])
            project_id, spider_id, job_num = spider.key.split(
                SCRAPINGHUB_JOBKEY_SEPARATOR)
            if spider_id == cfg.current_spider_id:
                return spider.name
        else:
            raise RuntimeError(f'No spider found with `{id}` ID.')

    """
    `_switch_*` methods calls `get_*` method, assigns value and logs it.
    """
    def _switch_spider(self, spider_name: str) -> Spider:
        spider = self.get_spider(spider_name)
        self._spider = spider
        self.logger.info(
            f'Spider switched to {spider_name}.')
        return spider

    def _switch_project(self, project_id: int) -> Project:
        project = self.get_project(project_id)
        self._project = project
        self.logger.info(
            f'Project switched to {project_id}.')
        return project

    def _switch_client(self, api_key: str) -> ScrapinghubClient:
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
            spider_name = self.default_spider_name
        spider = self._switch_spider(spider_name)
        return spider

    def switch_project(self, project_id: int or None =None) -> Project:
        if self.client is self.unset:
            raise ValueError(f'Can not change `project` while '
                             f'`client` is not set (=`{self.unset}`)')
        if project_id is None:
            project_id = self.default_project_id
        project = self._switch_project(project_id)
        self.reset_spider()
        return project

    def switch_client(self, api_key: str or None =None) -> ScrapinghubClient:
        if api_key is None:
            api_key = self.default_api_key
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

    def get_client(self, api_key: str) -> ScrapinghubClient:
        return ScrapinghubClient(str(api_key))


class SHubInterface(SHub):
    def _fetch_week_jobs(self, spider=None):
        """
        Fetches from Scrapy Cloud all current spider's jobs finished on the
        last week.
        :return: generator of `scrapinghub.client.jobs.Job` objects
        """
        if spider is None:
            spider = self.spider
        delta_seconds = datetime.timedelta(weeks=1).total_seconds() + 60 * 60
        week_ago = int((time.time() - delta_seconds) * (10 ** 3))
        for job_summary in spider.jobs.iter(
                startts=week_ago,
                state='finished',
                jobmeta=['key']):
            yield self.project.jobs.get(job_summary['key'])

    def fetch_week_items(self, spider=None):
        """
        Fetches from Scrapy Cloud all items produced by current spider on the
        last week at finished jobs.
        :return: generator of `dict` objects
        """
        for job in self._fetch_week_jobs(spider=spider):
            yield from job.items.iter()

    def fetch_week_fingerprints(self):
        """
        Generates `str` objects that are in the "fingerprint" field of items
        fetched in `_fetch_week_job` method.
        :return: generator of `str` objects
        """
        for item in self.fetch_week_items():
            yield item['fingerprint']

    def fetch_all_week_items(self):
        spiders = self.project.spiders
        for spider_dict in spiders.list():
            spider = spiders.get(spider_dict['id'])
            yield from self.fetch_week_items(spider)


class SHubFetcher:

    def __init__(self, settings: dict):
        """

        :param settings: see type annotations in `process_settings` method
        """
        self.settings = self.process_settings(settings)
        self.shub = SHub(lazy_mode=True)

    @classmethod
    def process_settings(cls, settings: dict, ) -> \
            Dict[str, Dict[int, Dict[str, Dict[str, Tuple[int, frozenset]]]]]:
        processed = dict()
        for api_key, projects in settings.items():
            api_key = str(api_key)
            projects: dict
            processed_projects = dict()

            for project_id, spiders in projects.items():
                project_id = int(project_id)
                spiders: dict
                processed_spiders = dict()

                for spider_name, options in spiders.items():
                    spider_name = str(spider_name)
                    options: Tuple[int, frozenset]

                    processed_spiders[spider_name] = (
                        int(options[0]),
                        frozenset(options[1]),
                    )
                processed_projects[project_id] = processed_spiders
            processed[api_key] = processed_projects
        return processed

    def latest_spiders_jobkeys(self, count: int, exclude: frozenset) -> Iterator[str]:
        STATE = 'state'
        META = 'meta'
        COUNT = 'count'
        FINISHED = 'finished'
        KEY = 'key'
        CLOSE_REASON = 'close_reason'
        ITEMS = 'items'

        spider = self.shub.spider
        for job_summary in spider.jobs.iter(**{
                    STATE: FINISHED,
                    META: [KEY, CLOSE_REASON, ITEMS],
                    COUNT: count,
                }):
            key = job_summary[KEY]
            if job_summary[CLOSE_REASON] != FINISHED:
                self.shub.logger.error(
                    f'job with {key} key finished unsuccessfully.')
            elif job_summary.get(ITEMS, 0) == 0:
                self.shub.logger.info(
                    f'job with {key} key has no items.')
            elif key in exclude:
                continue
            else:
                yield key

    def fetch_jobkeys(self) -> Iterator[dict]:
        for api_key, projects in self.settings.items():
            self.shub.switch_client(api_key)
            for project_id, spiders in projects.items():
                self.shub.switch_project(project_id)
                for spider_name, options in spiders.items():
                    self.shub.switch_spider(spider_name)
                    yield from self.latest_spiders_jobkeys(*options)
