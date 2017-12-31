import datetime
import logging
import time
from typing import Iterator, Iterable, Tuple, Dict, List, Union, NewType

from scrapinghub import ScrapinghubClient
from scrapinghub.client.jobs import Job
from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from ..config import cfg, SCRAPINGHUB_JOBKEY_SEPARATOR

STATE = 'state'
META = 'meta'
FINISHED = 'finished'
KEY = 'key'
CLOSE_REASON = 'close_reason'
ITEMS = 'items'


def iter_job_summaries(spider: Spider):
    yield from spider.jobs.iter(**{
        STATE: FINISHED,
        META: [KEY, CLOSE_REASON, ITEMS],
    })


def spider_name_to_id(spider_name: str, project: Project) -> int:
    spider: Spider = project.spiders.get(spider_name)
    project_id, spider_id = spider.key.split(SCRAPINGHUB_JOBKEY_SEPARATOR)
    return spider_id


def spider_id_to_name(spider_id: int, project: Project) -> str:
    for spider_dict in project.spiders.list():
        name = spider_dict['id']
        spider: Spider = project.spiders.get(name)
        if spider_id == spider.key.split(SCRAPINGHUB_JOBKEY_SEPARATOR)[1]:
            return name
    else:
        raise ValueError(f'No such spider with {spider_id} ID found')


def split_jobkey(jobkey: str) -> Tuple[int, int, int]:
    lst = jobkey.split(SCRAPINGHUB_JOBKEY_SEPARATOR)
    project_id, spider_id, job_num = [int(s) for s in lst]
    return project_id, spider_id, job_num


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
            spider_name = spider_dict['id']
            spider_id = spider_name_to_id(spider_name, self.project)
            if spider_id == cfg.current_spider_id:
                return spider_name
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


JobNumIter = Iterator[int]
JobKeyIter = Iterator[str]

SettingsInputType = Dict[
    str,                    # API key
    Dict[
        int,                # Project ID
        Dict[
            Union[str, int],            # Spider name or ID
            Iterable[int]   # Iterable over excluded job numbers
        ]
    ]
]
SpidersTuple = Tuple[
    Tuple[Spider, JobNumIter]
]
ProjectsTuple = Tuple[
    Tuple[Project, SpidersTuple]
]
ProcessedSettingsType = ClientsTuple = Tuple[
    Tuple[ScrapinghubClient, ProjectsTuple]
]


class SHubFetcher:

    def __init__(self, settings: SettingsInputType, *,
                 maximum_excluded_matches: int =2, logger: logging.Logger=None):
        """
        For example you have `1234567887654321123567887654321` API key, `274629`
        project ID and `spider001` spider with `1` ID:
            >>> f = SHubFetcher(
            ...     settings={
            ...         'your_32_char_API_key': {
            ...             274629: {
            ...                 'spider_number_1': (x for x in [305, 301, 300]),
            ...             }
            ...         }
            ...     },
            ...     maximum_excluded_matches=2, )
            >>> f.fetch_jobs()

        :param settings: see example above
        :param maximum_excluded_matches: how many jobnums (last digit from job
        key) from exclude must be matched to stop iteration
        """
        self.settings = self.process_settings(settings)
        self.maximum_excluded_matches = maximum_excluded_matches

        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger

    @classmethod
    def new_helper(cls):
        logger = logging.getLogger('SHubFetcher: SHub helper')
        logger.setLevel(logging.ERROR)
        shub = SHub(lazy_mode=True, logger=logger)
        return shub

    @classmethod
    def process_settings(cls, settings: SettingsInputType) -> ProcessedSettingsType:
        helper = cls.new_helper()
        processed: List[Tuple[ScrapinghubClient, ProjectsTuple]] = list()

        for api_key, projects in settings.items():
            if not isinstance(api_key, str):
                raise TypeError(
                    f'API key must a string, got {type(api_key)} instead.')
            helper.switch_client(api_key)
            processed_projects: List[Tuple[Project, SpidersTuple]] = list()

            for project_id, spiders in projects.items():
                if not isinstance(project_id, int):
                    raise TypeError(
                        f'project ID must an integer, '
                        f'got {type(project_id)} instead.')
                helper.switch_project(project_id)
                processed_spiders: List[Tuple[Spider, Iterator[int]]] = list()

                for spider_name_or_id, exclude_iterable in spiders.items():
                    if isinstance(spider_name_or_id, str):
                        spider_name = spider_name_or_id
                    elif isinstance(spider_name_or_id, int):
                        spider_name = spider_id_to_name(
                            spider_name_or_id, helper.project)
                    else:
                        raise TypeError(
                            f'Spider name or ID must a string or an integer, '
                            f'got {type(spider_name_or_id)} instead.')
                    # process spider name or ID
                    helper.switch_spider(spider_name)
                    # process exclude
                    exclude_list = [int(i) for i in exclude_iterable]  # type-check
                    exclude_list.sort(reverse=True)  # sort, to get bigger numbers first
                    exclude_iterator = iter(exclude_list)

                    processed_spiders.append((helper.spider, exclude_iterator, ))

                processed_spiders: SpidersTuple = tuple(processed_spiders)
                processed_projects.append((helper.project, processed_spiders, ))

            processed_projects: ProjectsTuple = tuple(processed_projects)
            processed.append((helper.client, processed_projects, ))

        processed: ClientsTuple = tuple(processed)
        return processed

    def latest_spiders_jobkeys(self, spider: Spider,
                               exclude_iterator: JobNumIter) -> JobKeyIter:
        completed = False
        excluded_counter = 0

        def in_excluded(job_num: int) -> True:
            nonlocal completed
            if not completed:
                try:
                    return job_num == next(exclude_iterator)
                except StopIteration:
                    completed = True
            return False

        for job_summary in iter_job_summaries(spider):
            key = job_summary[KEY]
            job_num = int(split_jobkey(key)[-1])

            if job_summary[CLOSE_REASON] != FINISHED:
                self.logger.error(
                    f'job with {key} key finished unsuccessfully.')
            elif job_summary.get(ITEMS, 0) == 0:
                self.logger.info(
                    f'job with {key} key has no items.')
            elif in_excluded(job_num):
                excluded_counter += 1
                if excluded_counter == self.maximum_excluded_matches:
                    self.logger.info(f'Stopped on {job_num}th job.')
                    break
            else:
                excluded_counter = 0
                yield key

    def iter_spider_exclude_tuple(self) -> Tuple[Spider, JobNumIter]:
        for client, projects in self.settings:
            for project, spiders in projects:
                yield from spiders

    def fetch_jobkeys(self) -> JobKeyIter:
        for spider, exclude in self.iter_spider_exclude_tuple():
            yield from self.latest_spiders_jobkeys(spider, exclude)

