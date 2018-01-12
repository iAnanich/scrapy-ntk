import logging
from typing import Iterator, Iterable, Tuple, Dict, List, Union
from functools import partial

from scrapinghub import ScrapinghubClient as Client
from scrapinghub.client.jobs import Job
from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from .constants import (
    META_KEY, META_ITEMS, META,
    META_CLOSE_REASON_FINISHED, META_CLOSE_REASON,
    META_STATE, META_STATE_FINISHED,
)
from .funcs import spider_id_to_name
from .manager import ScrapinghubManager
from .job import JobKey, JobSummary
from ..utils.counter import Threshold
from ..utils.iter_manager import IterManager, BaseContext

JobNumIter = Iterator[int]
JobKeyIter = Iterator[str]
JobIter = Iterator[Job]
ItemIter = Iterator[dict]
LogIter = Iterator[dict]

SettingsInputType = Dict[
    str,                      # API key
    Dict[
        int,                  # Project ID
        Dict[
            Union[str, int],  # Spider name or ID
            Iterable[int],    # Iterable over excluded job numbers
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
    Tuple[Client, ProjectsTuple]
]


class SHubFetcher:

    def __init__(self, settings: SettingsInputType, *,
                 maximum_fetched_jobs: int or None =None,
                 maximum_excluded_matches: int or None =None,
                 maximum_returned_jobs: int or None =None,
                 maximum_total_excluded: int or None =None,
                 logger: logging.Logger=None):
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

        :param settings: see `SettingsInputType`
        :param maximum_excluded_matches: how many job's numbers (last digit from
         job key) from exclude must be matched to stop iteration
        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger

        try:
            # it will check their values
            Threshold(maximum_fetched_jobs)
            Threshold(maximum_excluded_matches)
        except TypeError as exc:
            msg = f'Wrong `maximum_*` type: {str(exc)}'
            self.logger.exception(msg)
            raise TypeError(msg) from None

        self.maximum_excluded_matches = maximum_excluded_matches
        self.maximum_fetched_jobs = maximum_fetched_jobs
        self.maximum_returned_jobs = maximum_returned_jobs
        self.maximum_total_excluded = maximum_total_excluded

        self.settings = self.process_settings(settings)

    @classmethod
    def from_shub_defaults(cls, shub: ScrapinghubManager):
        # use empty list to get all jobs
        iterable = list()

        settings = {
            shub.defaults.api_key: {
                shub.defaults.project_id: {
                    shub.defaults.spider_id: iterable,
                }
            }
        }
        logger = shub.logger
        new = cls(settings=settings, logger=logger)
        return new

    @classmethod
    def new_helper(cls):
        logger = logging.getLogger('SHubFetcher: ScrapinghubManager helper')
        logger.setLevel(logging.ERROR)
        shub = ScrapinghubManager(lazy_mode=True, logger=logger)
        return shub

    @classmethod
    def process_settings(cls, settings: SettingsInputType) -> ProcessedSettingsType:
        helper = cls.new_helper()
        processed: List[Tuple[Client, ProjectsTuple]] = list()

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

    iter_job_summaries = staticmethod(partial(
        JobSummary.iter_from_spider,
        params={
            META_STATE: META_STATE_FINISHED,
            META      : [META_KEY, META_CLOSE_REASON, META_ITEMS],
        }
    ))

    def latest_spiders_jobkeys(self, spider: Spider,
                               exclude_iterator: JobNumIter) -> JobKeyIter:
        """
        Fetches latest jobs of the given spider, and yields their keys.
        :param spider: `Spider` instance
        :param exclude_iterator: object that yields job's numbers, that you do
        not want to get from this method
        :return: iterator that yields job's numbers
        """

        def context_processor(value: JobSummary, context_type: type) -> BaseContext:
            ctx = context_type(value=value, exclude_value=value.jobkey.job_num)
            return ctx

        def before_finish(ctx: BaseContext):
            self.logger.info(
                f'Finished on {ctx.value.jobkey.job_num} job number '
                f'with close reason: "{ctx.close_reason}".')

        def return_jobkey(ctx: BaseContext) -> JobKey:
            job_summary: JobSummary = ctx.value
            return job_summary.jobkey

        def unsuccessful_job(ctx: BaseContext) -> bool:
            if not ctx.value.was_successful:
                self.logger.error(
                    f'job with {ctx.value.jobkey} key finished unsuccessfully.')
                return True
            else:
                return False

        def empty_job(ctx: BaseContext) -> bool:
            if ctx.value.items < 1:
                self.logger.info(
                    f'job with {ctx.value.jobkey} key has no items.')
                return True
            else:
                return False

        iter_manager = IterManager(
            general_iterator=self.iter_job_summaries(spider),
            value_type=JobSummary,
            return_value_processor=return_jobkey,
            return_type=JobKey,
            exclude_iterator=exclude_iterator,
            exclude_value_type=int,
            exclude_default=0,
            max_iterations=self.maximum_fetched_jobs,
            max_exclude_matches=self.maximum_excluded_matches,
            max_returned_values=self.maximum_returned_jobs,
            max_total_excluded=self.maximum_total_excluded,
            before_finish=before_finish,
            context_processor=context_processor,
            case_processors=(unsuccessful_job, empty_job),
        )

        self.logger.info(f'Ready to fetch jobs for {spider.key} spider.')

        yield from iter_manager

    def latest_spiders_jobs(self, spider: Spider,
                            exclude_iterator: JobNumIter) -> JobIter:
        for jobkey in self.latest_spiders_jobkeys(spider, exclude_iterator):
            yield spider.jobs.get(job_key=str(jobkey))

    def iter_spider_exclude_tuple(self) -> Tuple[Spider, JobNumIter]:
        for client, projects in self.settings:
            for project, spiders in projects:
                yield from spiders

    def fetch_jobs(self) -> JobIter:
        for spider, exclude in self.iter_spider_exclude_tuple():
            yield from self.latest_spiders_jobs(spider, exclude)

    def fetch_jobkeys(self) -> JobKeyIter:
        for spider, exclude in self.iter_spider_exclude_tuple():
            yield from self.latest_spiders_jobkeys(spider, exclude)

    def fetch_items(self) -> ItemIter:
        for job in self.fetch_jobs():
            yield from job.items.iter()

    def fetch_logs(self) -> LogIter:
        for job in self.fetch_jobs():
            yield from job.logs.iter()

    def fetch(self, *, jobkey=False, job=False, items=False, logs=False) -> Iterator[dict]:
        if not any([job, jobkey, items, logs]):
            raise ValueError

        for job_obj in self.fetch_jobs():
            job_obj: Job
            result = dict()
            if jobkey:
                result['jobkey'] = job_obj.key
            if job:
                result['job'] = job_obj
            if items:
                result['items'] = job_obj.items
            if logs:
                result['logs'] = job_obj.logs
            yield result
