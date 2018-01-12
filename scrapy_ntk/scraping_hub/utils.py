from typing import Iterator, Tuple

from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider

from .constants import (
    STATE, STATE_FINISHED, META, KEY, CLOSE_REASON, ITEMS, JOBKEY_SEPARATOR,
)

# TODO: rename to `funcs`

__all__ = (
    'shortcut_api_key', 'iter_job_summaries', 'split_jobkey',
    'spider_name_to_id', 'spider_id_to_name',
)


def shortcut_api_key(api_key: str, margin: int =4) -> str:
    """
    Hides most of the API key for security reasons.
    :param api_key: string representing API key.
    :param margin: number of characters of the given `api_key` string to show on
    the start and the end.
    :return: shortcut API key
    """
    middle = '\u2026'
    return f'{api_key[:margin]}{middle}{api_key[-margin:]}'


def iter_job_summaries(spider: Spider) -> Iterator[dict]:
    yield from spider.jobs.iter(**{
        STATE: STATE_FINISHED,
        META: [KEY, CLOSE_REASON, ITEMS],
    })


def spider_name_to_id(spider_name: str, project: Project) -> int:
    spider: Spider = project.spiders.get(spider_name)
    project_id, spider_id = spider.key.split(JOBKEY_SEPARATOR)
    return spider_id


def spider_id_to_name(spider_id: int, project: Project) -> str:
    for spider_dict in project.spiders.list():
        name = spider_dict['id']
        spider: Spider = project.spiders.get(name)
        project_id, spider_id_str = spider.key.split(JOBKEY_SEPARATOR)
        if spider_id == int(spider_id_str):
            return name
    else:
        raise ValueError(f'No such spider with {spider_id} ID found')


def split_jobkey(jobkey: str) -> Tuple[int, int, int]:
    lst = jobkey.split(JOBKEY_SEPARATOR)
    project_id, spider_id, job_num = [int(s) for s in lst]
    return project_id, spider_id, job_num
