from typing import Iterator, Tuple

from scrapinghub.client.projects import Project
from scrapinghub.client.spiders import Spider
from scrapinghub.client.exceptions import NotFound

from .constants import (
    META_STATE, META_STATE_FINISHED, META, META_KEY, META_CLOSE_REASON, META_ITEMS, JOBKEY_SEPARATOR,
)

__all__ = (
    'shortcut_api_key',
    'spider_name_to_id', 'spider_id_to_name',
    'spider_from_id', 'spider_from_name',
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


def spider_name_to_id(spider_name: str, project: Project) -> int:
    spider: Spider = project.spiders.get(spider_name)
    project_id_str, spider_id_str = spider.key.split(JOBKEY_SEPARATOR)
    return int(spider_id_str)


def spider_id_to_name(spider_id: int, project: Project) -> str:
    for spider_dict in project.spiders.list():
        name = spider_dict['id']
        spider: Spider = project.spiders.get(name)
        project_id_str, spider_id_str = spider.key.split(JOBKEY_SEPARATOR)
        if spider_id == int(spider_id_str):
            return name
    else:
        raise NotFound(f'No such spider with {spider_id} ID found')


def spider_from_name(spider_name: str, project: Project) -> Spider:
    return project.spiders.get(spider_name)


def spider_from_id(spider_id: int, project: Project) -> Spider:
    return project.spiders.get(spider_id_to_name(spider_id, project))
