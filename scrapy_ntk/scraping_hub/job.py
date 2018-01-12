import re
import typing

from .constants import JOBKEY_SEPARATOR, JOBKEY_PATTERN
from ..utils.check import check_obj_type


class JobKey:

    AsTupleType = typing.Tuple[int, int, int]
    AsDictType = typing.Dict[str, int]

    separator = JOBKEY_SEPARATOR
    pattern = JOBKEY_PATTERN
    keys = ('project_id', 'spider_id', 'job_num')

    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], str):
            string = args[0]
            project_id, spider_id, job_num = self.parse(string)
        elif len(args) == 3 and all(isinstance(arg, int) for arg in args):
            project_id, spider_id, job_num = args
            string = self.concatenate(project_id, spider_id, job_num)
        else:
            raise ValueError
        self._string = string
        self._project_id = project_id
        self._spider_id = spider_id
        self._job_num = job_num

    @classmethod
    def concatenate(cls, project_id: int, spider_id: int, job_num: int) -> str:
        return cls.separator.join(int(i) for i in (project_id, spider_id, job_num))

    @classmethod
    def parse(self, string: str) -> AsTupleType:
        if re.fullmatch(self.pattern, string):
            # we know that there are only 3 elements because of pattern match
            elements: self.AsTupleType = tuple(
                int(s) for s in string.split(JOBKEY_SEPARATOR))
            for i, item, name in zip(range(3), elements, self.keys):
                check_obj_type(item, int, f'Item #{i} (for "{name}")')
                assert item > 0
            return elements
        else:
            raise ValueError

    @classmethod
    def from_string(cls, string: str) -> 'JobKey':
        return JobKey(cls.parse(string))

    @classmethod
    def from_tuple(cls, tupl: AsTupleType) -> 'JobKey':
        return JobKey(*tupl)

    @classmethod
    def from_dict(cls, dictionary: AsDictType) -> 'JobKey':
        return JobKey(dictionary[k] for k in cls.keys)

    def as_tuple(self) -> AsTupleType:
        return self._project_id, self._spider_id, self._job_num

    def as_dict(self) -> AsDictType:
        return {k: v for k, v in zip(self.keys, self.as_tuple())}

    def as_string(self) -> str:
        return self._string

    @property
    def project_id(self) -> int:
        return self._project_id

    @property
    def spider_id(self) -> int:
        return self._spider_id

    @property
    def job_num(self) -> int:
        return self._job_num

    def __iter__(self) -> typing.Iterator[int]:
        yield from self.as_tuple()

    def __repr__(self):
        return f'<JobKey {self.as_string()}>'

    def __str__(self):
        return self.as_string()
