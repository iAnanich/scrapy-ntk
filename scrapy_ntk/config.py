import json
import os
import sys

WEBRANDOMPROXY_URL = 'https://proxy-spider.com/api/proxies.example.txt'
JOBKEY_DEFAULT = '0/0/0'

# variable names
_JOBKEY = 'SHUB_JOBKEY'


def spider_to_root_path_join(from_file: str, target_file: str):
    return os.path.join(
        os.path.dirname(from_file),
        os.path.pardir, os.path.pardir,
        target_file)


class SettingsMaster:
    """ Class for control of given at start arguments, and some environment
    variables. Arguments can be get from spider too.
    **NOTE**: contains only `str` objects."""

    jobkey_env_varname = _JOBKEY

    def __init__(self):
        self._args_dict = None
        self._file_dict = None
        self._shub_jobkey = None

    def configure(self, cmd_args: dict=None,
                  file_args: dict=None,
                  file_path: os.PathLike=None,
                  shub_jobkey: dict=None):
        self._args_dict = cmd_args or self._parse_arguments()
        self._file_dict = file_args or self.parse_file(file_path)
        self._shub_jobkey = shub_jobkey or self._jobkey_handle()

    @classmethod
    def parse_file(cls, path: os.PathLike) -> dict:
        with open(path, 'r') as file:
            dictionary = json.load(file)
        return dictionary

    @classmethod
    def parse_config(cls, spider_file_path: str,
                     config_file_name: str='config.json'):
        return cls.parse_file(spider_to_root_path_join(spider_file_path,
                                                       config_file_name))

    def get_value(self, key: str,
                  args_only: bool =False,
                  json_only: bool =False,
                  required: bool =True,
                  default=None):
        try:
            from_args = self._args_dict[key]
        except KeyError:
            args_exist = False
        else:
            args_exist = True
        try:
            from_json = self._file_dict[key]
        except KeyError:
            json_exist = False
        else:
            json_exist = True
        # value from `args` must overwrite value fro json
        if args_exist and not json_only:
            return from_args
        elif json_exist and not args_only:
            return from_json
        elif not required:
            return default
        else:
            raise RuntimeError('Unable to find expected argument: ' + key)

    def _jobkey_handle(self):
        from_env = os.getenv(self.jobkey_env_varname, None)
        from_args = self._args_dict.get('DEVMODE', None)
        if from_env is not None:
            value = from_env
        elif from_args is not None:
            value = from_args
        else:
            value = JOBKEY_DEFAULT
        tupl = value.split('/')
        return {
            'CURRENT_PROJECT_ID': tupl[0],
            'CURRENT_SPIDER_ID': tupl[1],
            'CURRENT_JOB_ID': tupl[2],
        }

    def _parse_arguments(self) -> dict:
        arguments = sys.argv
        dictionary = {}
        for i in range(len(arguments)):
            if arguments[i] == '-a':
                args = arguments[i+1].split('=')
                dictionary[args[0]] = args[1]
        return dictionary

    # ============
    #  properties
    # ============
    # ScrapingHub
    @property
    def current_project_id(self) -> str:
        return self._shub_jobkey['CURRENT_PROJECT_ID']

    @property
    def current_spider_id(self) -> str:
        return self._shub_jobkey['CURRENT_SPIDER_ID']

    @property
    def current_job_id(self) -> str:
        return self._shub_jobkey['CURRENT_JOB_ID']

    @property
    def api_key(self) -> str:
        return self.get_value('SCRAPY_CLOUD_API_KEY')

    # complex
    @property
    def spider_to_worksheet_dict(self) -> str:
        return self.get_value(
            'SPIDERS',
            json_only=True, )

    # strings
    @property
    def spreadsheet_title(self) -> str:
        return self.get_value(
            'SPREADSHEET_TITLE',
            json_only=True, )

    @property
    def gspread_prefixfmt(self) -> str:
        return self.get_value(
            'GSPREAD_PREFIXFMT',
            default='{date} / START "{name}" spider',
            required=False, )

    @property
    def gspread_suffixfmt(self) -> str:
        return self.get_value(
            'GSPREAD_SUFFIXFMT',
            default='{date} / {count} articles scraped',
            required=False, )

    @property
    def gspread_datefmt(self) -> str:
        return self.get_value(
            'GSPREAD_DATEFMT',
            default='%d.%m %a %H:%M',
            required=False, )

    @property
    def gspread_enable_prefix(self) -> str:
        return self.get_value(
            'gspread_enable_prefix',
            default='True',
            required=False, )

    @property
    def gspread_enable_suffix(self) -> str:
        return self.get_value(
            'gspread_enable_suffix',
            default='True',
            required=False, )

    @property
    def proxy_mode(self) -> str:
        return self.get_value(
            'PROXY_MODE',
            default='random_web',
            required=False,
        )

    @property
    def item_datefmt(self) -> str:
        return self.get_value(
            'ITEM_DATEFMT',
            default='%d.%m %H:%M',
            required=False,
        )

    # bool
    @property
    def enable_gspread(self) -> str:
        return self.get_value(
            'ENABLE_GSPREAD',
            default='True',
            required=False,
            args_only=True, )

    @property
    def enable_shub(self) -> str:
        return self.get_value(
            'ENABLE_SHUB',
            default='True',
            required=False,
            args_only=True, )

    @property
    def enable_proxy(self):
        return self.get_value(
            'ENABLE_PROXY',
            default='False',
            required=False,
            args_only=True, )


cfg = SettingsMaster()
