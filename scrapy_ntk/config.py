import json
import os
import sys

WEBRANDOMPROXY_URL = 'https://proxy-spider.com/api/proxies.example.txt'
JOBKEY_DEFAULT = '0/0/0'

# config json files
OPTIONS_FILENAME = 'config.json'

# variable names
_JOBKEY = 'SHUB_JOBKEY'


class SettingsMaster:
    """ Class for control of given at start arguments, and some environment
    variables. Arguments can be get from spider too.
    **NOTE**: contains only `str` objects."""

    jobkey_env_varname = _JOBKEY
    options_filename = OPTIONS_FILENAME
    _file_level = 3

    def __init__(self):
        self._args_dict = self._parse_arguments()
        self._file_dict = self._parse_file()
        self._shub_jobkey = self._jobkey_handle()

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

    def _parse_file(self) -> dict:
        return json.load(open(self.options_filename))

    @staticmethod
    def path_to_config_file(file_name: str,
                            file_level: int =_file_level) -> str:
        path = __file__
        for _ in range(file_level):
            # wrap with parent directory
            path = os.path.abspath(os.path.join(path, os.pardir))
        return os.path.join(path, file_name)

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
