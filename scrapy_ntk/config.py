import json
import os
import sys

from .item import (
    FIELDS,
    URL, FINGERPRINT, TEXT, TAGS, DATE, HEADER, MEDIA, ERRORS
)
from .tools.cloud import SCRAPINGHUB_JOBKEY_SEPARATOR
from .tools.proxy import WEBRANDOMPROXY_URL

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

    gspread_oauth2_credentials_file_name = 'client-secret.json'
    config_json_file_name = 'config.json'

    def __init__(self):
        self._project_root_path = None
        self._args_dict = None
        self._file_dict = None
        self._shub_jobkey = None

        self._is_configured = False

    def configure(self, project_root_path: os.PathLike,
                  cmd_args: dict=None,
                  file_args: dict=None,
                  shub_jobkey: dict=None):
        self._project_root_path = project_root_path

        self._args_dict = cmd_args or self._parse_arguments()
        self._file_dict = file_args or self.parse_config()
        self._shub_jobkey = shub_jobkey or self._jobkey_handle()

        self._is_configured = True

    def check(self):
        if not self._is_configured:
            raise ValueError('Settings are not configured yet.')
        return True

    def path_to_config_file(self, file_name='config.json'):
        return os.path.join(self._project_root_path, file_name)

    @classmethod
    def parse_file(cls, path: os.PathLike or str) -> dict:
        with open(path, 'r') as file:
            dictionary = json.load(file)
        return dictionary

    def parse_config(self):
        return self.parse_file(
            self.path_to_config_file(self.config_json_file_name))

    def get_worksheet_id(self, spider_name: str) -> int:
        """ Secure method to use with GSpread.
        Returns only positive integers. """
        worksheet = self.worksheet_number
        take_from_dict = worksheet is None
        if take_from_dict:
            worksheet = self.spider_to_worksheet_dict[spider_name]
        worksheet = int(worksheet)
        if worksheet < 0:
            raise RuntimeError(
                f"Worksheet ID (number) must be positive for security reasons."
                f" Value from {'JSON dictionary' if take_from_dict else 'CMD arguments'}: `{worksheet}`")
        return worksheet

    def get_value(self, key: str,
                  args_only: bool =False,
                  json_only: bool =False,
                  required: bool =True,
                  default=None):
        if not self.is_configured:
            raise RuntimeError('Do not use SettingsMaster\'s properties in class fields.')
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
        tupl = value.split(SCRAPINGHUB_JOBKEY_SEPARATOR)
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
    @property
    def is_configured(self):
        return self.check()

    @property
    def project_root_path(self):
        path = self._project_root_path
        if path is None:
            raise ValueError
        return path

    @property
    def client_secret_path(self):
        return self.path_to_config_file(
            self.gspread_oauth2_credentials_file_name)

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
    def spider_to_worksheet_dict(self) -> dict:
        return self.get_value(
            'SPIDERS',
            json_only=True,
        )

    @property
    def columns(self) -> tuple:
        obj = self.get_value(
            'COLUMNS',
            json_only=True,
            required=False,
            default=[URL, HEADER, TAGS, TEXT, DATE, FINGERPRINT],
        )
        if isinstance(obj, list):
            for i in obj:
                if i not in FIELDS:
                    raise ValueError(f'{i} field is not supported.')
            return tuple(obj)
        else:
            raise TypeError

    @property
    def worksheet_number(self) -> str:
        return self.get_value(
            'WORKSHEET_NUMBER',
            args_only=True,
            required=False,
            default=None,
        )

    # strings
    @property
    def spreadsheet_title(self) -> str:
        return self.get_value(
            'SPREADSHEET_TITLE',
            json_only=False,
            args_only=False,
            required=True,
        )

    @property
    def backup_spreadsheet_title(self) -> str:
        return self.get_value(
            'BACKUP_SPREADSHEET_TITLE',
            json_only=False,
            args_only=False,
            required=False,
            default=None
        )

    @property
    def database_url(self) -> str:
        from .tools import web
        remote_url = web.get_response_content(self.get_value("DATABASE_AUTH_URL"))
        return self.get_value(
            'DATABASE_URL',
            json_only=False,
            args_only=True,
            required=False,
            default=remote_url,
        )

    @property
    def database_table_name(self) -> str:
        return self.get_value(
            'DATABASE_TABLE_NAME',
            json_only=False,
            args_only=False,
            required=True,
            default=None,
        )

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
            required=False,)

    @property
    def enable_database(self) -> str:
        return self.get_value(
            'ENABLE_DATABASE',
            default='True',
            required=False,)

    @property
    def enable_shub(self) -> str:
        return self.get_value(
            'ENABLE_SHUB',
            default='True',
            required=False,)

    @property
    def enable_proxy(self):
        return self.get_value(
            'ENABLE_PROXY',
            default='False',
            required=False,)


cfg = SettingsMaster()
