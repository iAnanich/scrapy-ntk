import typing

from .check import check_obj_type, raise_or_none

POSITIVE = frozenset([
    'TRUE', 'True', 'true', 'T', 't',
    'YES', 'Yes', 'yes', 'Y', 'y',
    '+', '1',
])
NEGATIVE = frozenset([
    'FALSE', 'False', 'false', 'F', 'f',
    'NO', 'No', 'no', 'N', 'n',
    '-', '0',
])


def to_string(option: str, option_length: int=None) -> str:
    # TODO: add encoding
    check_obj_type(option, str, 'Option')

    if option_length is not None:
        check_obj_type(option_length, int, 'String length')
        real_option_length = len(option)
        if real_option_length != option_length:
            raise ValueError(
                f'Given "{option}" has {real_option_length} length '
                f'while {option_length} length is expected.')
    return option


to_str = to_string


def option_to_string(positional_argument: int =0, keyword_argument: str ='option'):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            try:
                option = kwargs[keyword_argument]
                str_option = to_str(option)
                kwargs[keyword_argument] = str_option
            except KeyError:
                try:
                    option = args[positional_argument]
                    str_option = to_str(option)
                    list_args = list(args)
                    list_args[positional_argument] = str_option
                    args = tuple(list_args)
                except IndexError:
                    raise TypeError(
                        'Can not find "option" to convert it to string.')
            return func(*args, **kwargs)
        return wrapped
    return wrapper


@raise_or_none
def from_set(option: str, possible_options: typing.FrozenSet[str]) -> bool or None:
    if option in possible_options:
        return True
    else:
        raise ValueError(
            f'Given "{option}" option is not one of the '
            f'[{", ".join(possible_options)}] possible options.'
        )


@option_to_string()
def to_boolean(option: str) -> bool:
    if from_set(option, POSITIVE, raise_=False):
        return True
    elif from_set(option, NEGATIVE, raise_=False):
        return False
    else:
        raise ValueError(
            f'Given "{option}" option value can not be recognised as boolean. '
            f'Try [{", ".join(POSITIVE)}] for positive meaning '
            f'and [{", ".join(NEGATIVE)}] for negative meaning')


to_bool = to_boolean


@option_to_string()
def to_integer(option: str) -> int:
    try:
        integer = int(option)
    except ValueError:
        raise ValueError(
            f'Given "{option}" option value can not be recognised as integer. '
        ) from None
    else:
        return integer


to_int = to_integer
