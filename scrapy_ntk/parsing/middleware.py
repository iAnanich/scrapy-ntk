import typing

from scrapy.selector import SelectorList
from scrapy.http import HtmlResponse

from ..utils.func import StronglyTypedFunc, FuncSequence


class MiddlewareContainer(FuncSequence):

    def __init__(self, middleware_list: typing.List[StronglyTypedFunc]):
        super().__init__(*middleware_list)


class SelectMiddleware(StronglyTypedFunc):

    input_type = SelectorList
    output_type = SelectorList


class HTMLMiddleware(StronglyTypedFunc):

    input_type = HtmlResponse
    output_type = SelectorList


# shortcuts
SMW = SelectMiddleware
HMW = HTMLMiddleware


# ====================
#  actual middleware
# ====================
def select(selector: SelectorList, string_selector: str) -> SelectorList:
    return selector.css(string_selector)


def childes(selector: SelectorList,
            parent_tag: str,) -> SelectorList:
    if not isinstance(parent_tag, str):
        raise TypeError('Given `parent_tag` is not `str` object.')
    childes_selector = SelectorList()
    iterate_selector_string_template = parent_tag + ' > :nth-child({i})'
    i = 1
    # starting the iteration
    while True:
        child = selector.css(iterate_selector_string_template.format(i=i))
        if child:
            childes_selector.append(child)
            i += 1
        else:
            return childes_selector


def page_name(path: str) -> str:
    # 'a/b/c-d.e%A0.html' -> 'c-d.e%A0'
    return path.split('/')[-1][:-5]


def number_in_last_folder(path: str) -> str:
    # 'a/b/c/12345-d-e-f.html' -> '12345'
    if path.endswith('/'):
        path = path[:-1]
    return path.split('/')[-1].split('-')[0]


def last_folder(path: str) -> str:
    # 'a/b/c/d' -> 'd'
    return path.split('/')[-1]
