from typing import Iterator, List, Dict

from scrapy.selector import Selector

from ..utils.check import check_obj_type


Link = str
StringSelector = str


class LinkExplorer:

    allowed_string_selector_end = 'a::attr(href)'

    def __init__(self, list_of_string_css_selectors: List[StringSelector]):
        for i, string_selector in enumerate(list_of_string_css_selectors):
            check_obj_type(string_selector, Link, f'Element #{i}')
            if not string_selector.endswith(self.allowed_string_selector_end):
                raise ValueError(
                    f'string selector must end with '
                    f'"{self.allowed_string_selector_end}", '
                    f'got "{string_selector}".')
        self.list_of_string_selectors = list_of_string_css_selectors

    # --- --- ---
    @staticmethod
    def extract_all(selector: Selector,
                    string_selector: StringSelector) -> List[Link]:
        selected = selector.css(string_selector)
        if selected:
            return selected.extract()
        else:
            raise RuntimeError(f'`{string_selector}` selector failed')

    def yield_links(self, selector: Selector) -> Iterator[Link]:
        iterators: List[Iterator] = []
        errors: Dict[StringSelector, Exception] = {}

        for string_selector in self.list_of_string_selectors:
            try:
                gen = self.extract_all(selector, string_selector)
                iterators.append(gen)
            except RuntimeError as exc:
                errors[string_selector] = exc
        for iterator in iterators:
            yield from iterator
        for string_selector, error in errors.items():
            raise error
