import collections
import types
from typing import Iterator, Callable, Sequence

from .counter import Threshold, CounterWithThreshold
from .func import StronglyTypedFunc
from .check import check_obj_type


class ExcludeCheck:

    def __init__(self, iterator: Iterator, default=None):
        check_obj_type(iterator, collections.Iterator, 'Iterator')
        self._iterator = iterator
        self._default = default
        self._is_completed = False
        self._yield_next()

    def _yield_next(self):
        try:
            value = next(self._iterator)
        except StopIteration:
            value = self._default
            self._is_completed = True
        self._value = value
        return value

    def check_next(self, value):
        if self._is_completed:
            return False
        if value == self._value:
            self._yield_next()
            return True
        return False

    @property
    def value(self):
        return self._value


class Context:

    CLOSE_REASON = 'close_reason'
    VALUE = 'value'
    EXCLUDE_VALUE = 'exclude_value'
    _lock_keys = frozenset((CLOSE_REASON, VALUE, EXCLUDE_VALUE))

    _value_type: type = object
    _exclude_value_type: type = object

    def __init__(self, value, exclude_value):
        check_obj_type(value, self._value_type, 'Value')
        check_obj_type(exclude_value, self._exclude_value_type, 'Exclude value')
        self._dict = {
            self.VALUE: value,
            self.EXCLUDE_VALUE: exclude_value,
        }

    def set_close_reason(self, message: str):
        check_obj_type(message, str, 'Message')
        if self.close_reason:
            self._dict[self.CLOSE_REASON].append(message)
        else:
            self._dict[self.CLOSE_REASON] = [message, ]

    @property
    def value(self):
        return self._dict[self.VALUE]

    @property
    def exclude_value(self):
        return self._dict[self.EXCLUDE_VALUE]

    @property
    def close_reason(self) -> str:
        """
        Returns last set close reason message.
        :return: string
        """
        close_reasons = self._dict.get(self.CLOSE_REASON, None)
        if close_reasons:
            return close_reasons[-1]

    def dict_proxy(self):
        return types.MappingProxyType(self._dict)

    def update(self, dictionary: dict):
        for key, val in dictionary.items():
            self[key] = val

    def __getitem__(self, item: str):
        return self._dict[item]

    def __setitem__(self, key: str, value):
        if key not in self._lock_keys:
            self._dict[key] = value
        else:
            raise KeyError(f'{key} key can not be assigned in this way.')


class IterManager:

    _context_type = Context
    _context_processor_output_type = bool

    def __init__(self, general_iterator: Iterator,
                 value_type: type =None, return_type: type =None,
                 exclude_value_type: type =None,
                 exclude_iterator: Iterator =None, exclude_default=None,
                 max_iterations: int or None =None,
                 max_exclude_matches: int or None =None,
                 max_total_excluded: int or None =None,
                 max_returned_values: int or None =None,
                 case_processors: Sequence[Callable] =None,
                 context_processor: Callable =None,
                 return_value_processor: Callable =None,
                 before_finish: Callable =None):
        # `*_type` attributes can be even None because will be only used
        # by `func.StronglyTypedFunc` that uses `check.check_obj_type`
        self._value_type = value_type
        self._return_type = return_type
        self._exclude_type = exclude_value_type

        check_obj_type(general_iterator, collections.Iterator, 'General iterator')
        self._general_iterator = general_iterator

        check_obj_type(exclude_default, exclude_value_type, 'Exclude default value')
        self._exclude_default = exclude_default

        if exclude_iterator is None:
            exclude_iterator = iter([])  # empty iterator
        self._exclude_checker = ExcludeCheck(
            iterator=exclude_iterator,
            default=self._exclude_default)
        self._exclude_iterator = exclude_iterator

        self._total_iterations_threshold = Threshold(max_iterations)
        self._total_iterations_counter = CounterWithThreshold(
            threshold=self._total_iterations_threshold)

        self._exclude_matches_threshold = Threshold(max_exclude_matches)
        self._exclude_matches_counter = CounterWithThreshold(
            threshold=self._exclude_matches_threshold)

        self._total_excluded_threshold = Threshold(max_total_excluded)
        self._total_excluded_counter = CounterWithThreshold(
            threshold=self._total_excluded_threshold)

        self._total_returned_threshold = Threshold(max_returned_values)
        self._total_returned_counter = CounterWithThreshold(
            threshold=self._total_returned_threshold)

        if context_processor is None:
            context_processor = lambda value: Context(value=value, exclude_value=value)
        self._context_processor = StronglyTypedFunc(
            func=context_processor,
            input_type=self._value_type,
            output_type=self._context_type, )

        if before_finish is None:
            before_finish = lambda ctx: None
        self._before_finish = StronglyTypedFunc(
            func=before_finish,
            input_type=self._context_type,
            output_type=None, )

        if return_value_processor is None:
            return_value_processor = lambda ctx: ctx.value
        self._return_value_processor = StronglyTypedFunc(
                func=return_value_processor,
                input_type=self._context_type,
                output_type=self._return_type,)

        if case_processors is None:
            case_processors = []
        self._case_processors = [
            StronglyTypedFunc(
                func=processor,
                input_type=self._context_type,
                output_type=self._context_processor_output_type, )
            for processor in case_processors]

    def _chain_case_processors(self, context: Context) -> bool:
        """

        :param context: Context object
        :return: True if any case processor have returned True, else False
        """
        for processor in self._case_processors:
            if processor.call(context):
                return True
        else:
            return False

    def _check_exclude(self, context: Context) -> bool:
        """

        :param context: Context object
        :return: True if value must be returned, else False
        """
        if self._exclude_checker.check_next(context.exclude_value):
            if self._exclude_matches_counter.add():
                context.set_close_reason('Exclude matches threshold reached.')
            if self._total_excluded_counter.add():
                context.set_close_reason('Total excluded threshold reached.')
            return False
        else:
            self._exclude_matches_counter.drop()
            return True

    def _return(self, context: Context) -> object:
        """
        Increases `total_returned_counter`, and calls `return_value_processor`
        :param context: Context object
        :return: returns processed value
        """
        if self._total_returned_counter.add():
            context.set_close_reason('Returned values threshold reached.')
        return self._return_value_processor.call(context)

    def __iter__(self):
        for value in self._general_iterator:
            context: Context = self._context_processor.call(value)
            if self._chain_case_processors(context):
                continue
            if self._check_exclude(context):
                yield self._return(context)
            if self._total_iterations_counter.add():
                context.set_close_reason('Iterations count threshold reached.')
            if context.close_reason:
                self._before_finish.call(context)
                break
