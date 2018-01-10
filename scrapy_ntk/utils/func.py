import abc
import typing

from .check import has_wrong_type, raise_type_error, check_obj_type


class BaseFunc(abc.ABC):

    def __init__(self, func, args: tuple = None, kwargs: dict = None):
        if kwargs is None:
            kwargs = dict()
        if args is None:
            args = tuple()

        if not isinstance(args, tuple):
            raise TypeError('Given `args` are not `tuple` object.')
        if not isinstance(kwargs, dict):
            raise TypeError('Given `kwargs` are not `dict` object.')
        if not callable(func):
            raise TypeError('Given `func` argument must be callable.')

        self.function = func
        self.args = args
        self.kwargs = kwargs

    @abc.abstractmethod
    def call(self, input_value):
        pass


class Func(BaseFunc):

    def call(self, input_value):
        return self.function(input_value, *self.args, **self.kwargs)


class StronglyTypedFunc(BaseFunc):

    # None value will cause type check to pass any type
    output_type = None
    input_type = None

    def __init__(self, func, args: tuple =None, kwargs: dict =None,
                 input_type: type =None, output_type: type =None):
        super().__init__(
            func=func,
            args=args,
            kwargs=kwargs,
        )

        # override class attributes
        if input_type is not None:
            self.input_type = input_type
        if output_type is not None:
            self.output_type = output_type

    def call(self, input_value):
        self._check_input(input_value)
        output_value = self.function(input_value, *self.args, **self.kwargs)
        self._check_output(output_value)
        return output_value

    def _check_input(self, value):
        self._check_type(value, self.input_type, 'input')

    def _check_output(self, value):
        self._check_type(value, self.output_type, 'output')

    def _check_type(self, value, expected: type or None, action: str):
        if has_wrong_type(value, expected):
            raise_type_error(
                obj_repr=repr(value),
                obj_type=type(value),
                obj_name=f'{action.capitalize()} value',
                expected_obj_type=expected,
            )


class FuncSequence:

    func_type = BaseFunc

    def __init__(self, *funcs: func_type):
        for i, func in enumerate(funcs):
            check_obj_type(func, self.func_type, f'Callable #{i}')
        self._list: typing.List[self.func_type] = list(funcs)

    def process(self, value):
        for middleware in self._list:
            value = middleware.call(value)
        else:
            return value

    # some list methods
    def copy(self):
        return self.__class__(*self._list)

    def clear(self):
        try:
            while True:
                self._list.pop()
        except IndexError:
            pass

    def reverse(self):
        sequence = self._list
        n = len(sequence)
        for i in range(n//2):
            sequence[i], sequence[n - i - 1] = sequence[n - i - 1], sequence[i]

    def pop(self, index: int =-1):
        v = self._list[index]
        del self._list[index]
        return v

    def append(self, func: func_type):
        check_obj_type(func, self.func_type, f'Callable')
        self._list.append(func)

    def remove(self, value: func_type):
        del self._list[self._list.index(value)]

    def extend(self, funcs: typing.Sequence[func_type]):
        for i, func in enumerate(funcs):
            check_obj_type(func, self.func_type, f'Callable #{i}')
            self._list.append(func)
