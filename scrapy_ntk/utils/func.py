import abc
import typing


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
        self._check_type(value, self.input_type, 'Input')

    def _check_output(self, value):
        self._check_type(value, self.output_type, 'Output')

    def _check_type(self, value, expected: type or None, action: str ='Given'):
        if self._is_wrong_type(value, expected):
            self._raise_type_error(action, value, expected)

    def _is_wrong_type(self, value, expected: type or None):
        # if `expected` type is `None` it will
        # return False without `isinstance` call
        return expected is not None and not isinstance(value, expected)

    def _raise_type_error(self, action: str, value, expected_type: type):
        raise TypeError(
            f'{action} value type is {type(value)}, but not {expected_type}')


class FuncSequence:

    func_type = BaseFunc

    def __init__(self, *funcs: BaseFunc):
        for middleware in funcs:
            self._check_func_type(middleware)
        self._list: typing.List[BaseFunc] = list(funcs)

    def process(self, value):
        for middleware in self._list:
            value = middleware.call(value)
        else:
            return value

    def _check_func_type(self, func):
        if not isinstance(func, self.func_type):
            raise TypeError(f'is not `{self.func_type.__name__}` object.')

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

    def append(self, func: BaseFunc):
        self._check_func_type(func)
        self._list.append(func)

    def remove(self, value: BaseFunc):
        del self._list[self._list.index(value)]

    def extend(self, funcs: typing.Sequence[BaseFunc]):
        for func in funcs:
            self._check_func_type(func)
            self._list.append(func)
