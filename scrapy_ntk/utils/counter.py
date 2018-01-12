import typing


ThresholdBasis = typing.Union[int, None]


class Threshold:

    def __init__(self, val: ThresholdBasis=None):
        if val is None or val is 0:
            self._value = 0  # as equivalent to None
        elif isinstance(val, int):
            if val <= 0:
                raise TypeError(
                    f'first argument must not be less then zero, got "{val}".')
            else:
                self._value = val
        else:
            raise TypeError(
                f'first argument must be of type `int` or `NoneType`, '
                f'got "{val}" of "{type(val)}" type.')

    # public API
    @property
    def value(self) -> ThresholdBasis:
        if not self:  # if self._value == 0
            return None
        return self._value

    def __int__(self):
        if not self:
            raise TypeError(
                f'this threshold equals to None and '
                f'can not be used as an integer.')
        return self._value

    def __bool__(self):
        return self._value != 0

    # string representations
    def __repr__(self):
        return f'<Threshold {self.value}>'

    def __str__(self):
        return str(self.value)


class Counter:

    def __init__(self):
        self._is_enabled = True
        self._count = 0

    def add(self) -> bool:
        if self._is_enabled:
            self._count += 1
            return bool(self)
        else:
            return False

    def drop(self):
        if self._is_enabled:
            self._count = 0

    @property
    def count(self) -> int:
        return self._count

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def enable(self):
        self._is_enabled = True

    def disable(self):
        self._is_enabled = False

    # string representations
    def __repr__(self):
        return f'<Counter count={self._count}>'

    def __str__(self):
        return str(self._count)


class CounterWithThreshold(Counter):

    def __init__(self, threshold: Threshold = None):
        super().__init__()
        if not threshold:
            self._is_enabled = False
            self._count = -1
        self._threshold = threshold

    @property
    def threshold(self) -> Threshold:
        return self._threshold

    def __bool__(self):
        if self._is_enabled:
            return self._count >= int(self._threshold)
        return True

    def __repr__(self):
        return f'<Counter {"REACHED" if bool(self) else "count=%s" % self._count}>'
