import typing


TypeOrNone = typing.Union[type, None]


def has_wrong_type(obj, expected_obj_type: TypeOrNone) -> bool:
    """
    Checks if given `obj` object has not given `expected_obj_type` type. If
    `expected_obj_type` is `None` than it will return `True`
    :param obj: any object
    :param expected_obj_type: expected type of the object or `None`
    :return: `True` if `obj` object is not of `expected_obj_type` type, `False`
    if `expected_obj_type` is `None` or `obj` object has `expected_obj_type` type
    """
    # if `expected` type is `None` it will
    # return False without `isinstance` call
    return expected_obj_type is not None and not isinstance(obj, expected_obj_type)


def has_any_type(obj, *obj_types: type):
    return any(isinstance(obj, obj_type) for obj_type in obj_types)


def raise_type_error(obj_repr: str, obj_type: type, expected_obj_type: type,
                     obj_name: str ='This'):
    raise TypeError(
        f'{obj_name} {obj_repr} has "{obj_type}" type while '
        f'"{expected_obj_type}" is expected.'
    )


def check_obj_type(obj, expected_obj_type: TypeOrNone, obj_name: str ='object'):
    if has_wrong_type(obj=obj, expected_obj_type=expected_obj_type):
        raise_type_error(
            obj_name=obj_name,
            obj_repr=repr(obj),
            obj_type=type(obj),
            expected_obj_type=expected_obj_type,
        )


def raise_or_none(func, *exceptions):
    if callable(func) and not isinstance(func, type):
        def wrapped(*args, raise_=True, **kwargs):
            if raise_:
                return func(*args, **kwargs)

            try:
                return func(*args, **kwargs)
            except:
                return None
        return wrapped
    if issubclass(func, Exception):
        exceptions = tuple([func, *exceptions])
        def wrapper(func):
            def wrapped(*args, raise_=True, **kwargs):
                if raise_:
                    return func(*args, **kwargs)

                try:
                    return func(*args, **kwargs)
                except exceptions:
                    return None
            return wrapped
        return wrapper
    elif callable(func):
        def wrapped(*args, raise_=True, **kwargs):
            if raise_:
                return func(*args, **kwargs)

            try:
                return func(*args, **kwargs)
            except:
                return None
        return wrapped
    else:
        raise_type_error(repr(func), type(func), typing.Union[typing.Callable, Exception])
