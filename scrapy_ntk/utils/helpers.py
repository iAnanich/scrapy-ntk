

def collect_kwargs(locals_dict: dict, self=False, cls=False):
    kwargs = locals_dict.copy()

    def pop(key: str):
        try:
            kwargs.pop(key)
        except KeyError:
            pass

    if not self:
        pop('self')
    if not cls:
        pop('cls')

    return kwargs
