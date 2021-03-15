import re
from functools import wraps
from inspect import signature
from itertools import islice

from typing import Callable, ClassVar, List, Tuple, TypeVar, Union

__all__ = ["CRITERION", "force_regex_filters", "check_filters", "batcher"]
CRITERION = Union[str, Callable[[str], bool]]
RT = TypeVar("RT")


def force_regex_filters(func: Callable[..., RT]) -> Callable[..., RT]:
    func_params = signature(func).parameters
    try:
        regex_filter_args_idx = tuple(func_params).index("regex_filters")
    except ValueError:
        raise ValueError(f"function `{func.__qualname__}` has no `regex_filters` argument") from None
    del func_params

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        arg_list = list(args)
        if regex_filter_args_idx < len(args):
            rfs = arg_list[regex_filter_args_idx] or []
        elif "regex_filters" in kwargs:
            rfs = kwargs["regex_filters"] or []
        else:
            rfs = []

        enforced_filters = []
        for field, criterion in rfs:
            if not callable(criterion):
                def match(val: str):
                    return re.search(criterion, val, re.IGNORECASE) is not None
                enforced_filters.append((field, match))
            else:
                enforced_filters.append((field, criterion))

        if regex_filter_args_idx < len(args):
            arg_list[regex_filter_args_idx] = rfs
        elif "regex_filters" in kwargs:
            kwargs.update({"regex_filters": enforced_filters})
        return func(*arg_list, **kwargs)

    return wrapper


def check_filters(model: ClassVar, regex_filters: List[Tuple[str, CRITERION]] = None, _and: bool = True) -> bool:
    filter_evals = []
    for field, match in regex_filters:
        if field not in model.filterable_attrs:
            raise ValueError(f"field `{field}` cannot be used as a regex filter on `{model.__class__.__name__}`")
        val = getattr(model, field)
        filter_evals.append(match(val))
    if not filter_evals:
        return True
    return (_and and all(filter_evals)) or (not _and and any(filter_evals))


def batcher(iterable, batch_size: int = 50):
    it = iter(iterable)
    while True:
        lst = list(islice(it, 0, batch_size))
        if lst:
            yield lst
        else:
            break
