import re
from functools import wraps
from inspect import signature
from itertools import islice

from typing import Callable, ClassVar, List, Tuple, TypeVar, Union

__all__ = ["CRITERION", "force_regex_filters", "check_filters", "batcher"]
CRITERION = Union[str, Callable[[str], bool]]
RT = TypeVar("RT")


def force_regex_filters(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    Decorator function used to ensure all regex filters follow the (str, match_func) format,
    even when the filter supplied is of format (str, str).
    """
    func_params = signature(func).parameters
    try:
        regex_filter_args_index = tuple(func_params).index("regex_filters")
    except ValueError:
        raise ValueError(f"function `{func.__qualname__}` has no `regex_filters` argument") from None
    del func_params

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        arg_list = list(args)
        if regex_filter_args_index < len(args):
            rfs = arg_list[regex_filter_args_index] or []
        elif "regex_filters" in kwargs:
            rfs = kwargs["regex_filters"] or []
        else:
            rfs = []

        enforced_filters = []
        for field, criterion in rfs:
            # if criterion is callable, keep criterion
            # if criterion is string, use it for regex substr match in a callable returning bool
            # otherwise raise error
            if callable(criterion):
                enforced_filters.append((field, criterion))
            elif isinstance(criterion, str):
                def match(val: str) -> bool:
                    return re.search(criterion, val, re.IGNORECASE) is not None
                enforced_filters.append((field, match))
            else:
                raise ValueError("criterion can only be a callable->bool or a string")

        if regex_filter_args_index < len(args):
            arg_list[regex_filter_args_index] = enforced_filters
        else:
            kwargs.update({"regex_filters": enforced_filters})
        return func(*arg_list, **kwargs)

    return wrapper


def check_filters(model: ClassVar, regex_filters: List[Tuple[str, CRITERION]] = None, _and: bool = True) -> bool:
    """
    Check whether a given model (dataset, geography, group, variable) passes the given list of regex
    filters. The filters can be combined with "and" or "or" logical operators.

    :param model: Dataset, Geography, Group or Variable
    :param regex_filters: list of 2-tuple with first element being the field and second the regex
    :param _and: whether to use "and" or "or" logical operator
    :return: boolean indicating whether model passed filter
    """
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
    """
    Batches an iterable based on a provided batch size

    :param iterable: any iterable
    :param batch_size: size of each batch
    :return: iterable of batches of batch size
    """
    it = iter(iterable)
    while True:
        lst = list(islice(it, 0, batch_size))
        if lst:
            yield lst
        else:
            break
