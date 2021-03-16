import requests
from dataclasses import dataclass
from functools import lru_cache

from .utils import CRITERION, check_filters
from typing import List, Optional, Tuple


@dataclass(order=True)
class Variable:
    name: str
    label: str
    group_name: str
    limit: int
    concept: Optional[str] = ""
    predicate_type: Optional[str] = None
    attributes: Optional[List[str]] = None

    filterable_attrs = ["name", "label", "concept", "group_name"]


@lru_cache(maxsize=2)
def _search_variables(var_url: str,
                      regex_filters: List[Tuple[str, CRITERION]] = None,
                      and_or: str = "and") -> List[Variable]:
    """
    Searches variables returned through the variable url. Reduce outputs by specifying
    a list of 2-tuple regex filters and the and/or logical operator used to combine said
    filters.

    :param var_url: the url containing all variables, provided by groups or datasets
    :param regex_filters: list of 2-tuple with first element being the field and second the regex
    :param and_or: whether to use "and" or "or" to join regex_filters
    :return: list of Variables
    """
    resp = requests.get(var_url)
    resp.raise_for_status()

    hits = []
    with resp:
        var_infos = resp.json().get("variables")
        for var_name, info in var_infos.items():
            if info.get("attributes", None):
                attrs = info["attributes"].split(",")
            else:
                attrs = None
            model = Variable(
                name=var_name,
                label=info["label"],
                group_name=info["group"],
                limit=info["limit"],
                concept=info.get("concept", ""),
                predicate_type=info.get("predicateType", ""),
                attributes=attrs,
            )
            if check_filters(model, regex_filters=regex_filters, _and=(and_or == "and")):
                hits.append(model)
    return hits
