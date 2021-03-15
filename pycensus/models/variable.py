import requests
from dataclasses import dataclass

from pycensus.utils import CRITERION, check_filters, force_regex_filters
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


@force_regex_filters
def _search_variables(var_url: str,
                      regex_filters: List[Tuple[str, CRITERION]] = None,
                      collective_eval: str = "and") -> List[Variable]:
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
            if check_filters(model, regex_filters=regex_filters, _and=(collective_eval == "and")):
                hits.append(model)
    return hits
