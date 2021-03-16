import requests
from dataclasses import dataclass

from .variable import Variable, _search_variables
from .utils import CRITERION, check_filters, force_regex_filters
from typing import List, Tuple


@dataclass(order=True)
class Group:
    name: str
    description: str
    var_url: str

    filterable_attrs = ["name", "description"]

    def search_variables(self,
                         regex_filters: List[Tuple[str, CRITERION]] = None,
                         collective_eval: str = "and") -> List[Variable]:
        return _search_variables(self.var_url, regex_filters, collective_eval)


@force_regex_filters
def _search_groups(group_url: str,
                   regex_filters: List[Tuple[str, CRITERION]] = None,
                   collective_eval: str = "and") -> List[Group]:
    resp = requests.get(group_url)
    resp.raise_for_status()

    hits = []
    with resp:
        group_infos = resp.json().get("groups")
        for info in group_infos:
            model = Group(
                name=info["name"],
                description=info["description"],
                var_url=info["variables"],
            )
            if check_filters(model, regex_filters=regex_filters, _and=(collective_eval == "and")):
                hits.append(model)
    return hits
