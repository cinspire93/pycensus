import requests
from dataclasses import dataclass

from .variable import Variable, _search_variables
from .utils import CRITERION, check_filters
from typing import List, Tuple


@dataclass(order=True)
class Group:
    name: str
    description: str
    var_url: str

    filterable_attrs = ["name", "description"]

    def search_variables(self,
                         regex_filters: List[Tuple[str, CRITERION]] = None,
                         and_or: str = "and") -> List[Variable]:
        """
        Searches variables assigned to the group itself. Reduce outputs further by specifying
        a list of 2-tuple regex filters and the and/or logical operator used to combine said
        filters.

        :param regex_filters: list of 2-tuple with first element being the field and second the regex
        :param and_or: whether to use "and" or "or" to join regex_filters
        :return: list of Variables
        """
        return _search_variables(self.var_url, regex_filters, and_or)


def _search_groups(group_url: str,
                   regex_filters: List[Tuple[str, CRITERION]] = None,
                   and_or: str = "and") -> List[Group]:
    """
    Searches groups returned through the group url. Reduce outputs by specifying a list of
    2-tuple regex filters and identifying the and/or logical operator.

    :param group_url: the url containing all variables, provided by datasets
    :param regex_filters: list of 2-tuple with first element being the field and second the regex
    :param and_or: whether to use "and" or "or" to join regex_filters
    :return: list of Groups
    """
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
            if check_filters(model, regex_filters=regex_filters, _and=(and_or == "and")):
                hits.append(model)
    return hits
