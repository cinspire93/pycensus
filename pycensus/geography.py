import requests
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from .utils import CRITERION, check_filters
from typing import List, Optional, Tuple


@dataclass(order=True)
class Geography:
    sort_index: int = field(init=False, repr=False)
    name: str
    geo_level: str
    reference_date: datetime
    requires: Optional[List[str]] = field(default_factory=lambda: [])
    wildcard: Optional[List[str]] = field(default_factory=lambda: [])
    optional_wildcard: str = ""
    complexity: int = field(init=False, repr=False)

    filterable_attrs = ["name", "geo_level"]

    def __post_init__(self):
        self.sort_index = int(self.geo_level)
        self.complexity = len(self.requires) + 1

    def filter_to_params(self, geo_filters: Optional[List[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
        """
        Converts a given list of 2-tuple geography filters to a list of tuples used by
        the requests library to create query params.

        :param geo_filters: list of 2-tuple with first element being the geo element and the second geo ids
        :return: list of 2-tuple request query parameters
        """
        if geo_filters is None:
            return [("for", f"{self.name}:*")]

        filter_dict = defaultdict(list)
        for geo_loc, value in geo_filters:
            filter_dict[geo_loc].append(value)
        filters = {k: ",".join(v) for k, v in filter_dict.items()}

        required_fields = [self.name] + self.requires
        for f in required_fields:
            if f not in filters and f != self.optional_wildcard:
                raise ValueError(f"required geography field not found in filter: {f}")

        for_filter = [("for", f"{self.name}:{filters.pop(self.name)}")]
        in_filter = []
        for f, value in filters.items():
            if value == "*" and field not in self.wildcard:
                raise ValueError(f"geography field {f} does not accept wildcards")
            in_filter.append(("in", f"{f}:{value}"))

        return for_filter + in_filter


def _search_geography(geo_url: str, regex_filters: List[Tuple[str, CRITERION]]) -> List[Geography]:
    """
    Searches geographies returned through the geo url. Reduce outputs by specifying a list of
    2-tuple regex filters.

    :param geo_url: the url containing all geographies, provided by datasets
    :param regex_filters: list of 2-tuple with first element being the field and second the regex
    :return: list of Geographies
    """
    resp = requests.get(geo_url)
    resp.raise_for_status()

    hits = []
    with resp:
        geo_infos = resp.json().get("fips")
        for info in geo_infos:
            model = Geography(
                name=info["name"],
                geo_level=info["geoLevelDisplay"],
                reference_date=datetime.strptime(info["referenceDate"], "%Y-%m-%d"),
                requires=info.get("requires", []),
                wildcard=info.get("wildcard", []),
                optional_wildcard=info.get("optionalWithWCFor", ""),
            )
            if check_filters(model, regex_filters=regex_filters):
                hits.append(model)
    return hits
