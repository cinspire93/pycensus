import requests
from dataclasses import dataclass
from itertools import chain

from .geography import Geography, _search_geography
from .group import Group, _search_groups
from .variable import Variable, _search_variables
from .utils import *
from typing import List, Optional, Tuple


@dataclass
class Dataset:
    title: str
    description: str
    year: int
    path: str
    geo_url: str
    var_url: str
    group_url: str
    access_url: Optional[str] = None

    def search_geography(self, regex_filters: List[Tuple[str, CRITERION]] = None) -> List[Geography]:
        return _search_geography(self.geo_url, regex_filters)

    def search_groups(self,
                      regex_filters: List[Tuple[str, CRITERION]] = None,
                      collective_eval: str = "and") -> List[Group]:
        return _search_groups(self.group_url, regex_filters, collective_eval)

    def search_variables(self,
                         regex_filters: List[Tuple[str, CRITERION]] = None,
                         collective_eval: str = "and") -> List[Variable]:
        return _search_variables(self.var_url, regex_filters, collective_eval)

    def download(self,
                 geo_level: Optional[str] = None,
                 geography: Optional[Geography] = None,
                 geo_filters: Optional[List[Tuple[str, str]]] = None,
                 variables: Optional[List[Variable]] = None,
                 variable_names: Optional[List[str]] = None) -> List[List[str]]:
        if geo_level is not None:
            geo = self.search_geography([("geo_level", geo_level)])[0]
        elif geography is not None:
            geo = geography
        else:
            raise ValueError("must specify either `geo_level` or `geography`")
        geo_params = geo.filter_to_params(geo_filters)

        if variables:
            var_names = [v.name for v in variables]
        elif variable_names is not None:
            var_names = variable_names
        else:
            raise ValueError("must specify either `variables` or `variable_names`")

        return self._download(geo.complexity, geo_params, var_names)

    def _download(self,
                  geo_complexity: int,
                  geo_params: List[Tuple[str, str]],
                  var_names: List[str],
                  var_batch_size: int = 50) -> List[List[str]]:
        var_batches = batcher(var_names, batch_size=var_batch_size)
        expected_batches = len(var_names) / var_batch_size
        batch_outputs = []
        for i, batch in enumerate(var_batches, 1):
            params = [("get", ",".join(batch))] + geo_params
            resp = requests.get(self.access_url, params=params)
            resp.raise_for_status()
            results = resp.json()
            if i < expected_batches:
                results = [row[:-1 * geo_complexity] for row in results]
            batch_outputs.append(results)
        final_output = [list(chain(*row_batches)) for row_batches in zip(*batch_outputs)]
        return final_output

    @staticmethod
    def initialize(year: int, path: str):
        ds = search_datasets(year, path, include_sub_datasets=False)
        if not ds:
            raise ValueError("no dataset found, check `year` and `path`")
        return ds[0]


def search_datasets(year: int, path: str = None, include_sub_datasets: bool = True) -> List[Dataset]:
    base_url = "https://api.census.gov/data"
    resp = requests.get(f"{base_url}/{year}.json")
    resp.raise_for_status()

    hits = []
    with resp:
        dataset_infos = resp.json().get("dataset", [])
        p = path or ""
        for info in dataset_infos:
            access_url = None
            api_dist_urls = list(filter(lambda x: x["format"] == "API", info["distribution"]))
            if api_dist_urls:
                access_url = api_dist_urls[0]["accessURL"]
            dataset = Dataset(
                title=info["title"],
                description=info["description"],
                year=year,
                path="/".join(info["c_dataset"]),
                geo_url=info["c_geographyLink"],
                var_url=info["c_variablesLink"],
                group_url=info["c_groupsLink"],
                access_url=access_url,
            )
            if (not include_sub_datasets and dataset.path.endswith(p)) or \
               (include_sub_datasets and p in dataset.path):
                hits.append(dataset)
    return hits
