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

    def search_geography(self,
                         regex_filters: List[Tuple[str, CRITERION]] = None) -> List[Geography]:
        """
        Searches geography associated with the dataset itself. Reduce outputs further by
        specifying a list of 2-tuple regex filters.

        :param regex_filters: list of 2-tuple with first element being the field and second the regex
        :return: list of Geographies
        """
        return _search_geography(self.geo_url, regex_filters)

    def search_groups(self,
                      regex_filters: List[Tuple[str, CRITERION]] = None,
                      and_or: str = "and") -> List[Group]:
        """
        Searches groups associated with the dataset itself. Reduce outputs further by specifying
        a list of 2-tuple regex filters and the and/or logical operator used to combine said
        filters.

        :param regex_filters: list of 2-tuple with first element being the field and second the regex
        :param and_or: whether to use "and" or "or" to join regex_filters
        :return: list of Groups
        """
        return _search_groups(self.group_url, regex_filters, and_or)

    def search_variables(self,
                         regex_filters: List[Tuple[str, CRITERION]] = None,
                         and_or: str = "and") -> List[Variable]:
        """
        Searches variables associated with the dataset itself. Reduce outputs further by
        specifying a list of 2-tuple regex filters and the and/or logical operator used to
        combine said filters.

        :param regex_filters: list of 2-tuple with first element being the field and second the regex
        :param and_or: whether to use "and" or "or" to join regex_filters
        :return: list of Variables
        """
        return _search_variables(self.var_url, regex_filters, and_or)

    def download(self,
                 geo_level: Optional[str] = None,
                 geography: Optional[Geography] = None,
                 geo_filters: Optional[List[Tuple[str, str]]] = None,
                 variable_names: Optional[List[str]] = None,
                 variables: Optional[List[Variable]] = None) -> List[List[str]]:
        """
        Downloads census data based on geography info (geo_level or geography) and an
        optional list of 2-tuple geo_filters. Either a list of variables or variable
        names must be supplied to this function.

        :param geo_level: geography level as shown in the census API, takes precedence over geography
        :param geography: geography object
        :param geo_filters: list of 2-tuple with first element being the geo element and the second geo ids
        :param variable_names: list of variable names, takes precedence over variables
        :param variables: list of Variables
        :return: actual data
        """
        if geo_level is not None:
            geo = self.search_geography([("geo_level", geo_level)])[0]
        elif geography is not None:
            geo = geography
        else:
            raise ValueError("must specify either `geo_level` or `geography`")
        geo_params = geo.filter_to_params(geo_filters)

        if variable_names is not None:
            var_names = variable_names
        elif variables is not None:
            var_names = [v.name for v in variables]
        else:
            raise ValueError("must specify either `variable_names` or `variables`")

        return self._download(geo.complexity, geo_params, var_names)

    def _download(self,
                  geo_complexity: int,
                  geo_params: List[Tuple[str, str]],
                  var_names: List[str],
                  var_batch_size: int = 50) -> List[List[str]]:
        """
        Downloads census data based on geography info, such as the geo complexity and
        geo parameters in the form of a list of 2-tuples. Must also specify the variable
        names one wises to be returned by census API. The batch size is needed because
        we can only query 50 variables in each API call.

        :param geo_complexity: number of geography tiers in a given geography
        :param geo_params: list of 2-tuple request query parameters
        :param var_names: names of variables
        :param var_batch_size: batch_size for variables in each API call
        :return: actual data
        """
        var_batches = batcher(var_names, batch_size=var_batch_size)
        expected_batches = len(var_names) / var_batch_size
        batch_outputs = []
        for i, batch in enumerate(var_batches, 1):
            params = [("get", ",".join(batch))] + geo_params
            resp = requests.get(self.access_url, params=params)
            resp.raise_for_status()
            results = resp.json()
            # geo complexity indicates the number of columns reserved by the API
            # to store geography tiers. Since it will be included in every batch
            # API call, we filter them out for every batch except the last one.
            if i < expected_batches:
                results = [row[:-1 * geo_complexity] for row in results]
            batch_outputs.append(results)
        # this is equivalent to concatenating batch outputs horizontally
        final_output = [list(chain(*row_batches)) for row_batches in zip(*batch_outputs)]
        return final_output

    @staticmethod
    def initialize(year: int, path: str):
        """
        Initializes a dataset based on the year and the path of the census dataset.
        If more than 1 record is returned, the first dataset will used to instantiate
        the class.

        :param year: year of the census dataset
        :param path: path of the census dataset
        :return: Dataset
        """
        datasets = search_datasets(year, path, include_sub_datasets=False)
        if not datasets:
            raise ValueError("no dataset found, check `year` and `path`")
        return datasets[0]


def search_datasets(year: int, path: str = None, include_sub_datasets: bool = True) -> List[Dataset]:
    """
    Searches through all datasets in the census API based on the year and the path.
    For datasets like acs5 and acs5/profile, you can also limit the number of outputs
    by preventing it from returning sub datasets that share the same prefix.

    :param year: year of the census dataset
    :param path: path of the census dataset
    :param include_sub_datasets: whether to include sub datasets
    :return: list of Datasets
    """
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
