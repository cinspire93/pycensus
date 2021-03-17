import pytest
import re
from dataclasses import dataclass

from pycensus.utils import *


class TestUtils:

    @pytest.fixture
    def bad_class(self):
        @dataclass
        class BadClass:
            random_attr = "wrong_attr"
        return BadClass()

    @pytest.fixture
    def proper_class(self):
        @dataclass
        class ProperClass:
            attr1: str
            attr2: str
            filterable_attrs = ["attr1", "attr2"]
        return ProperClass("val1", "val2")

    def test_check_filters_raise_on_no_filter_attr(self, bad_class):
        with pytest.raises(AttributeError):
            check_filters(bad_class, regex_filters=[])

    def test_check_filters_raise_on_filter_attr_mismatch(self, proper_class):
        with pytest.raises(ValueError):
            check_filters(proper_class, regex_filters=[("attr3", "wrong")])

    def test_check_filters_and_or(self, proper_class):
        filters = [
            ("attr1", lambda x: re.search("1", x, re.IGNORECASE) is not None),
            ("attr2", lambda x: re.search("3", x, re.IGNORECASE) is not None),
        ]
        assert not check_filters(proper_class, filters, _and=True)
        assert check_filters(proper_class, filters, _and=False)

    def test_force_regex_filters_raise_on_no_parameter(self):
        with pytest.raises(ValueError):
            @force_regex_filters
            def test(a):
                return a

    def test_force_regex_filters_ensure_callables(self):
        rfs = [
            ("attr1", "1"),
            ("attr2", lambda x: re.search("3", x, re.IGNORECASE) is not None),
        ]

        @force_regex_filters
        def test(regex_filters):
            return regex_filters

        enforced_filters = test(rfs)
        for (enforced_field, enforced_filter), (orig_field, orig_filter) in zip(enforced_filters, rfs):
            assert enforced_field == orig_field
            assert callable(enforced_filter)

    def test_batcher(self):
        lst = range(9)
        assert list(batcher(lst, batch_size=5)) == [[0, 1, 2, 3, 4], [5, 6, 7, 8]]
