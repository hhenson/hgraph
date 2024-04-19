import pytest

from hgraph import TIME_SERIES_TYPE, TSL, TS, Size
from hgraph.nodes._conditional import if_then_else, if_true, filter_
from hgraph.test import eval_node


def test_if_then_else():
    expected = [
        None,
        2,
        6,
        3
    ]

    assert eval_node(if_then_else, [None, True, False, True], [1, 2, 3], [4, 5, 6]) == expected


@pytest.mark.parametrize("condition,tick_once_only,expected", [
    ([True, False, True], False, [True, None, True]),
    ([True, False, True], True, [True, None, None]),
])
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected


def test_filter_():
    assert eval_node(filter_, [True, False, False, True, None], [1, 2, 3, None, 4]) == [1, None, None, 3, 4]

def test_filter_tsl():
    assert eval_node(filter_[TIME_SERIES_TYPE: TSL[TS[int], Size[2]]],
                     [True, False, None, True],
                     [(1, 1), (2, 2), {1: 3}, None, {0: 5}]) == \
                     [{0: 1, 1: 1}, None, None, {0: 2, 1: 3}, {0: 5}]