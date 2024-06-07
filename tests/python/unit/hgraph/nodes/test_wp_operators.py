from typing import Callable

import pytest

from hgraph import graph, TIME_SERIES_TYPE, TS, WiringError, TSL, Size
from hgraph.test import eval_node


def test_wp_operators_wiring():
    @graph
    def g(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return lhs + rhs

    assert eval_node(g[TIME_SERIES_TYPE: TS[int]], lhs=[1, 2, None], rhs=[2, None, 3]) == [3, 4, 5]


def test_wp_operators_wiring_w_consts():
    @graph
    def g(lhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return (lhs + 1) + (1 + lhs)

    assert eval_node(g[TIME_SERIES_TYPE: TS[int]], lhs=[1, 2, None]) == [4, 6, None]
    with pytest.raises(WiringError):
        eval_node(g[TIME_SERIES_TYPE: TSL[TS[str], Size[1]]], lhs=[])


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [1, 3, 3], [True, False, True]),
    ([1, 2], [4, 2, 3], [False, True, False]),
    ([None, 1, None, 2], [1, None, 2], [None, True, False, True]),
])
def test_eq_operator(lhs, rhs, expected):
    @graph
    def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
       return lhs == rhs

    assert eval_node(eq_, lhs, rhs) == expected