from typing import Callable

import pytest

from hgraph import graph, TIME_SERIES_TYPE, TS, WiringError, TSL, Size
from hgraph.test import eval_node


def test_wp_operators():
    @graph
    def g(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return lhs + rhs

    assert eval_node(g[TIME_SERIES_TYPE: TS[int]], lhs=[1, 2, None], rhs=[2, None, 3]) == [3, 4, 5]
    with pytest.raises(WiringError):
        assert eval_node(g[TIME_SERIES_TYPE: TSL[TS[str], Size[1]]], lhs=[], rhs=[]) == []
