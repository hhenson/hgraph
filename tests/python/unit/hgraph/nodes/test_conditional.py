from hgraph import TIME_SERIES_TYPE, TSL, TS, Size
from hgraph.nodes._conditional import filter_

from hgraph.test import eval_node


def test_filter_():
    assert eval_node(filter_, [True, False, False, True, None], [1, 2, 3, None, 4]) == [1, None, None, 3, 4]

    
def test_filter_tsl():
    assert eval_node(filter_[TIME_SERIES_TYPE: TSL[TS[int], Size[2]]],
                     [True, False, None, True],
                     [(1, 1), (2, 2), {1: 3}, None, {0: 5}]) == \
                     [{0: 1, 1: 1}, None, None, {0: 2, 1: 3}, {0: 5}]


