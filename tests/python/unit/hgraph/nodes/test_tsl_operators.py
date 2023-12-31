from hgraph import Size, TS, TSL
from hgraph.nodes import merge
from hgraph.test import eval_node


def test_merge():
    assert eval_node(merge, [{1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}],
                     resolution_dict={"tsl": TSL[TS[int], Size[3]]}) == [1, 2, 4, None, 6]
