from hgraph import TIME_SERIES_TYPE, TS, graph, TSL, compute_node, REF, PythonTimeSeriesReference
from hgraph.nodes import valid
from hgraph.test import eval_node


def test_valid():
    assert eval_node(valid[TIME_SERIES_TYPE: TS[int]], [None, 1]) == [False, True]


def test_valid_1():
    @compute_node
    def make_ref(a: REF[TS[int]], b: REF[TS[int]], i: TS[int]) -> REF[TS[int]]:
        return [a.value, b.value, PythonTimeSeriesReference()][i.value]

    @graph
    def g(a: TS[int], b: TS[int], i: TS[int]) -> TS[bool]:
        return valid[TIME_SERIES_TYPE: TS[int]](make_ref(a, b, i))
    assert eval_node(g,
                     a=[None, None, None, None,    1],
                     b=[None, None,    1, None, None],
                     i=[2,    1,    None,    0, None, 2]) == \
                       [False, False, True, False, True, False]
