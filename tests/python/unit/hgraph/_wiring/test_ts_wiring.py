import pytest

from hgraph import compute_node, TS, graph, IncorrectTypeBinding
from hgraph.test import eval_node


def test_ts_type_mismatch():
    @compute_node
    def n(ts: TS[int]) -> TS[int]:
        return ts.value

    @compute_node
    def f(ts: TS[float]) -> TS[float]:
        return ts.value

    @graph
    def g(ts: TS[float]) -> TS[int]:
        return n(f(ts))

    with pytest.raises(IncorrectTypeBinding):
        assert eval_node(g, [1.0]) == [1.0]
