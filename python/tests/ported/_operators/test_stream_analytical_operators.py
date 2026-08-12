import pytest

from hgraph import graph, TSD, TS, mean
from hgraph.test import eval_node


def test_mean_tsd_int():
    @graph
    def app(tsd: TSD[int, TS[int]]) -> TS[float]:
        return mean(tsd)

    assert eval_node(app, [{1: 10, 2: 20}]) == [15.0]


def test_mean_tsd_float():
    @graph
    def app(tsd: TSD[int, TS[float]]) -> TS[float]:
        return mean(tsd)

    assert eval_node(app, [{1: 10.0, 2: 20.0}]) == [15.0]
