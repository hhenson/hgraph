from hgraph import graph, TSD, TS, mean, diff, count
from hgraph._impl._operators._stream_analytical_operators import ewma
from hgraph.test import eval_node


def test_diff():
    assert eval_node(diff, [1, 2, 3, 4,]) == [None, 1, 1, 1]


def test_ewma():
    assert eval_node(ewma,
                     [1.0, 2.0, 3.0, 4.0, 3.0, 2.0, 1.0],
                     0.5) == [1.0, 1.5, 2.25, 3.125, 3.0625, 2.53125, 1.765625]


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


def test_count():
    expected = [
        1,
        2,
        3,
    ]

    assert eval_node(count, [3, 2, 1,], resolution_dict={'ts': TS[int]}) == expected
