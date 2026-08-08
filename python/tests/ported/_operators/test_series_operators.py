from datetime import date, datetime

import pyarrow as pa
import pytest

from hgraph import contains_, graph, TS, Series
NodeException = Exception
from hgraph.test import eval_node

import pytest as _pytest_mark


def _series_eq(actual, expected):
    a = actual.to_pylist()
    e = expected.to_pylist()
    assert len(a) == len(e), f"{a} != {e}"
    for x, y in zip(a, e):
        if isinstance(y, float) or isinstance(x, float):
            assert abs((x or 0) - (y or 0)) < 1e-9, f"{a} != {e}"
        else:
            assert x == y, f"{a} != {e}"




def test_get_item_series():
    s = pa.array([1, 2, 3])

    @graph
    def g(ts: TS[Series[int]], index: int) -> TS[int]:
        return ts[index]

    assert eval_node(g, [s], 0) == [1]
    assert eval_node(g, [s], 2) == [3]
    with pytest.raises(NodeException):
        assert eval_node(g, [s], 4)


def test_get_item_series_ts():
    s = pa.array([1, 2, 3])

    @graph
    def g(ts: TS[Series[int]], index: TS[int]) -> TS[int]:
        return ts[index]

    assert eval_node(g, [s], [0, 2]) == [1, 3]
    with pytest.raises(NodeException):
        assert eval_node(g, [s], [4])


def test_div_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [pa.array([2, 2, 2])])
    _series_eq(results[0], pa.array([2.0, 2.0, 2.0]))


def test_div_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [pa.array([2.0, 2.0, 2.0])])
    _series_eq(results[0], pa.array([2.0, 2.0, 2.0]))


def test_div_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [2])
    _series_eq(results[0], pa.array([2.0, 2.0, 2.0]))


def test_div_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [2.0])
    _series_eq(results[0], pa.array([2.0, 2.0, 2.0]))


def test_div_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [pa.array([2.0, 2.0, 2.0])])
    _series_eq(results[0], pa.array([2.0, 1.9, 1.75]))


def test_div_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [pa.array([2, 2, 2])])
    _series_eq(results[0], pa.array([2.0, 1.9, 1.75]))


def test_mul_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [pa.array([2, 2, 2])])
    _series_eq(results[0], pa.array([8, 8, 8]))


def test_mul_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [pa.array([2.0, 2.0, 2.0])])
    _series_eq(results[0], pa.array([8.0, 8.0, 8.0]))


def test_mul_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [2])
    _series_eq(results[0], pa.array([8, 8, 8]))


def test_mul_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4, 4, 4])], [2.0])
    _series_eq(results[0], pa.array([8.0, 8.0, 8.0]))


def test_mul_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [2])
    _series_eq(results[0], pa.array([8.0, 7.6, 7.0]))


def test_mul_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [2.0])
    _series_eq(results[0], pa.array([8.0, 7.6, 7.0]))


def test_mul_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [pa.array([2.0, 2.0, 2.0])])
    _series_eq(results[0], pa.array([8.0, 7.6, 7.0]))


def test_mul_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [pa.array([2, 2, 2])])
    _series_eq(results[0], pa.array([8.0, 7.6, 7.0]))


def test_sub_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [1.0])
    _series_eq(results[0], pa.array([3.0, 2.8, 2.5]))


def test_sub_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [1])
    _series_eq(results[0], pa.array([3.0, 2.8, 2.5]))


def test_sub_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [1])
    _series_eq(results[0], pa.array([3, 2, 2]))


def test_sub_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [1.0])
    _series_eq(results[0], pa.array([3.0, 2.0, 2.0]))


def test_sub_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [pa.array([2, 6, 1])])
    _series_eq(results[0], pa.array([2, -3, 2]))


def test_sub_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [pa.array([2.0, 6.0, 1.2])])
    _series_eq(results[0], pa.array([2.0, -3.0, 1.8]))


def test_sub_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4.2, 3.0, 3.1])], [pa.array([2.0, 6.0, 1.2])])
    _series_eq(results[0], pa.array([2.2, -3.0, 1.9]))


def test_sub_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pa.array([4.2, 3.0, 3.1])], [pa.array([2, 6, 1])])
    _series_eq(results[0], pa.array([2.2, -3.0, 2.1]))


def test_add_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [1.0])
    _series_eq(results[0], pa.array([5.0, 4.8, 4.5]))


def test_add_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4.0, 3.8, 3.5])], [1])
    _series_eq(results[0], pa.array([5.0, 4.8, 4.5]))


def test_add_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [1])
    _series_eq(results[0], pa.array([5, 4, 4]))


def test_add_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [1.0])
    _series_eq(results[0], pa.array([5.0, 4.0, 4.0]))


def test_add_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [pa.array([2, 6, 1])])
    _series_eq(results[0], pa.array([6, 9, 4]))


def test_add_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4, 3, 3])], [pa.array([2.0, 6.0, 1.2])])
    _series_eq(results[0], pa.array([6.0, 9.0, 4.2]))


def test_add_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4.2, 3.0, 3.1])], [pa.array([2.0, 6.0, 1.2])])
    _series_eq(results[0], pa.array([6.2, 9.0, 4.3]))


def test_add_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pa.array([4.2, 3.0, 3.1])], [pa.array([2, 6, 1])])
    _series_eq(results[0], pa.array([6.2, 9.0, 4.1]))


def test_series_contains():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[bool]:
        return contains_(lhs, rhs)

    results = eval_node(g, [pa.array([4, 3, 3])], [3, 6, 4])
    assert results == [True, False, True]
