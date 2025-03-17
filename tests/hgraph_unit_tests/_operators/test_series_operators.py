import polars as pl
import pytest
from polars.testing import assert_series_equal

from hgraph import graph, TS, Series, NodeException
from hgraph.test import eval_node


def test_get_item_series():
    s = pl.Series("a", [1, 2, 3])

    @graph
    def g(ts: TS[Series[int]], index: int) -> TS[int]:
        return ts[index]

    assert eval_node(g, [s], 0) == [1]
    assert eval_node(g, [s], 2) == [3]
    with pytest.raises(NodeException):
        assert eval_node(g, [s], 4)


def test_get_item_series_ts():
    s = pl.Series("a", [1, 2, 3])

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

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [pl.Series(values=[2, 2, 2])])
    assert_series_equal(results[0], pl.Series(values=[2.0, 2.0, 2.0]))


def test_div_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [pl.Series(values=[2.0, 2.0, 2.0])])
    assert_series_equal(results[0], pl.Series(values=[2.0, 2.0, 2.0]))


def test_div_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [2])
    assert_series_equal(results[0], pl.Series(values=[2.0, 2.0, 2.0]))


def test_div_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [2.0])
    assert_series_equal(results[0], pl.Series(values=[2.0, 2.0, 2.0]))


def test_div_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [pl.Series(values=[2.0, 2.0, 2.0])])
    assert_series_equal(results[0], pl.Series(values=[2.0, 1.9, 1.75]))


def test_div_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs / rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [pl.Series(values=[2, 2, 2])])
    assert_series_equal(results[0], pl.Series(values=[2.0, 1.9, 1.75]))


def test_mul_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [pl.Series(values=[2, 2, 2])])
    assert_series_equal(results[0], pl.Series(values=[8, 8, 8]))


def test_mul_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [pl.Series(values=[2.0, 2.0, 2.0])])
    assert_series_equal(results[0], pl.Series(values=[8.0, 8.0, 8.0]))


def test_mul_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [2])
    assert_series_equal(results[0], pl.Series(values=[8, 8, 8]))


def test_mul_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4, 4, 4])], [2.0])
    assert_series_equal(results[0], pl.Series(values=[8.0, 8.0, 8.0]))


def test_mul_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [2])
    assert_series_equal(results[0], pl.Series(values=[8.0, 7.6, 7.0]))


def test_mul_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [2.0])
    assert_series_equal(results[0], pl.Series(values=[8.0, 7.6, 7.0]))


def test_mul_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [pl.Series(values=[2.0, 2.0, 2.0])])
    assert_series_equal(results[0], pl.Series(values=[8.0, 7.6, 7.0]))


def test_mul_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs * rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [pl.Series(values=[2, 2, 2])])
    assert_series_equal(results[0], pl.Series(values=[8.0, 7.6, 7.0]))


def test_sub_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [1.0])
    assert_series_equal(results[0], pl.Series(values=[3.0, 2.8, 2.5]))


def test_sub_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [1])
    assert_series_equal(results[0], pl.Series(values=[3.0, 2.8, 2.5]))


def test_sub_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [1])
    assert_series_equal(results[0], pl.Series(values=[3, 2, 2]))


def test_sub_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [1.0])
    assert_series_equal(results[0], pl.Series(values=[3.0, 2.0, 2.0]))


def test_sub_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [pl.Series(values=[2, 6, 1])])
    assert_series_equal(results[0], pl.Series(values=[2, -3, 2]))


def test_sub_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [pl.Series(values=[2.0, 6.0, 1.2])])
    assert_series_equal(results[0], pl.Series(values=[2.0, -3.0, 1.8]))


def test_sub_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4.2, 3.0, 3.1])], [pl.Series(values=[2.0, 6.0, 1.2])])
    assert_series_equal(results[0], pl.Series(values=[2.2, -3.0, 1.9]))


def test_sub_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs - rhs

    results = eval_node(g, [pl.Series(values=[4.2, 3.0, 3.1])], [pl.Series(values=[2, 6, 1])])
    assert_series_equal(results[0], pl.Series(values=[2.2, -3.0, 2.1]))


def test_add_series_float_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [1.0])
    assert_series_equal(results[0], pl.Series(values=[5.0, 4.8, 4.5]))


def test_add_series_float_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4.0, 3.8, 3.5])], [1])
    assert_series_equal(results[0], pl.Series(values=[5.0, 4.8, 4.5]))


def test_add_series_int_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[int]) -> TS[Series[int]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [1])
    assert_series_equal(results[0], pl.Series(values=[5, 4, 4]))


def test_add_series_int_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[float]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [1.0])
    assert_series_equal(results[0], pl.Series(values=[5.0, 4.0, 4.0]))


def test_add_series_int_series_int():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[int]]) -> TS[Series[int]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [pl.Series(values=[2, 6, 1])])
    assert_series_equal(results[0], pl.Series(values=[6, 9, 4]))


def test_add_series_int_series_float():
    @graph
    def g(lhs: TS[Series[int]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4, 3, 3])], [pl.Series(values=[2.0, 6.0, 1.2])])
    assert_series_equal(results[0], pl.Series(values=[6.0, 9.0, 4.2]))


def test_add_series_float_series_float():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[float]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4.2, 3.0, 3.1])], [pl.Series(values=[2.0, 6.0, 1.2])])
    assert_series_equal(results[0], pl.Series(values=[6.2, 9.0, 4.3]))


def test_add_series_float_series_int():
    @graph
    def g(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
        return lhs + rhs

    results = eval_node(g, [pl.Series(values=[4.2, 3.0, 3.1])], [pl.Series(values=[2, 6, 1])])
    assert_series_equal(results[0], pl.Series(values=[6.2, 9.0, 4.1]))
