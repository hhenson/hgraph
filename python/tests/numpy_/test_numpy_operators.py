import math

import numpy as np
import pytest

from hgraph import Array, Size, TS, WindowSize, compute_node, graph, to_window
from hgraph.numpy_ import (ARRAY, add_docs, as_array, corrcoef, cumsum,
                           extract_dimensions_from_array,
                           extract_type_from_array, get_item, quantile)
from hgraph.test import eval_node


def test_array_type_retains_element_and_dimensions():
    annotation = Array[float, Size[3], Size[2]]
    assert extract_type_from_array(annotation) is float
    assert extract_dimensions_from_array(annotation) == (3, 2)
    assert annotation == Array[float, Size[3], Size[2]]
    assert hash(annotation) == hash(Array[float, Size[3], Size[2]])
    assert extract_dimensions_from_array(Array[int, Size[-1]]) == (-1,)
    assert "Size[-1]" in repr(Array[int, Size[-1]])


def test_add_docs_is_part_of_the_public_numpy_surface():
    def source():
        "source docs"

    @add_docs(source)
    def target():
        pass

    assert "source docs" in target.__doc__


def test_fixed_array_capacity_accepts_a_shorter_logical_value():
    @compute_node
    def passthrough(ts: TS[Array[int, Size[3]]]) -> TS[Array[int, Size[3]]]:
        return ts.value

    values = [np.array([1]), np.array([1, 2]), np.array([1, 2, 3])]
    actual = eval_node(passthrough, values)
    assert all(np.array_equal(a, b) for a, b in zip(actual, values))

    with pytest.raises(ValueError, match="at most 3 elements"):
        eval_node(passthrough, [np.array([1, 2, 3, 4])])


def test_as_array_full_window():
    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        return as_array(to_window(ts, 3, 3))

    actual = eval_node(g, [1, 2, 3])
    assert actual[:2] == [None, None]
    np.testing.assert_array_equal(actual[2], np.array([1, 2, 3]))


def test_as_array_min_size_uses_native_default_zero():
    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        return as_array(to_window(ts, 3, 2))

    actual = eval_node(g, [1, 2, 3])
    assert actual[0] is None
    np.testing.assert_array_equal(actual[1], np.array([1, 2, 0]))
    np.testing.assert_array_equal(actual[2], np.array([1, 2, 3]))


def test_as_array_accepts_an_explicit_zero():
    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        return as_array(to_window(ts, 3, 2), -1)

    actual = eval_node(g, [1, 2])
    assert actual[0] is None
    np.testing.assert_array_equal(actual[1], np.array([1, 2, -1]))


def test_get_item_scalar_and_slices():
    @graph
    def row(ts: TS[Array[int, Size[3], Size[2]]]) -> TS[Array[int, Size[2]]]:
        return get_item(ts, 1)

    @graph
    def item(ts: TS[Array[int, Size[3], Size[2]]]) -> TS[int]:
        return get_item(ts, (1, 0))

    value = np.array([[1, 2], [3, 4], [5, 6]])
    np.testing.assert_array_equal(eval_node(row, [value])[0], np.array([3, 4]))
    assert eval_node(item, [value]) == [3]

    @graph
    def last(ts: TS[Array[int, Size[3], Size[2]]]) -> TS[Array[int, Size[2]]]:
        return get_item(ts, -1)

    np.testing.assert_array_equal(eval_node(last, [value])[0], np.array([5, 6]))


def test_get_item_three_dimensions():
    @graph
    def g(ts: TS[Array[int, Size[2], Size[2], Size[2]]]) -> TS[Array[int, Size[2]]]:
        return get_item(ts, (0, 1))

    value = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
    np.testing.assert_array_equal(eval_node(g, [value])[0], np.array([3, 4]))


def test_cumsum_flattened_and_axis():
    value = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])

    @graph
    def flattened(ts: TS[Array[float, Size[2], Size[3]]]) -> TS[Array[float, Size[6]]]:
        return cumsum(ts)

    @graph
    def axis_zero(ts: TS[Array[float, Size[2], Size[3]]]) -> TS[Array[float, Size[2], Size[3]]]:
        return cumsum(ts, 0)

    @graph
    def last_axis(ts: TS[Array[float, Size[2], Size[3]]]) -> TS[Array[float, Size[2], Size[3]]]:
        return cumsum(ts, -1)

    assert np.array_equal(
        eval_node(flattened, [value])[0],
        np.array([1.0, 3.0, 6.0, 10.0, 15.0, 21.0]),
    )
    assert np.array_equal(
        eval_node(axis_zero, [value])[0],
        np.array([[1.0, 2.0, 3.0], [5.0, 7.0, 9.0]]),
    )
    assert np.array_equal(
        eval_node(last_axis, [value])[0],
        np.array([[1.0, 3.0, 6.0], [4.0, 9.0, 15.0]]),
    )


def test_cumsum_upstream_specialization_call_shape():
    actual = eval_node(
        cumsum[ARRAY: Array[float, Size[3]]],
        [np.array([1, 2, 3])],
    )
    np.testing.assert_array_equal(actual[0], np.array([1.0, 3.0, 6.0]))


def test_cumsum_integer_overflow_has_defined_wrapping():
    @graph
    def g(ts: TS[Array[int, Size[2]]]) -> TS[Array[int, Size[2]]]:
        return cumsum(ts)

    value = np.array([np.iinfo(np.int64).max, 1], dtype=np.int64)
    np.testing.assert_array_equal(
        eval_node(g, [value])[0],
        np.array([np.iinfo(np.int64).max, np.iinfo(np.int64).min], dtype=np.int64),
    )


def test_corrcoef_call_shapes():
    @graph
    def scalar(ts: TS[Array[float, Size[4]]]) -> TS[float]:
        return corrcoef(ts)

    @graph
    def matrix(ts: TS[Array[float, Size[2], Size[4]]]) -> TS[Array[float, Size[2], Size[2]]]:
        return corrcoef(ts)

    @graph
    def pair(ts: TS[Array[float, Size[4]]]) -> TS[Array[float, Size[2], Size[2]]]:
        return corrcoef(ts, ts)

    @graph
    def columns(ts: TS[Array[float, Size[3], Size[2]]]) -> TS[Array[float, Size[2], Size[2]]]:
        return corrcoef(ts, rowvar=False)

    vector = np.array([1.0, 2.0, 3.0, 4.0])
    assert eval_node(scalar, [vector]) == [1.0]
    assert math.isnan(eval_node(scalar, [np.ones(4)])[0])
    expected = np.ones((2, 2))
    np.testing.assert_allclose(
        eval_node(matrix, [np.array([vector, vector])])[0], expected)
    np.testing.assert_allclose(eval_node(pair, [vector])[0], expected)
    np.testing.assert_allclose(
        eval_node(columns, [np.array([[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]])])[0],
        expected,
    )


def test_quantile_array_and_window():
    @graph
    def array_value(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return quantile(ts, 0.5)

    @graph
    def window_value(ts: TS[int]) -> TS[float]:
        return quantile(to_window(ts, 4), 0.5)

    assert eval_node(array_value, [np.array([1, 2, 3, 4])]) == [2.5]
    assert eval_node(window_value, [1, 2, 3, 4]) == [None, None, None, 2.5]


def test_quantile_method_and_keepdims_call_shapes():
    @graph
    def lower(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return quantile(ts, 0.5, method="lower")

    @graph
    def keepdims(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return quantile(ts, 0.5, keepdims=True)

    value = np.array([1, 2, 3, 4])
    assert eval_node(lower, [value]) == [2.0]
    assert eval_node(keepdims, [value]) == [2.5]


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("linear", 2.875),
        ("lower", 2.0),
        ("higher", 3.0),
        ("midpoint", 2.5),
        ("nearest", 3.0),
    ],
)
def test_quantile_uses_arrow_interpolation(method, expected):
    @graph
    def g(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return quantile(ts, 0.625, method=method)

    assert eval_node(g, [np.array([1, 2, 3, 4])]) == [expected]
