import importlib
import math

import numpy as np
import pytest

import hgraph as hg
import hgraph.nodes as hg_nodes
import hgraph_analytics as hga


def test_rolling_window_result_generic_schema_retains_shape():
    resolved = hg.TSB[hga.RollingWindowResult[int, hg.Size[3]]]
    assert "RollingWindowResult" in repr(resolved)


def test_rolling_window_uses_native_arrays():
    actual = hg.eval_node(hga.rolling_window, [1, 2, 3, 4], hg.Size[3])
    assert actual[:2] == [None, None]
    np.testing.assert_array_equal(actual[2]["buffer"], np.array([1, 2, 3]))
    np.testing.assert_array_equal(actual[3]["buffer"], np.array([2, 3, 4]))
    np.testing.assert_array_equal(
        actual[2]["index"],
        np.array([hg.MIN_ST, hg.MIN_ST + hg.MIN_TD, hg.MIN_ST + hg.MIN_TD * 2]),
    )


def test_rolling_window_early_output_has_truthful_dynamic_shape():
    actual = hg.eval_node(hga.rolling_window, [1, 2, 3], hg.Size[3], 2)
    assert actual[0] is None
    np.testing.assert_array_equal(actual[1]["buffer"], np.array([1, 2]))
    np.testing.assert_array_equal(actual[2]["buffer"], np.array([1, 2, 3]))


@pytest.mark.parametrize(
    ("period", "minimum"), [(0, None), (-1, None), (3, -1), (3, 4)]
)
def test_rolling_window_rejects_invalid_sizes(period, minimum):
    args = (period,) if minimum is None else (period, minimum)
    with pytest.raises(hg.WiringError, match="to_window: .*period"):
        hg.eval_node(hga.rolling_window, [1, 2, 3], *args)


def test_quantile_accepts_arrays_and_tick_windows():
    @hg.graph
    def array_value(ts: hg.TS[hg.Array[int, hg.Size[4]]]) -> hg.TS[float]:
        return hga.quantile(ts, 0.5)

    @hg.graph
    def window_value(ts: hg.TS[int]) -> hg.TS[float]:
        return hga.quantile(hg.to_window(ts, 4), 0.5)

    assert hg.eval_node(array_value, [np.array([1, 2, 3, 4])]) == [2.5]
    assert hg.eval_node(window_value, [1, 2, 3, 4]) == [None, None, None, 2.5]


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
def test_quantile_interpolation_methods(method, expected):
    @hg.graph
    def app(ts: hg.TS[hg.Array[int, hg.Size[4]]]) -> hg.TS[float]:
        return hga.quantile(ts, 0.625, method=method)

    assert hg.eval_node(app, [np.array([1, 2, 3, 4])]) == [expected]


def test_array_std_honors_ddof_and_uses_stable_variance():
    @hg.graph
    def sample(ts: hg.TS[hg.Array[int, hg.Size[4]]]) -> hg.TS[float]:
        return hga.array_std(ts, 1)

    @hg.graph
    def stable(ts: hg.TS[hg.Array[float, hg.Size[4]]]) -> hg.TS[float]:
        return hga.array_std(ts)

    integer_values = np.array([1, 2, 3, 4])
    assert hg.eval_node(sample, [integer_values]) == [np.std(integer_values, ddof=1)]
    offset_values = np.array([1e16, 1e16, 1e16 + 2.0, 1e16 + 4.0])
    assert hg.eval_node(stable, [offset_values]) == [np.sqrt(5.0)]


def test_numpy_prefixed_compatibility_surface_is_deprecated():
    assert hg_nodes.np_rolling_window._deprecated
    assert hg_nodes.np_quantile._deprecated
    assert hg_nodes.np_std._deprecated
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("hgraph.numpy_")


def test_window_values_full_window():
    @hg.graph
    def app(ts: hg.TS[int]) -> hg.TS[hg.Array[int, hg.Size[3]]]:
        return hga.window_values(hg.to_window(ts, 3, 3))

    actual = hg.eval_node(app, [1, 2, 3])
    assert actual[:2] == [None, None]
    np.testing.assert_array_equal(actual[2], np.array([1, 2, 3]))


def test_window_values_uses_default_and_explicit_padding():
    @hg.graph
    def default(ts: hg.TS[int]) -> hg.TS[hg.Array[int, hg.Size[3]]]:
        return hga.window_values(hg.to_window(ts, 3, 2))

    @hg.graph
    def explicit(ts: hg.TS[int]) -> hg.TS[hg.Array[int, hg.Size[3]]]:
        return hga.window_values(hg.to_window(ts, 3, 2), -1)

    default_values = hg.eval_node(default, [1, 2, 3])
    assert default_values[0] is None
    np.testing.assert_array_equal(default_values[1], np.array([1, 2, 0]))
    np.testing.assert_array_equal(default_values[2], np.array([1, 2, 3]))
    explicit_values = hg.eval_node(explicit, [1, 2])
    assert explicit_values[0] is None
    np.testing.assert_array_equal(explicit_values[1], np.array([1, 2, -1]))


def test_array_get_item_selects_scalars_and_slices():
    @hg.graph
    def row(
        ts: hg.TS[hg.Array[int, hg.Size[3], hg.Size[2]]],
    ) -> hg.TS[hg.Array[int, hg.Size[2]]]:
        return hga.array_get_item(ts, 1)

    @hg.graph
    def item(ts: hg.TS[hg.Array[int, hg.Size[3], hg.Size[2]]]) -> hg.TS[int]:
        return hga.array_get_item(ts, (1, 0))

    @hg.graph
    def last(
        ts: hg.TS[hg.Array[int, hg.Size[3], hg.Size[2]]],
    ) -> hg.TS[hg.Array[int, hg.Size[2]]]:
        return hga.array_get_item(ts, -1)

    value = np.array([[1, 2], [3, 4], [5, 6]])
    np.testing.assert_array_equal(hg.eval_node(row, [value])[0], np.array([3, 4]))
    assert hg.eval_node(item, [value]) == [3]
    np.testing.assert_array_equal(hg.eval_node(last, [value])[0], np.array([5, 6]))


def test_array_get_item_supports_three_dimensions():
    @hg.graph
    def app(
        ts: hg.TS[hg.Array[int, hg.Size[2], hg.Size[2], hg.Size[2]]],
    ) -> hg.TS[hg.Array[int, hg.Size[2]]]:
        return hga.array_get_item(ts, (0, 1))

    value = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
    np.testing.assert_array_equal(hg.eval_node(app, [value])[0], np.array([3, 4]))


def test_cumulative_sum_supports_flattened_and_axis_accumulation():
    value = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])

    @hg.graph
    def flattened(
        ts: hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]],
    ) -> hg.TS[hg.Array[float, hg.Size[6]]]:
        return hga.cumulative_sum(ts)

    @hg.graph
    def axis_zero(
        ts: hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]],
    ) -> hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]]:
        return hga.cumulative_sum(ts, 0)

    @hg.graph
    def last_axis(
        ts: hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]],
    ) -> hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]]:
        return hga.cumulative_sum(ts, -1)

    np.testing.assert_array_equal(
        hg.eval_node(flattened, [value])[0],
        np.array([1.0, 3.0, 6.0, 10.0, 15.0, 21.0]),
    )
    np.testing.assert_array_equal(
        hg.eval_node(axis_zero, [value])[0],
        np.array([[1.0, 2.0, 3.0], [5.0, 7.0, 9.0]]),
    )
    np.testing.assert_array_equal(
        hg.eval_node(last_axis, [value])[0],
        np.array([[1.0, 3.0, 6.0], [4.0, 9.0, 15.0]]),
    )


def test_cumulative_sum_integer_overflow_has_defined_wrapping():
    @hg.graph
    def app(
        ts: hg.TS[hg.Array[int, hg.Size[2]]],
    ) -> hg.TS[hg.Array[int, hg.Size[2]]]:
        return hga.cumulative_sum(ts)

    value = np.array([np.iinfo(np.int64).max, 1], dtype=np.int64)
    np.testing.assert_array_equal(
        hg.eval_node(app, [value])[0],
        np.array([np.iinfo(np.int64).max, np.iinfo(np.int64).min], dtype=np.int64),
    )


def test_correlation_call_shapes():
    @hg.graph
    def scalar(ts: hg.TS[hg.Array[float, hg.Size[4]]]) -> hg.TS[float]:
        return hga.correlation(ts)

    @hg.graph
    def matrix(
        ts: hg.TS[hg.Array[float, hg.Size[2], hg.Size[4]]],
    ) -> hg.TS[hg.Array[float, hg.Size[2], hg.Size[2]]]:
        return hga.correlation(ts)

    @hg.graph
    def pair(
        ts: hg.TS[hg.Array[float, hg.Size[4]]],
    ) -> hg.TS[hg.Array[float, hg.Size[2], hg.Size[2]]]:
        return hga.correlation(ts, ts)

    @hg.graph
    def columns(
        ts: hg.TS[hg.Array[float, hg.Size[3], hg.Size[2]]],
    ) -> hg.TS[hg.Array[float, hg.Size[2], hg.Size[2]]]:
        return hga.correlation(ts, rowvar=False)

    vector = np.array([1.0, 2.0, 3.0, 4.0])
    assert hg.eval_node(scalar, [vector]) == [1.0]
    assert math.isnan(hg.eval_node(scalar, [np.ones(4)])[0])
    expected = np.ones((2, 2))
    np.testing.assert_allclose(
        hg.eval_node(matrix, [np.array([vector, vector])])[0], expected
    )
    np.testing.assert_allclose(hg.eval_node(pair, [vector])[0], expected)
    np.testing.assert_allclose(
        hg.eval_node(
            columns,
            [np.array([[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]])],
        )[0],
        expected,
    )
