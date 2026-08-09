import numpy as np

from hgraph import MIN_ST, MIN_TD, Array, Size, TS, TSB
from hgraph.nodes import (NpRollingWindowResult, np_quantile,
                          np_rolling_window, np_std)
from hgraph.test import eval_node


def test_rolling_window_result_generic_schema_retains_shape():
    resolved = TSB[NpRollingWindowResult[int, Size[3]]]
    assert "NpRollingWindowResult" in repr(resolved)


def test_np_rolling_window_uses_native_arrays():
    actual = eval_node(np_rolling_window, [1, 2, 3, 4], Size[3])
    assert actual[:2] == [None, None]
    np.testing.assert_array_equal(actual[2]["buffer"], np.array([1, 2, 3]))
    np.testing.assert_array_equal(actual[3]["buffer"], np.array([2, 3, 4]))
    np.testing.assert_array_equal(
        actual[2]["index"],
        np.array([MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]),
    )


def test_np_rolling_window_early_output_has_truthful_dynamic_shape():
    actual = eval_node(np_rolling_window, [1, 2, 3], Size[3], 2)
    assert actual[0] is None
    np.testing.assert_array_equal(actual[1]["buffer"], np.array([1, 2]))
    np.testing.assert_array_equal(actual[2]["buffer"], np.array([1, 2, 3]))


def test_np_quantile_and_std_delegate_to_native_operators():
    def quantile_graph(ts: TS[Array[int, Size[2]]]) -> TS[float]:
        return np_quantile(ts, 0.8)

    def std_graph(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return np_std(ts, 1)

    def population_std_graph(ts: TS[Array[int, Size[4]]]) -> TS[float]:
        return np_std(ts)

    assert eval_node(quantile_graph, [np.array([1, 2])]) == [1.8]
    assert eval_node(std_graph, [np.array([1, 2, 3, 4])]) == [
        np.std(np.array([1, 2, 3, 4]), ddof=1)
    ]
    assert eval_node(population_std_graph, [np.array([1, 2, 3, 4])]) == [
        np.std(np.array([1, 2, 3, 4]))
    ]


def test_np_std_uses_arrow_stable_variance_kernel():
    def std_graph(ts: TS[Array[float, Size[4]]]) -> TS[float]:
        return np_std(ts)

    values = np.array([1e16, 1e16, 1e16 + 2.0, 1e16 + 4.0])
    assert eval_node(std_graph, [values]) == [np.sqrt(5.0)]
