from __future__ import annotations

import numpy as np
import pytest

import hgraph as hg
from hgraph.nodes import np_quantile, np_rolling_window, np_std
from hgraph.test import eval_node


def test_core_analytical_aliases_warn_and_delegate():
    with pytest.warns(DeprecationWarning, match=r"hgraph\.diff.*hgraph_analytics\.diff"):
        assert eval_node(hg.diff, [1, 2, 4]) == [None, 1, 2]

    with pytest.warns(
        DeprecationWarning,
        match=r"hgraph\.pct_change.*hgraph_analytics\.pct_change",
    ):
        assert eval_node(hg.pct_change, [100.0, 110.0, 121.0]) == [None, 0.1, 0.1]


def test_core_statistical_aliases_preserve_old_names():
    with pytest.warns(
        DeprecationWarning,
        match=r"hgraph\.rolling_average.*hgraph_analytics\.rolling_mean",
    ):
        assert eval_node(hg.rolling_average, [1, 2, 3, 4], 2) == [
            None,
            None,
            2.5,
            3.5,
        ]

    with pytest.warns(DeprecationWarning, match=r"hgraph\.std.*hgraph_analytics\.std"):
        assert eval_node(hg.std, [1.0, 2.0, 3.0]) == [
            0.0,
            0.5,
            pytest.approx(0.816496580927726),
        ]

    with pytest.warns(DeprecationWarning, match=r"hgraph\.var.*hgraph_analytics\.var"):
        assert eval_node(hg.var, [1.0, 2.0, 3.0]) == [0.0, 0.25, pytest.approx(2.0 / 3.0)]


def test_core_array_aliases_preserve_old_call_shapes():
    @hg.graph
    def array_ops(ts: hg.TS[hg.Array[int, hg.Size[4]]]):
        return {
            "item": hg.get_item(ts, -1),
            "sum": hg.cumsum(ts),
            "correlation": hg.corrcoef(ts),
            "quantile": hg.quantile(ts, 0.5, keepdims=True),
            "std": hg.np_std(ts, 0),
        }

    with pytest.warns(DeprecationWarning) as caught:
        actual = eval_node(array_ops, [np.array([1, 2, 3, 4])])[0]

    assert len(caught) == 5
    assert actual["item"] == 4
    np.testing.assert_array_equal(actual["sum"], np.array([1, 3, 6, 10]))
    assert actual["correlation"] == 1.0
    assert actual["quantile"] == 2.5
    assert actual["std"] == pytest.approx(np.std(np.array([1, 2, 3, 4])))


def test_core_node_np_aliases_warn_and_delegate():
    @hg.graph
    def quantile_graph(ts: hg.TS[hg.Array[int, hg.Size[4]]]) -> hg.TS[float]:
        return np_quantile(ts, 0.5, keepdims=True)

    @hg.graph
    def std_graph(ts: hg.TS[hg.Array[int, hg.Size[4]]]) -> hg.TS[float]:
        return np_std(ts, 1)

    with pytest.warns(DeprecationWarning, match="np_quantile"):
        assert eval_node(quantile_graph, [np.array([1, 2, 3, 4])]) == [2.5]
    with pytest.warns(DeprecationWarning, match="np_std"):
        assert eval_node(std_graph, [np.array([1, 2, 3, 4])]) == [
            pytest.approx(1.2909944487358056)
        ]
    with pytest.warns(DeprecationWarning, match="np_rolling_window"):
        actual = eval_node(np_rolling_window, [1, 2, 3], hg.Size[2])
    assert actual[0] is None
    np.testing.assert_array_equal(actual[1]["buffer"], np.array([1, 2]))
    np.testing.assert_array_equal(actual[2]["buffer"], np.array([2, 3]))


def test_scalar_ewma_conversions_warn_and_delegate():
    with pytest.warns(DeprecationWarning, match="center_of_mass_to_alpha"):
        assert hg.center_of_mass_to_alpha(1.0) == 0.5
    with pytest.warns(DeprecationWarning, match="span_to_alpha"):
        assert hg.span_to_alpha(1.0) == 1.0
