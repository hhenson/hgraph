from datetime import timedelta
import math

import pytest
from frozendict import frozendict

import hgraph as hg
import hgraph_analytics as hga


def test_running_std_and_var_use_analytics_registry():
    @hg.graph
    def running_std(ts: hg.TS[int]) -> hg.TS[float]:
        return hga.std(ts)

    @hg.graph
    def running_var(ts: hg.TS[int]) -> hg.TS[float]:
        return hga.var(ts)

    assert hg.eval_node(running_std, [1, 2, 3, 5]) == pytest.approx(
        [0.0, 0.5, math.sqrt(2.0 / 3.0), math.sqrt(35.0 / 16.0)]
    )
    assert hg.eval_node(running_var, [1, 2, 3]) == pytest.approx(
        [0.0, 0.25, 2.0 / 3.0]
    )


def test_std_reduces_a_core_window_with_explicit_ddof():
    @hg.graph
    def window_std(ts: hg.TS[int]) -> hg.TS[float]:
        return hga.std(hg.to_window(ts, 5, 3), ddof=1)

    actual = hg.eval_node(window_std, [1, 2, 3, 4, 5])
    assert actual[:2] == [None, None]
    assert actual[2:] == pytest.approx(
        [1.0, math.sqrt(5.0 / 3.0), math.sqrt(2.5)]
    )


def test_binary_dispersion_preserves_migrated_contract():
    @hg.graph
    def binary_std(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[float]:
        return hga.std(lhs, rhs)

    @hg.graph
    def binary_var(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[float]:
        return hga.var(lhs, rhs)

    assert hg.eval_node(binary_std, [1, 2], [2, 3]) == pytest.approx(
        [math.sqrt(0.5), math.sqrt(0.5)]
    )
    assert hg.eval_node(binary_var, [1, 2], [2, 3]) == [0.5, 0.5]


def test_scalar_container_dispersion_preserves_collection_contracts():
    @hg.graph
    def tuple_std(ts: hg.TS[tuple[int, ...]]) -> hg.TS[float]:
        return hga.std(ts)

    @hg.graph
    def set_var(ts: hg.TS[frozenset[float]]) -> hg.TS[float]:
        return hga.var(ts)

    @hg.graph
    def mapping_std(ts: hg.TS[frozendict[int, int]]) -> hg.TS[float]:
        return hga.std(ts)

    assert hg.eval_node(tuple_std, [(1, 2, 3), ()]) == [1.0, 0.0]
    assert hg.eval_node(
        set_var,
        [frozenset(), frozenset({1.0, 2.0}), frozenset({1.0, 2.0, 3.0, 5.0})],
    ) == pytest.approx([0.0, 0.5, 35.0 / 12.0])

    base = 2**40
    assert hg.eval_node(
        mapping_std,
        [frozendict({1: base, 2: base + 10, 3: base + 20})],
    ) == pytest.approx([10.0])


def test_time_series_collection_dispersion_preserves_shape():
    class ABSchema(hg.TimeSeriesSchema):
        a: hg.TS[float]
        b: hg.TS[float]

    @hg.graph
    def set_std(ts: hg.TSS[int]) -> hg.TS[float]:
        return hga.std(ts)

    @hg.graph
    def list_std(
        lhs: hg.TSL[hg.TS[float], hg.Size[2]],
        rhs: hg.TSL[hg.TS[float], hg.Size[2]],
    ) -> hg.TSL[hg.TS[float], hg.Size[2]]:
        return hga.std(lhs, rhs)

    @hg.graph
    def bundle_std(lhs: hg.TSB[ABSchema], rhs: hg.TSB[ABSchema]) -> hg.TSB[ABSchema]:
        return hga.std(lhs, rhs)

    assert hg.eval_node(set_std, [set(), {1}, {1, 2}, {1, 2, -1, 3}]) == pytest.approx(
        [0.0, 0.0, math.sqrt(0.5), math.sqrt(35.0 / 12.0)]
    )
    assert hg.eval_node(list_std, [(1.0, 2.0)], [(2.0, 3.0)]) == [
        {0: math.sqrt(0.5), 1: math.sqrt(0.5)}
    ]
    assert hg.eval_node(
        bundle_std,
        [{"a": 7.0, "b": 8.0}],
        [{"a": 5.0, "b": 9.0}],
    ) == [{"a": math.sqrt(2.0), "b": math.sqrt(0.5)}]


def test_rolling_mean_supports_tick_and_duration_periods():
    assert hg.eval_node(hga.rolling_mean, [1, 2, 3, 4, 5], 3, 2) == [
        None,
        1.5,
        2.0,
        3.0,
        4.0,
    ]

    duration = hg.eval_node(
        hga.rolling_mean, [1, 2, 3, 4, 5], hg.MIN_TD * 3
    )
    assert duration[:-1] == [None, None, None, 3.0, 4.0, 4.5, 5.0]
    assert math.isnan(duration[-1])


def test_resample_reticks_the_latest_value():
    @hg.graph
    def sampled(ts: hg.TS[int], period: timedelta) -> hg.TS[int]:
        return hga.resample(ts, period)

    assert hg.eval_node(
        sampled,
        [1, 2, 3, 4, 5, 6],
        2 * hg.MIN_TD,
        __end_time__=hg.MIN_ST + 10 * hg.MIN_TD,
    ) == [None, None, 3, None, 5, None, 6, None, 6]


def test_statistics_are_no_longer_exported_from_core():
    assert not hasattr(hg, "std")
    assert not hasattr(hg, "var")
    assert not hasattr(hg, "rolling_average")
    assert not hasattr(hg, "resample")
