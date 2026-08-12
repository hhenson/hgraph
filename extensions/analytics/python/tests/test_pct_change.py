import math

import pytest

import hgraph as hg
import hgraph_analytics as hga


def test_migrated_analytical_helpers():
    assert hga.center_of_mass_to_alpha(1.0) == 0.5
    assert hga.span_to_alpha(1.0) == 1.0
    with pytest.raises(ValueError, match="Center of mass must be positive"):
        hga.center_of_mass_to_alpha(0.0)
    with pytest.raises(ValueError, match="Span must be positive"):
        hga.span_to_alpha(0.0)


def test_diff_count_clip_and_ewma():
    assert hg.eval_node(hga.diff, [1, 2, 4, 7]) == [None, 1, 2, 3]
    assert hg.eval_node(hga.count, [3, None, 2, 1]) == [1, None, 2, 3]
    assert hg.eval_node(
        hga.count,
        [3, 2, 1],
        reset=[None, True, None],
        resolution_dict={"ts": hg.TS[int], "reset": hg.TS[bool]},
    ) == [1, 1, 2]
    assert hg.eval_node(
        hga.count,
        [3, None, 2, 1],
        reset=[None, True, None],
        resolution_dict={"ts": hg.TS[int], "reset": hg.TS[bool]},
    ) == [1, None, 1, 2]
    assert hg.eval_node(hga.clip, [-1, 1, 3], 0, 2) == [0, 1, 2]
    assert hg.eval_node(hga.clip, [-1.0, 0.5, 2.0], 0.0, 1.0) == [
        0.0,
        0.5,
        1.0,
    ]
    assert hg.eval_node(hga.ewma, [1.0, 2.0, 3.0, 4.0], 0.5) == [
        1.0,
        1.5,
        2.25,
        3.125,
    ]


def test_count_accepts_a_mapped_dictionary_signal():
    @hg.graph
    def app(tsd: hg.TSD[int, hg.TS[int]]) -> hg.TS[int]:
        return hga.count(hg.map_(lambda value: value + 1, tsd))

    assert hg.eval_node(
        app,
        [{1: 10}, {2: 20}, None, {1: hg.REMOVE}],
    ) == [1, 2, None, 3]


def test_clip_rejects_reversed_bounds():
    with pytest.raises(Exception, match="min must be <= max"):
        hg.eval_node(hga.clip, [1.0], 1.0, -1.0)


def test_default_and_sparse_observations():
    assert hg.eval_node(hga.pct_change, [1, 2, 3]) == [None, 1.0, 0.5]
    assert hg.eval_node(hga.pct_change, [1, None, 2]) == [None, None, 1.0]


def test_longer_period_and_float_input():
    result = hg.eval_node(hga.pct_change, [10.0, 11.0, 12.0, 15.0], 2)
    assert result[:2] == [None, None]
    assert result[2:] == pytest.approx([0.2, 4.0 / 11.0])


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (hg.DivideByZero.NAN, math.nan),
        (hg.DivideByZero.ZERO, 0.0),
        (hg.DivideByZero.ONE, 1.0),
        (hg.DivideByZero.NONE, None),
    ],
)
def test_zero_denominator_policies(policy, expected):
    result = hg.eval_node(hga.pct_change, [0.0, 1.0, 2.0], 1, policy)
    assert result[0] is None
    if math.isnan(expected) if isinstance(expected, float) else False:
        assert math.isnan(result[1])
    else:
        assert result[1] == expected
    assert result[2] == 1.0


def test_zero_denominator_error_is_the_default():
    with pytest.raises(Exception, match="division by zero"):
        hg.eval_node(hga.pct_change, [0.0, 1.0])


@pytest.mark.parametrize("period", [0, -1])
def test_period_must_be_positive(period):
    with pytest.raises(hg.WiringError, match="period must be positive"):
        hg.eval_node(hga.pct_change, [1.0, 2.0], period)


def test_public_signature_and_fractional_units():
    assert hga.pct_change.__name__ == "pct_change"
    assert hg.eval_node(hga.pct_change, [100.0, 105.0]) == [None, 0.05]
