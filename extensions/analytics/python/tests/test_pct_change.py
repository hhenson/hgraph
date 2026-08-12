import math

import pytest

import hgraph as hg
import hgraph_analytics as hga


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
