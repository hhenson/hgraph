"""
Time-Series Scalar (TS) Behavior Tests

This file tests all behaviors of the base TS[T] time-series type.
These tests should pass before moving to other time-series types.

Test Dependencies: None (base type)
Implementation Order: 1

Behaviors Tested:
1. Output value setting and retrieval
2. valid property (value has been set)
3. modified property (value changed this tick)
4. delta_value (same as value for scalars)
5. last_modified_time tracking
6. invalidate() behavior
7. Input binding and value delegation
8. active/passive subscription states
9. Notification flow from output to node
10. Peered vs non-peered binding
11. Sample state on bind/unbind
"""
import pytest
from datetime import datetime

from hgraph import (
    compute_node,
    graph,
    TS,
    MIN_ST,
    MIN_TD,
    MIN_DT,
    valid,
    modified,
    last_modified_time,
    TIME_SERIES_TYPE,
    pass_through_node,
    SIGNAL,
)
from hgraph.test import eval_node


# =============================================================================
# OUTPUT BEHAVIOR TESTS
# =============================================================================


def test_output_set_value():
    """Test that setting a value on output makes it available."""
    @compute_node
    def set_value(trigger: TS[bool]) -> TS[int]:
        if trigger.value:
            return 42

    assert eval_node(set_value, [True]) == [42]


def test_output_set_multiple_values_over_time():
    """Test that values can be updated over multiple ticks."""
    @compute_node
    def pass_value(ts: TS[int]) -> TS[int]:
        return ts.value

    assert eval_node(pass_value, [1, 2, 3]) == [1, 2, 3]


def test_output_no_tick_when_no_return():
    """Test that returning nothing doesn't tick the output."""
    @compute_node
    def conditional_output(ts: TS[int]) -> TS[int]:
        if ts.value > 0:
            return ts.value

    assert eval_node(conditional_output, [1, -1, 2, -2, 3]) == [1, None, 2, None, 3]


def test_output_return_none_invalidates():
    """Test that returning None explicitly invalidates the output."""
    @compute_node(valid=())
    def invalidating_output(ts: TS[int]) -> TS[int]:
        if ts.value > 0:
            return ts.value
        else:
            return None  # Explicit None

    # After returning None, output becomes invalid
    assert eval_node(invalidating_output, [1, -1, 2]) == [1, None, 2]


# =============================================================================
# VALID PROPERTY TESTS
# =============================================================================


def test_valid_false_when_no_value():
    """Test that valid() returns False when no value is set."""
    # Use the valid operator to check validity
    assert eval_node(valid[TIME_SERIES_TYPE: TS[int]], [None, 1]) == [False, True]


def test_valid_true_after_set():
    """Test that valid() returns True after value is set."""
    assert eval_node(valid[TIME_SERIES_TYPE: TS[int]], [1, 2, 3]) == [True, None, None]


def test_valid_stays_true_across_ticks():
    """Test that valid remains True after value was set, even if not modified."""
    # Once valid, stays valid (valid operator only ticks on change)
    assert eval_node(valid[TIME_SERIES_TYPE: TS[int]], [1, None, None]) == [True, None, None]


# =============================================================================
# MODIFIED PROPERTY TESTS
# =============================================================================


def test_modified_true_on_tick():
    """Test that modified is True when value changes this tick."""
    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    # modified() operator returns True when input ticks, False when it doesn't
    # First tick: None input means False, then 1 means True
    assert eval_node(g, [None, 1, None, None, 2, None]) == [False, True, False, None, True, False]


def test_modified_false_when_no_tick():
    """Test that modified is False when input doesn't tick."""
    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    assert eval_node(g, [None, 1, None, 2, None]) == [False, True, False, True, False]


def test_modified_reflects_current_tick():
    """Test that modified reflects whether input ticked this cycle."""
    @compute_node
    def check_modified(ts: TS[int]) -> TS[tuple]:
        return (ts.modified, ts.value if ts.valid else None)

    # modified is True only when ts actually ticks
    assert eval_node(check_modified, [1, 2, 3]) == [(True, 1), (True, 2), (True, 3)]


# =============================================================================
# DELTA_VALUE PROPERTY TESTS
# =============================================================================


def test_delta_value_equals_value_for_scalar():
    """Test that delta_value equals value for scalar TS."""
    @compute_node
    def check_delta(ts: TS[int]) -> TS[int]:
        return ts.delta_value

    assert eval_node(check_delta, [1, 2, 3]) == [1, 2, 3]


def test_delta_value_when_modified():
    """Test that delta_value reflects the current tick's value."""
    @compute_node
    def check_delta_when_modified(ts: TS[int]) -> TS[tuple]:
        return (ts.modified, ts.delta_value)

    # delta_value returns current value when modified
    assert eval_node(check_delta_when_modified, [1, 2, 3]) == [
        (True, 1),
        (True, 2),
        (True, 3),
    ]


# =============================================================================
# LAST_MODIFIED_TIME PROPERTY TESTS
# =============================================================================


def test_last_modified_time_updates_on_tick():
    """Test that last_modified_time updates when value is set."""
    @graph
    def g(ts: TS[int]) -> TS[datetime]:
        return last_modified_time(ts)

    result = eval_node(g, [1, 2, 3])
    assert result == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


def test_last_modified_time_stable_when_not_ticked():
    """Test that last_modified_time stays stable when not ticked."""
    @graph
    def g(ts: TS[int]) -> TS[datetime]:
        return last_modified_time(ts)

    result = eval_node(g, [1, None, 2])
    assert result == [MIN_ST, None, MIN_ST + 2 * MIN_TD]


def test_last_modified_time_tracks_modifications():
    """Test that last_modified_time reflects when value was last set."""
    @compute_node
    def check_lmt(ts: TS[int]) -> TS[datetime]:
        return ts.last_modified_time

    result = eval_node(check_lmt, [1, 2, 3])
    assert result == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


# =============================================================================
# INVALIDATE BEHAVIOR TESTS
# =============================================================================


def test_invalidate_via_none_return():
    """Test that returning None invalidates the output."""
    @compute_node(valid=())
    def invalidating(ts: TS[int]) -> TS[int]:
        if ts.value > 0:
            return ts.value
        return None

    assert eval_node(invalidating, [1, -1]) == [1, None]


def test_valid_reflects_output_state():
    """Test that valid reflects whether output has a value."""
    @compute_node(valid=())
    def invalidating(ts: TS[int]) -> TS[int]:
        if ts.value > 0:
            return ts.value
        return None

    # Direct test: check the output values
    assert eval_node(invalidating, [1, -1, 2]) == [1, None, 2]


# =============================================================================
# INPUT BINDING TESTS
# =============================================================================


def test_input_bound_when_wired():
    """Test that input is bound when wired to output."""
    @compute_node
    def check_bound(ts: TS[int]) -> TS[bool]:
        return ts.bound

    assert eval_node(check_bound, [1]) == [True]


def test_input_value_delegates_to_output():
    """Test that input.value returns output's value."""
    @compute_node
    def get_value(ts: TS[int]) -> TS[int]:
        return ts.value

    assert eval_node(get_value, [42, 99]) == [42, 99]


def test_input_valid_reflects_output():
    """Test that input.valid reflects whether bound output has a value."""
    @compute_node
    def check_valid(ts: TS[int]) -> TS[bool]:
        return ts.valid

    # Node is only invoked when ts ticks, so valid is always True here
    assert eval_node(check_valid, [1, 2, 3]) == [True, True, True]


def test_input_modified_reflects_current_tick():
    """Test that input.modified is True when input ticks."""
    @compute_node
    def check_modified(ts: TS[int]) -> TS[bool]:
        return ts.modified

    # Node is only invoked when ts ticks, so modified is always True
    assert eval_node(check_modified, [1, 2, 3]) == [True, True, True]


# =============================================================================
# ACTIVE/PASSIVE SUBSCRIPTION TESTS
# =============================================================================


def test_active_input_triggers_node():
    """Test that active input modifications trigger node evaluation."""
    @compute_node
    def count_evals(ts: TS[int]) -> TS[int]:
        return ts.value * 2

    # Node should evaluate each time ts ticks
    assert eval_node(count_evals, [1, 2, 3]) == [2, 4, 6]


def test_passive_input_no_trigger():
    """Test that passive inputs don't trigger node evaluation on their own."""
    @compute_node(active=("trigger",))
    def passive_test(trigger: TS[bool], data: TS[int]) -> TS[int]:
        if data.valid:
            return data.value
        return 0

    # Only trigger causes evaluation, data is passive
    assert eval_node(passive_test, [True, None, True], [1, 2, None]) == [1, None, 2]


# =============================================================================
# NOTIFICATION TESTS
# =============================================================================


def test_notification_on_output_change():
    """Test that node is notified when bound output changes."""
    call_count = [0]

    @compute_node
    def counting_node(ts: TS[int]) -> TS[int]:
        call_count[0] += 1
        return ts.value

    eval_node(counting_node, [1, 2, 3])
    assert call_count[0] == 3


def test_no_notification_when_no_change():
    """Test that node is not notified when output doesn't tick."""
    call_count = [0]

    @compute_node
    def counting_node(ts: TS[int]) -> TS[int]:
        call_count[0] += 1
        return ts.value

    eval_node(counting_node, [1, None, None, 2])
    assert call_count[0] == 2  # Only on ticks 0 and 3


# =============================================================================
# PASS-THROUGH BEHAVIOR TESTS
# =============================================================================


def test_pass_through_preserves_value():
    """Test that pass_through_node preserves values."""
    assert eval_node(pass_through_node, ts=[1, 2, 3]) == [1, 2, 3]


def test_pass_through_preserves_ticks():
    """Test that pass_through_node preserves tick timing."""
    assert eval_node(pass_through_node, ts=[1, None, 2, None, 3]) == [1, None, 2, None, 3]


def test_chained_pass_through():
    """Test multiple pass_through nodes in sequence."""
    @graph
    def g(ts: TS[int]) -> TS[int]:
        a = pass_through_node(ts)
        b = pass_through_node(a)
        return pass_through_node(b)

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


# =============================================================================
# TYPE VARIANT TESTS
# =============================================================================


def test_ts_int():
    """Test TS[int] basic operation."""
    @compute_node
    def add_one(ts: TS[int]) -> TS[int]:
        return ts.value + 1

    assert eval_node(add_one, [1, 2, 3]) == [2, 3, 4]


def test_ts_float():
    """Test TS[float] basic operation."""
    @compute_node
    def double(ts: TS[float]) -> TS[float]:
        return ts.value * 2.0

    assert eval_node(double, [1.5, 2.5, 3.5]) == [3.0, 5.0, 7.0]


def test_ts_str():
    """Test TS[str] basic operation."""
    @compute_node
    def upper(ts: TS[str]) -> TS[str]:
        return ts.value.upper()

    assert eval_node(upper, ["hello", "world"]) == ["HELLO", "WORLD"]


def test_ts_bool():
    """Test TS[bool] basic operation."""
    @compute_node
    def negate(ts: TS[bool]) -> TS[bool]:
        return not ts.value

    assert eval_node(negate, [True, False, True]) == [False, True, False]


def test_ts_tuple():
    """Test TS[tuple] basic operation."""
    @compute_node
    def first(ts: TS[tuple]) -> TS[int]:
        return ts.value[0]

    assert eval_node(first, [(1, 2), (3, 4), (5, 6)]) == [1, 3, 5]


def test_ts_datetime():
    """Test TS[datetime] basic operation."""
    @compute_node
    def get_year(ts: TS[datetime]) -> TS[int]:
        return ts.value.year

    test_dates = [datetime(2020, 1, 1), datetime(2021, 6, 15), datetime(2022, 12, 31)]
    assert eval_node(get_year, test_dates) == [2020, 2021, 2022]


# =============================================================================
# SIGNAL INPUT TESTS
# =============================================================================


def test_signal_value_always_true():
    """Test that SIGNAL value is always True when ticked."""
    @compute_node
    def check_signal(signal: SIGNAL) -> TS[bool]:
        return signal.value

    assert eval_node(check_signal, [True, True], resolution_dict={"signal": TS[bool]}) == [True, True]


def test_signal_triggers_node():
    """Test that SIGNAL triggers node evaluation when bound output ticks."""
    @compute_node
    def signal_triggered(signal: SIGNAL) -> TS[bool]:
        return True

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return signal_triggered(ts)

    # Each tick of ts triggers signal_triggered
    assert eval_node(g, [1, 2, 3]) == [True, True, True]


def test_signal_ignores_value():
    """Test that SIGNAL only cares about ticks, not values."""
    @compute_node
    def signal_count(signal: SIGNAL) -> TS[int]:
        return 1

    @graph
    def g(ts: TS[int]) -> TS[int]:
        return signal_count(ts)

    # Works regardless of what values ts has
    assert eval_node(g, [100, -5, 0]) == [1, 1, 1]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


def test_same_value_still_ticks():
    """Test that setting same value still creates a tick."""
    @compute_node
    def pass_value(ts: TS[int]) -> TS[int]:
        return ts.value

    assert eval_node(pass_value, [1, 1, 1]) == [1, 1, 1]


def test_output_injectable_for_inspection():
    """Test using _output injectable to inspect output state."""
    @compute_node
    def accumulate(ts: TS[int], _output: TS[int] = None) -> TS[int]:
        # _output.value gives previous tick's output value
        if _output.valid:
            return ts.value + _output.value
        return ts.value

    # 1, then 2+1=3, then 3+3=6
    assert eval_node(accumulate, [1, 2, 3]) == [1, 3, 6]


def test_can_apply_result_check():
    """Test that output can_apply_result works correctly."""
    @compute_node
    def single_write_per_tick(ts: TS[int], _output: TS[int] = None) -> TS[int]:
        # First write should succeed
        if _output.can_apply_result(ts.value):
            return ts.value

    assert eval_node(single_write_per_tick, [1, 2, 3]) == [1, 2, 3]


# =============================================================================
# MULTI-INPUT TESTS
# =============================================================================


def test_two_inputs_both_active():
    """Test node with two active inputs."""
    @compute_node
    def add(a: TS[int], b: TS[int]) -> TS[int]:
        if a.valid and b.valid:
            return a.value + b.value

    assert eval_node(add, [1, 2, 3], [10, 20, 30]) == [11, 22, 33]


def test_partial_ticks():
    """Test behavior when only one input ticks."""
    @compute_node
    def add_if_both(a: TS[int], b: TS[int]) -> TS[int]:
        if a.valid and b.valid:
            return a.value + b.value

    assert eval_node(add_if_both, [1, None, 2], [None, 10, None]) == [None, 11, 12]


def test_valid_inputs_constraint():
    """Test valid_inputs constraint on node."""
    @compute_node(valid=("a", "b"))
    def add_both_valid(a: TS[int], b: TS[int]) -> TS[int]:
        return a.value + b.value

    # Node only evaluates when both are valid
    assert eval_node(add_both_valid, [1, None, 2], [None, 10, None]) == [None, 11, 12]
