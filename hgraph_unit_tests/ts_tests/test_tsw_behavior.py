"""
Time-Series Window (TSW) Behavior Tests

This file tests all behaviors of the TSW[T, Size] time-series window type.
TSW represents a circular buffer of values with associated timestamps.

Test Dependencies: TS (base type must work first)
Implementation Order: 6

Behaviors Tested:
1. Fixed-size window (count-based)
2. Time-based window (duration-based)
3. min_size validity
4. Circular buffer overflow
5. removed_value tracking
6. Window iteration
7. Window aggregation operations
8. valid, modified properties
"""
import pytest
from datetime import timedelta

from hgraph import (
    compute_node,
    graph,
    TS,
    TSW,
    MIN_TD,
    to_window,
    sum_,
    mean,
)
from hgraph.test import eval_node


# =============================================================================
# FIXED WINDOW (COUNT-BASED) TESTS
# =============================================================================


# Test fixed-size (count-based) window behavior.

def test_create_fixed_window():
    """Test creating fixed-size window."""
    @graph
    def g(ts: TS[int]) -> TSW[int]:
        return to_window(ts, 3, 1)

    result = eval_node(g, [1, 2, 3])
    # Window outputs latest value added
    assert result == [1, 2, 3]


def test_window_below_min_size():
    """Test window below min_size returns values but may not be all_valid."""
    @compute_node
    def check_all_valid(window: TSW[int]) -> TS[bool]:
        return window.all_valid

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 3, 3)  # min_size = 3
        return check_all_valid(window)

    # all_valid False until 3 elements
    result = eval_node(g, [1, 2, 3, 4])
    assert result == [False, False, True, True]


def test_window_at_min_size():
    """Test window becomes valid at min_size."""
    @graph
    def g(ts: TS[int]) -> TS[float]:
        window = to_window(ts, 3, 3)
        return mean(window)

    import numpy as np
    result = eval_node(g, [1, 2, 3, 4])
    # mean only available after 3 elements
    assert result[0] is None
    assert result[1] is None
    assert result[2] == pytest.approx(np.mean([1, 2, 3]))
    assert result[3] == pytest.approx(np.mean([2, 3, 4]))


def test_window_overflow():
    """Test circular buffer overflow removes oldest."""
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 1)
        return sum_(window)

    # Window size 3: sums last 3 values
    result = eval_node(g, [1, 2, 3, 4, 5])
    assert result == [1, 3, 6, 9, 12]  # 1, 1+2, 1+2+3, 2+3+4, 3+4+5


# Test window sum operation.

def test_sum_fixed_window():
    """Test summing fixed window."""
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 1)
        return sum_(window)

    assert eval_node(g, [1, 2, 3]) == [1, 3, 6]


def test_sum_with_overflow():
    """Test sum after window overflow."""
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 2, 1)
        return sum_(window)

    # Window size 2
    result = eval_node(g, [1, 2, 3, 4])
    assert result == [1, 3, 5, 7]  # 1, 1+2, 2+3, 3+4


# Test window mean operation.

def test_mean_fixed_window():
    """Test mean of fixed window."""
    @graph
    def g(ts: TS[int]) -> TS[float]:
        window = to_window(ts, 3, 3)
        return mean(window)

    import numpy as np
    result = eval_node(g, [1, -2, 3, 4])
    assert result[:2] == [None, None]  # Below min_size
    assert result[2] == pytest.approx(np.mean([1, -2, 3]))
    assert result[3] == pytest.approx(np.mean([-2, 3, 4]))


# =============================================================================
# STATE PROPERTY TESTS
# =============================================================================


# Test TSW output state properties.

def test_valid_from_first_value():
    """Test that window is valid after first value."""
    @compute_node
    def check_valid(window: TSW[int]) -> TS[bool]:
        return window.valid

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 3, 1)
        return check_valid(window)

    assert eval_node(g, [1, 2]) == [True, True]


def test_all_valid_after_min_size():
    """Test that all_valid requires min_size elements."""
    @compute_node
    def check_all_valid(window: TSW[int]) -> TS[bool]:
        return window.all_valid

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 3, 2)  # min_size = 2
        return check_all_valid(window)

    result = eval_node(g, [1, 2, 3])
    assert result == [False, True, True]


def test_modified_on_value_add():
    """Test that modified is True when value is added."""
    @compute_node
    def check_modified(window: TSW[int]) -> TS[bool]:
        return window.modified

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 3, 1)
        return check_modified(window)

    assert eval_node(g, [1, 2, 3]) == [True, True, True]


# =============================================================================
# WINDOW ITERATION TESTS
# =============================================================================


# Test TSW iteration behavior.

def test_iterate_window_values():
    """Test iterating over window values."""
    @compute_node
    def to_list(window: TSW[int]) -> TS[tuple]:
        return tuple(window.value)

    @graph
    def g(ts: TS[int]) -> TS[tuple]:
        window = to_window(ts, 3, 1)
        return to_list(window)

    result = eval_node(g, [1, 2, 3, 4])
    assert result[0] == (1,)
    assert result[1] == (1, 2)
    assert result[2] == (1, 2, 3)
    assert result[3] == (2, 3, 4)  # Oldest dropped


def test_window_length():
    """Test getting window current length."""
    @compute_node
    def window_len(window: TSW[int]) -> TS[int]:
        return int(len(list(window.value)))

    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 1)
        return window_len(window)

    result = eval_node(g, [1, 2, 3, 4])
    assert result == [1, 2, 3, 3]  # Max 3 due to window size


# =============================================================================
# REMOVED VALUE TESTS
# =============================================================================


# Test TSW removed value tracking.

def test_removed_value_on_overflow():
    """Test that removed_value is set on overflow."""
    @compute_node
    def get_removed(window: TSW[int]) -> TS[int]:
        rv = window.removed_value
        if rv is not None:
            return int(rv)  # Convert numpy type to Python int
        return -1

    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 2, 1)
        return get_removed(window)

    result = eval_node(g, [1, 2, 3, 4])
    # No overflow until third element
    assert result[:2] == [-1, -1]
    assert result[2] == 1  # 1 removed when 3 added
    assert result[3] == 2  # 2 removed when 4 added


# =============================================================================
# ABSOLUTE VALUE TESTS
# =============================================================================


# Test absolute value on window.

def test_abs_window():
    """Test applying abs to window values."""
    from hgraph import abs_

    @graph
    def g(ts: TS[int]) -> TSW[int]:
        window = to_window(ts, 3, 1)
        return abs_(window)

    result = eval_node(g, [1, -2, 3])
    assert result == [1, 2, 3]


# =============================================================================
# TYPE VARIANT TESTS
# =============================================================================


# Test TSW with various value types.

def test_tsw_float():
    """Test TSW[float] window."""
    @graph
    def g(ts: TS[float]) -> TS[float]:
        window = to_window(ts, 3, 1)
        return sum_(window)

    result = eval_node(g, [1.0, 2.5, 3.5])
    assert result == [1.0, 3.5, 7.0]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


# Test TSW edge cases and boundary conditions.

def test_window_size_one():
    """Test window with size 1."""
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 1, 1)
        return sum_(window)

    # Window of 1 just passes through values
    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


def test_min_size_equals_size():
    """Test window where min_size equals size."""
    @compute_node
    def check_all_valid(window: TSW[int]) -> TS[bool]:
        return window.all_valid

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 3, 3)
        return check_all_valid(window)

    result = eval_node(g, [1, 2, 3, 4])
    assert result == [False, False, True, True]


def test_min_size_one():
    """Test window with min_size 1."""
    @compute_node
    def check_all_valid(window: TSW[int]) -> TS[bool]:
        return window.all_valid

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        window = to_window(ts, 5, 1)
        return check_all_valid(window)

    # All valid from first value
    assert eval_node(g, [1, 2, 3]) == [True, True, True]
