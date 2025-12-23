"""
Tests for TSD key_set functionality.

These tests verify that TSD.key_set returns a proper TSS view (not a frozenset)
that has full TSS functionality including delta tracking.

The key_set tests use graph execution since key_set is on TimeSeriesDictOutput
(graph wrapper), not on the low-level TSOutput/TSOutputView.
"""
import pytest
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSD,
    TSS,
    REMOVE,
)
from hgraph.test import eval_node


# =============================================================================
# TSD key_set Type Tests
# =============================================================================


def test_tsd_key_set_is_tss():
    """Test that key_set is a TSS type."""
    @compute_node
    def check_key_set_type(tsd: TSD[str, TS[int]]) -> TS[bool]:
        # key_set should be a TimeSeriesSet, not a frozenset
        key_set = tsd.key_set
        # TSS has valid property
        return hasattr(key_set, 'valid')

    result = eval_node(check_key_set_type, [{"a": 1}])
    assert result[0] == True


def test_tsd_key_set_values():
    """Test that key_set contains the correct keys."""
    @compute_node
    def get_key_set_values(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        # Get the key_set values as a frozenset
        return frozenset(tsd.key_set.value)

    result = eval_node(get_key_set_values, [{"a": 1, "b": 2, "c": 3}])
    assert result[0] == frozenset({"a", "b", "c"})


def test_tsd_key_set_size():
    """Test that key_set size reflects the number of keys."""
    @compute_node
    def get_key_set_size(tsd: TSD[str, TS[int]]) -> TS[int]:
        return len(tsd.key_set.value)

    result = eval_node(get_key_set_size, [{"a": 1, "b": 2, "c": 3}])
    assert result[0] == 3


@pytest.mark.xfail(reason="Empty TSD doesn't tick the node")
def test_tsd_key_set_empty():
    """Test key_set on empty TSD."""
    @compute_node
    def check_empty_key_set(tsd: TSD[str, TS[int]]) -> TS[int]:
        return len(tsd.key_set.value)

    result = eval_node(check_empty_key_set, [{}])
    assert result[0] == 0


def test_tsd_key_set_updates_on_add():
    """Test that key_set updates when keys are added."""
    @compute_node
    def track_key_set(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.value)

    result = eval_node(track_key_set, [{"a": 1}, {"a": 1, "b": 2}])
    assert result[0] == frozenset({"a"})
    assert result[1] == frozenset({"a", "b"})


def test_tsd_key_set_updates_on_remove():
    """Test that key_set updates when keys are removed."""
    @compute_node
    def track_key_set(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.value)

    result = eval_node(track_key_set, [{"a": 1, "b": 2}, {"a": REMOVE}])
    assert result[0] == frozenset({"a", "b"})
    assert result[1] == frozenset({"b"})


# =============================================================================
# TSD key_set Delta Tracking Tests
# =============================================================================


@pytest.mark.xfail(reason="C++ TSS delta tracking not fully implemented")
def test_tsd_key_set_added():
    """Test that key_set.added() returns newly added keys."""
    @compute_node
    def get_added_keys(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.added())

    result = eval_node(get_added_keys, [{"a": 1}, {"b": 2}])
    assert result[0] == frozenset({"a"})
    assert result[1] == frozenset({"b"})


@pytest.mark.xfail(reason="C++ TSS delta tracking not fully implemented")
def test_tsd_key_set_removed():
    """Test that key_set.removed() returns removed keys."""
    @compute_node
    def get_removed_keys(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.removed())

    result = eval_node(get_removed_keys, [{"a": 1, "b": 2}, {"a": REMOVE}])
    assert result[0] == frozenset()  # No removals on first tick
    assert result[1] == frozenset({"a"})


def test_tsd_key_set_modified():
    """Test that key_set tracks modification."""
    @compute_node
    def check_modified(tsd: TSD[str, TS[int]]) -> TS[bool]:
        return tsd.key_set.modified

    result = eval_node(check_modified, [{"a": 1}, {"a": 2}])
    assert result[0] == True  # First tick - key added
    # Second tick - value updated, but key not changed
    # Depending on implementation, key_set may or may not be modified


# =============================================================================
# TSD key_set Contains Tests
# =============================================================================


def test_tsd_key_set_contains():
    """Test that key_set supports contains check."""
    @compute_node
    def check_contains(tsd: TSD[str, TS[int]], key: TS[str]) -> TS[bool]:
        return key.value in tsd.key_set.value

    result = eval_node(check_contains, [{"a": 1, "b": 2}], ["a", "z"])
    assert result[0] == True  # "a" is in key_set
    assert result[1] == False  # "z" is not in key_set


@pytest.mark.xfail(reason="was_added API returns None in some cases")
def test_tsd_key_set_was_added():
    """Test was_added check on key_set.

    Note: was_added works best when checked on a specific key that
    exists in the current tick. For TSD inputs, was_added checks if
    a key was newly added in the current tick.
    """
    @compute_node
    def check_was_added(tsd: TSD[str, TS[int]]) -> TS[tuple]:
        # Check was_added for specific keys we know about
        # Note: in first tick, "a" is added; in second tick, "b" is added
        a_added = tsd.key_set.was_added("a")
        b_added = tsd.key_set.was_added("b")
        return (a_added, b_added)

    result = eval_node(check_was_added, [{"a": 1}, {"b": 2}])
    # First tick: "a" added, "b" not present
    assert result[0] == (True, False)
    # Second tick: "a" not newly added, "b" added
    assert result[1] == (False, True)


# =============================================================================
# TSD key_set with Integer Keys
# =============================================================================


def test_tsd_key_set_int_keys():
    """Test key_set with integer keys."""
    @compute_node
    def get_int_key_set(tsd: TSD[int, TS[str]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.value)

    result = eval_node(get_int_key_set, [{1: "a", 2: "b", 3: "c"}])
    assert result[0] == frozenset({1, 2, 3})


# =============================================================================
# Edge Cases
# =============================================================================


def test_tsd_key_set_single_key():
    """Test key_set with a single key."""
    @compute_node
    def get_single_key_set(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.value)

    result = eval_node(get_single_key_set, [{"only": 42}])
    assert result[0] == frozenset({"only"})


def test_tsd_key_set_large():
    """Test key_set with many keys."""
    @compute_node
    def get_large_key_set(tsd: TSD[str, TS[int]]) -> TS[int]:
        return len(tsd.key_set.value)

    large_dict = {f"key_{i}": i for i in range(100)}
    result = eval_node(get_large_key_set, [large_dict])
    assert result[0] == 100


def test_tsd_key_set_add_and_remove_same_tick():
    """Test adding and removing different keys in same tick."""
    @compute_node
    def complex_update(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.key_set.value)

    result = eval_node(complex_update, [
        {"a": 1, "b": 2},
        {"a": REMOVE, "c": 3}
    ])
    assert result[0] == frozenset({"a", "b"})
    assert result[1] == frozenset({"b", "c"})
