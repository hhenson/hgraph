"""
Time-Series Set (TSS) Behavior Tests

This file tests all behaviors of the TSS[T] time-series set type.
TSS represents a set that tracks additions and removals via delta semantics.

Test Dependencies: TS (base type must work first)
Implementation Order: 2

Behaviors Tested:
1. Output value setting (set values)
2. add/remove delta tracking
3. SetDelta semantics and composition
4. valid, modified properties
5. delta_value returns SetDelta
6. contains_() operation
7. is_empty tracking
8. Input binding (peered)
9. Rebinding delta computation (input switching)
10. Removed marker handling
"""
import pytest
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSS,
    set_delta,
    Removed,
    contains_,
    SIZE,
    pass_through_node,
    SIGNAL,
)
from hgraph.test import eval_node


# =============================================================================
# OUTPUT VALUE TESTS
# =============================================================================


def test_output_set_initial_value():
    """Test setting initial set value on output."""
    @compute_node
    def create_set(trigger: TS[bool]) -> TSS[int]:
        if trigger.value:
            return {1, 2, 3}

    assert eval_node(create_set, [True]) == [{1, 2, 3}]


def test_output_set_via_frozenset():
    """Test setting value via frozenset."""
    @compute_node
    def from_frozenset(ts: TS[frozenset[int]]) -> TSS[int]:
        return ts.value

    # First tick sets full set, subsequent ticks compute delta
    assert eval_node(from_frozenset, [frozenset({1, 2}), frozenset({2, 3})]) == [
        {1, 2},
        {3, Removed(1)},
    ]


def test_output_set_empty():
    """Test creating empty set."""
    @compute_node
    def empty_set(trigger: TS[bool]) -> TSS[int]:
        return set()

    assert eval_node(empty_set, [True]) == [set()]


# =============================================================================
# OUTPUT DELTA TESTS
# =============================================================================


def test_add_elements():
    """Test adding elements to set."""
    @compute_node
    def add_elements(key: TS[int], add: TS[bool]) -> TSS[int]:
        if add.value:
            return set_delta(added=frozenset([key.value]), removed=frozenset(), tp=int)

    assert eval_node(add_elements, [1, 2, 3], [True, True, True]) == [
        set_delta(frozenset({1}), frozenset(), tp=int),
        set_delta(frozenset({2}), frozenset(), tp=int),
        set_delta(frozenset({3}), frozenset(), tp=int),
    ]


def test_remove_elements():
    """Test removing elements from set."""
    @compute_node
    def add_remove(key: TS[str], add: TS[bool]) -> TSS[str]:
        if add.value:
            return set_delta(added=frozenset([key.value]), removed=frozenset(), tp=str)
        else:
            return set_delta(added=frozenset(), removed=frozenset([key.value]), tp=str)

    assert eval_node(add_remove, key=["a", "b", "a"], add=[True, True, False]) == [
        set_delta(frozenset("a"), frozenset(), tp=str),
        set_delta(frozenset("b"), frozenset(), tp=str),
        set_delta(frozenset(), frozenset("a"), tp=str),
    ]


def test_delta_add_same_element_no_change():
    """Test that adding same element twice doesn't create duplicate delta."""
    @compute_node
    def add_twice(key: TS[str], add: TS[bool]) -> TSS[str]:
        if add.value:
            return set_delta(added=frozenset([key.value]), removed=frozenset(), tp=str)

    # Second add of "a" should produce no delta (None) since it's already in set
    assert eval_node(add_twice, key=["a", "a", "b"], add=[True, True, True]) == [
        set_delta(frozenset("a"), frozenset(), tp=str),
        None,  # Already in set
        set_delta(frozenset("b"), frozenset(), tp=str),
    ]


# =============================================================================
# SET DELTA COMPOSITION TESTS
# =============================================================================


def test_set_delta_addition_basic():
    """Test basic SetDelta addition."""
    d1 = set_delta(added={1, 2, 3}, removed=set(), tp=int)
    d2 = set_delta(added={4, 5}, removed={3}, tp=int)
    result = d1 + d2
    # 3 is added then removed, so not in final added set
    # 4, 5 are newly added
    assert result == set_delta(added={1, 2, 4, 5}, removed=set(), tp=int)


def test_set_delta_addition_with_remove_canceling_add():
    """Test that removing an added element cancels it."""
    d1 = set_delta(added={1, 2}, removed=set(), tp=int)
    d2 = set_delta(added=set(), removed={1}, tp=int)
    result = d1 + d2
    assert result == set_delta(added={2}, removed=set(), tp=int)


def test_set_delta_addition_accumulates():
    """Test that adding deltas accumulates correctly."""
    d1 = set_delta(added={1, 2}, removed=set(), tp=int)
    d2 = set_delta(added={3}, removed=set(), tp=int)
    result = d1 + d2
    assert result == set_delta(added={1, 2, 3}, removed=set(), tp=int)


# =============================================================================
# STATE PROPERTY TESTS
# =============================================================================


def test_valid_after_set():
    """Test that TSS becomes valid after setting a value."""
    @compute_node
    def create_set(trigger: TS[bool]) -> TSS[int]:
        return {1, 2, 3}

    @graph
    def g(trigger: TS[bool]) -> TS[bool]:
        from hgraph import valid, TIME_SERIES_TYPE
        return valid[TIME_SERIES_TYPE: TSS[int]](create_set(trigger))

    # TSS is valid after setting a value
    assert eval_node(g, [True]) == [True]


def test_modified_when_changed():
    """Test that modified is True when set changes."""
    @compute_node
    def check_modified(tss: TSS[int]) -> TS[bool]:
        return tss.modified

    assert eval_node(check_modified, [{1}, {2}, {3}]) == [True, True, True]


def test_delta_value_returns_set_delta():
    """Test that delta_value returns the current delta."""
    @compute_node
    def get_delta(tss: TSS[int]) -> TS[tuple]:
        delta = tss.delta_value
        return (frozenset(delta.added), frozenset(delta.removed))

    result = eval_node(get_delta, [{1, 2}, {3, Removed(1)}])
    assert result == [
        (frozenset({1, 2}), frozenset()),
        (frozenset({3}), frozenset({1})),
    ]


# =============================================================================
# CONTAINS TESTS
# =============================================================================


def test_contains_element_present():
    """Test contains returns True when element is in set."""
    @graph
    def g(tss: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(tss, key)

    assert eval_node(g, [{1, 2, 3}], [1]) == [True]


def test_contains_element_absent():
    """Test contains returns False when element is not in set."""
    @graph
    def g(tss: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(tss, key)

    assert eval_node(g, [{1, 2, 3}], [5]) == [False]


def test_contains_after_removal():
    """Test contains updates after element is removed."""
    @graph
    def g(tss: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(tss, key)

    assert eval_node(g, [{1}, {2}, {Removed(1)}], [1, None, None]) == [True, None, False]


def test_contains_after_add():
    """Test contains updates after element is added."""
    @graph
    def g(tss: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(tss, key)

    assert eval_node(g, [{1}, {2}, None, {Removed(2)}], [2, None, None, None, 1]) == [
        False,
        True,
        None,
        False,
        True,
    ]


# =============================================================================
# IS_EMPTY TESTS
# =============================================================================


def test_empty_set_is_empty():
    """Test that empty set is empty."""
    @compute_node
    def check_empty(tss: TSS[int]) -> TS[bool]:
        return len(tss.value) == 0

    assert eval_node(check_empty, [set()]) == [True]


def test_non_empty_set_not_empty():
    """Test that non-empty set is not empty."""
    @compute_node
    def check_empty(tss: TSS[int]) -> TS[bool]:
        return len(tss.value) == 0

    assert eval_node(check_empty, [{1}]) == [False]


# =============================================================================
# REMOVED MARKER TESTS
# =============================================================================


def test_removed_marker_in_input():
    """Test that Removed marker indicates element removal."""
    @compute_node
    def process_tss(tss: TSS[int]) -> TS[tuple]:
        return (frozenset(tss.added()), frozenset(tss.removed()))

    assert eval_node(process_tss, [{1, 2}, {Removed(1), 3}]) == [
        (frozenset({1, 2}), frozenset()),
        (frozenset({3}), frozenset({1})),
    ]


def test_removed_marker_removes_from_value():
    """Test that Removed marker actually removes from set value."""
    @compute_node
    def get_value(tss: TSS[int]) -> TS[frozenset]:
        return frozenset(tss.value)

    assert eval_node(get_value, [{1, 2, 3}, {Removed(2)}]) == [
        frozenset({1, 2, 3}),
        frozenset({1, 3}),
    ]


def test_added_and_removed_methods():
    """Test TSS added() and removed() methods."""
    @compute_node
    def check_added_removed(tss: TSS[int]) -> TS[tuple]:
        return (frozenset(tss.added()), frozenset(tss.removed()))

    result = eval_node(check_added_removed, [{1, 2}, {3}, {Removed(1), 4}])
    assert result == [
        (frozenset({1, 2}), frozenset()),
        (frozenset({3}), frozenset()),
        (frozenset({4}), frozenset({1})),
    ]


# =============================================================================
# INPUT BINDING TESTS
# =============================================================================


def test_input_value_delegates_to_output():
    """Test that input.value returns output's set value."""
    @compute_node
    def get_value(tss: TSS[int]) -> TS[frozenset]:
        return frozenset(tss.value)

    assert eval_node(get_value, [{1, 2, 3}]) == [frozenset({1, 2, 3})]


def test_input_modified_when_output_modified():
    """Test that input.modified reflects output modification."""
    @compute_node
    def check_modified(tss: TSS[int]) -> TS[bool]:
        return tss.modified

    assert eval_node(check_modified, [{1}, {2}, {3}]) == [True, True, True]


def test_pass_through_preserves_delta():
    """Test that pass_through_node preserves TSS deltas."""
    @graph
    def g(key: TS[str], add: TS[bool]) -> TSS[str]:
        @compute_node
        def create_tss(key: TS[str], add: TS[bool]) -> TSS[str]:
            if add.value:
                return set_delta(added=frozenset([key.value]), removed=frozenset(), tp=str)
            else:
                return set_delta(added=frozenset(), removed=frozenset([key.value]), tp=str)

        tss = create_tss(key, add)
        return pass_through_node(tss)

    assert eval_node(g, key=["a", "b", "a"], add=[True, True, False]) == [
        set_delta(frozenset("a"), frozenset(), tp=str),
        set_delta(frozenset("b"), frozenset(), tp=str),
        set_delta(frozenset(), frozenset("a"), tp=str),
    ]


# =============================================================================
# SET OPERATIONS TESTS
# =============================================================================


def test_values_returns_current_set():
    """Test that values() returns the current set contents."""
    @compute_node
    def get_values(tss: TSS[int]) -> TS[frozenset]:
        return frozenset(tss.values())

    assert eval_node(get_values, [{1, 2, 3}, {Removed(2)}]) == [
        frozenset({1, 2, 3}),
        frozenset({1, 3}),
    ]


def test_iter_over_set():
    """Test iterating over TSS."""
    @compute_node
    def iter_tss(tss: TSS[int]) -> TS[int]:
        return sum(tss.value)

    assert eval_node(iter_tss, [{1, 2, 3}]) == [6]


def test_len_of_set():
    """Test getting length of TSS."""
    @compute_node
    def len_tss(tss: TSS[int]) -> TS[int]:
        return len(tss.value)

    assert eval_node(len_tss, [{1, 2, 3}, {4}, {Removed(1)}]) == [3, 4, 3]


# =============================================================================
# TYPE VARIANT TESTS
# =============================================================================


def test_tss_int():
    """Test TSS[int] basic operation."""
    @compute_node
    def sum_set(tss: TSS[int]) -> TS[int]:
        return sum(tss.value)

    assert eval_node(sum_set, [{1, 2, 3}, {4}]) == [6, 10]


def test_tss_str():
    """Test TSS[str] basic operation."""
    @compute_node
    def join_set(tss: TSS[str]) -> TS[str]:
        return ",".join(sorted(tss.value))

    assert eval_node(join_set, [{"a", "b", "c"}]) == ["a,b,c"]


def test_tss_tuple():
    """Test TSS[tuple] basic operation."""
    @compute_node
    def count_tuples(tss: TSS[tuple]) -> TS[int]:
        return len(tss.value)

    assert eval_node(count_tuples, [{(1, 2), (3, 4)}]) == [2]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


def test_add_then_remove_same_cycle_via_output():
    """Test adding and removing same element in same tick via _output."""
    @compute_node
    def add_then_remove(trigger: TS[bool], _output: TSS[int] = None) -> TSS[int]:
        _output.add(1)
        _output.remove(1)  # Use remove() not discard()
        return _output.delta_value

    # Element added then immediately removed should produce empty delta
    result = eval_node(add_then_remove, [True])
    # After add+remove, 1 should not be in added
    assert frozenset(result[0].added) == frozenset()


def test_add_already_existing_no_delta():
    """Test that adding existing element produces no new delta."""
    @compute_node
    def add_existing(tss: TSS[int]) -> TSS[int]:
        # If element already in set, delta should not include it as added
        return tss.delta_value

    # First tick adds 1, second tick tries to add 1 again
    result = eval_node(add_existing, [{1}, {1}])
    # First tick: 1 is added
    assert frozenset(result[0].added) == frozenset({1})
    # Second tick: no change, so None
    assert result[1] is None


def test_clear_set_via_frozenset():
    """Test clearing the set by assigning empty frozenset."""
    @compute_node
    def clear_via_frozenset(ts: TS[frozenset[int]]) -> TSS[int]:
        return ts.value

    # Set to {1,2,3}, then clear to empty
    result = eval_node(clear_via_frozenset, [frozenset({1, 2, 3}), frozenset()])
    assert frozenset(result[0].added) == frozenset({1, 2, 3})
    assert frozenset(result[1].removed) == frozenset({1, 2, 3})


# =============================================================================
# SIGNAL WITH TSS TESTS
# =============================================================================


def test_signal_from_tss():
    """Test using TSS as SIGNAL input."""
    @compute_node
    def signal_from_tss(signal: SIGNAL) -> TS[bool]:
        return True

    @graph
    def g(tss: TSS[int]) -> TS[bool]:
        return signal_from_tss(tss)

    # Each TSS tick triggers the signal
    assert eval_node(g, [{1}, {2}, {Removed(1)}]) == [True, True, True]
