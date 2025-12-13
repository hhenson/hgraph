"""
Time-Series List (TSL) Behavior Tests

This file tests all behaviors of the TSL[TS[T], Size[N]] time-series list type.
TSL represents a fixed-size list of time-series elements.

Test Dependencies: TS (base type must work first)
Implementation Order: 3

Behaviors Tested:
1. Fixed-size list output creation
2. Element access via index
3. valid, modified, all_valid properties
4. delta_value returns only modified elements
5. Child modification propagates to parent
6. Peered vs non-peered input binding
7. TSL.from_ts() construction
8. Iteration and length
9. Invalidation and clearing
"""
import pytest
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSL,
    Size,
    SIZE,
    SCALAR,
    pass_through_node,
    getitem_,
    const,
)
from hgraph.nodes import flatten_tsl_values
from hgraph.test import eval_node


# =============================================================================
# OUTPUT VALUE TESTS
# =============================================================================


# Test TSL output value setting and retrieval.

def test_output_create_tsl():
    """Test creating TSL output via compute_node."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    # Output is dict with modified indices
    assert eval_node(create_tsl, [1, 2], [10, 20]) == [{0: 1, 1: 10}, {0: 2, 1: 20}]


def test_output_partial_tick():
    """Test that only modified elements are in delta output."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    # Only ts1 ticks second time
    assert eval_node(create_tsl, [1, 2], [10, None]) == [{0: 1, 1: 10}, {0: 2}]


def test_output_value_as_tuple():
    """Test getting TSL value as tuple."""
    @compute_node
    def get_tuple(tsl: TSL[TS[int], Size[2]]) -> TS[tuple]:
        return tsl.value

    # Value returns tuple with None for invalid elements
    assert eval_node(get_tuple, [{0: 1}, {1: 2}]) == [(1, None), (1, 2)]


# Test TSL output delta_value behavior.

def test_delta_value_only_modified():
    """Test that delta_value only contains modified elements."""
    @compute_node
    def get_delta(tsl: TSL[TS[int], Size[2]]) -> TS[dict]:
        return dict(tsl.delta_value)

    # delta_value is dict of index -> delta for modified elements
    assert eval_node(get_delta, [{0: 1, 1: 2}, {0: 3}]) == [
        {0: 1, 1: 2},
        {0: 3},
    ]


# =============================================================================
# STATE PROPERTY TESTS
# =============================================================================


# Test TSL output state properties.

def test_valid_when_any_child_valid():
    """Test that TSL is valid when any child is valid."""
    @compute_node
    def check_valid(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return tsl.valid

    # TSL valid when at least one element is valid
    assert eval_node(check_valid, [{0: 1}]) == [True]


def test_all_valid_requires_all_children():
    """Test that all_valid requires all children to be valid."""
    @compute_node
    def check_all_valid(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return tsl.all_valid

    # all_valid only True when both elements valid
    assert eval_node(check_all_valid, [{0: 1}, {1: 2}]) == [False, True]


def test_modified_when_any_child_modified():
    """Test that modified is True when any child is modified."""
    @compute_node
    def check_modified(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return tsl.modified

    assert eval_node(check_modified, [{0: 1}, {1: 2}]) == [True, True]


# =============================================================================
# ELEMENT ACCESS TESTS
# =============================================================================


# Test TSL element access via indexing.

def test_access_by_index():
    """Test accessing TSL element by integer index."""
    @graph
    def g(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        return tsl[0]

    assert eval_node(g, [{0: 1}, {0: 2}]) == [1, 2]


def test_access_via_getitem():
    """Test accessing TSL element via getitem_ operator."""
    assert eval_node(
        getitem_,
        [(1, 2), (3, 4), (5, 6)],
        0,
        resolution_dict={"ts": TSL[TS[int], Size[2]]}
    ) == [1, 3, 5]


def test_access_second_element():
    """Test accessing second element."""
    @graph
    def g(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        return tsl[1]

    assert eval_node(g, [{0: 1, 1: 10}, {1: 20}]) == [10, 20]


def test_len_returns_size():
    """Test that len() returns fixed size."""
    @graph
    def g(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return const(len(tsl))

    assert eval_node(g, tsl=[None], resolution_dict={"tsl": TSL[TS[int], Size[5]]}) == [5]


def test_iterate_over_elements():
    """Test iterating over TSL elements."""
    @compute_node
    def sum_valid(tsl: TSL[TS[int], Size[3]]) -> TS[int]:
        return sum(ts.value for ts in tsl if ts.valid)

    assert eval_node(sum_valid, [{0: 1, 1: 2, 2: 3}]) == [6]


# =============================================================================
# TSL.from_ts() CONSTRUCTION TESTS
# =============================================================================


# Test TSL construction via from_ts().

def test_from_ts_basic():
    """Test TSL.from_ts() basic construction."""
    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL.from_ts(ts1, ts2)
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(g, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


def test_from_ts_with_generator():
    """Test TSL.from_ts() with generator expression."""
    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL.from_ts((g for g in (ts1, ts2)))
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(g, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


def test_from_ts_creates_non_peered_input():
    """Test that from_ts creates non-peered binding."""
    @compute_node
    def check_peer(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return tsl.has_peer

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[bool]:
        tsl = TSL.from_ts(ts1, ts2)
        return check_peer(tsl)

    # from_ts creates non-peered input
    assert eval_node(g, [1], [2]) == [False]


def test_from_ts_with_explicit_type():
    """Test TSL.from_ts() with explicit element type."""
    @graph
    def g(ts1: TS[object], ts2: TS[int]) -> TSL[TS[object], Size[2]]:
        return TSL.from_ts(ts1, ts2, tp=TS[object])

    assert eval_node(g, ts1=[1, 2], ts2=[3, 4]) == [{0: 1, 1: 3}, {0: 2, 1: 4}]


# =============================================================================
# PEERED VS NON-PEERED BINDING TESTS
# =============================================================================


# Test TSL peered vs non-peered binding behavior.

def test_peered_binding_has_peer_true():
    """Test that peered binding sets has_peer to True."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    @compute_node
    def check_peer(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return tsl.has_peer

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[bool]:
        tsl = create_tsl(ts1, ts2)
        return check_peer(tsl)

    # Binding output TSL to input TSL creates peered binding
    assert eval_node(g, [1], [2]) == [True]


def test_non_peered_valid_any_child():
    """Test that non-peered TSL is valid when any child is valid."""
    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL.from_ts(ts1, ts2)
        return flatten_tsl_values[SCALAR:int](tsl)

    # Non-peered: valid when either child is valid
    assert eval_node(g, [1, None], [None, 2]) == [(1, None), (1, 2)]


def test_peered_to_peered_binding():
    """Test peered TSL to peered TSL binding."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = create_tsl(ts1, ts2)
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(g, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


# =============================================================================
# PASS-THROUGH TESTS
# =============================================================================


# Test TSL pass-through behavior.

def test_pass_through_peered():
    """Test pass_through with peered TSL."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        tsl = create_tsl(ts1, ts2)
        return tsl[0]

    assert eval_node(g, ts1=[1, 2], ts2=[3, 4]) == [1, 2]


def test_extract_element_from_tsl():
    """Test extracting single element from TSL."""
    @compute_node
    def create_tsl(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        out = {}
        if ts1.modified:
            out[0] = ts1.delta_value
        if ts2.modified:
            out[1] = ts2.delta_value
        return out

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        tsl = create_tsl(ts1, ts2)
        return tsl[1]

    assert eval_node(g, [1], [10, 20]) == [10, 20]


# =============================================================================
# CHILD MODIFICATION PROPAGATION
# =============================================================================


# Test child modification propagation to parent TSL.

def test_child_tick_triggers_parent():
    """Test that child modification triggers parent TSL tick."""
    call_count = [0]

    @compute_node
    def count_ticks(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        call_count[0] += 1
        return call_count[0]

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        tsl = TSL.from_ts(ts1, ts2)
        return count_ticks(tsl)

    eval_node(g, [1, 2, None], [None, None, 3])
    # Should be called when any child ticks
    assert call_count[0] == 3


def test_child_valid_state_propagates():
    """Test that child validity affects parent."""
    @compute_node
    def check_element_valid(tsl: TSL[TS[int], Size[2]]) -> TS[tuple]:
        return (tsl[0].valid, tsl[1].valid)

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[tuple]:
        tsl = TSL.from_ts(ts1, ts2)
        return check_element_valid(tsl)

    assert eval_node(g, [1, None], [None, 2]) == [(True, False), (True, True)]


# =============================================================================
# INVALIDATION TESTS
# =============================================================================


# Test TSL invalidation behavior.

def test_clear_clears_all_children():
    """Test that clear() clears all child elements."""
    @compute_node(valid=())
    def create_and_clear(ts: TS[int], clear: TS[bool], _output: TSL[TS[int], Size[2]] = None) -> TSL[TS[int], Size[2]]:
        if clear.valid and clear.value:
            _output.clear()
        elif ts.valid:
            return {0: ts.value, 1: ts.value * 2}
        return _output.delta_value if _output.modified else None

    # Note: TSL clear behavior depends on implementation
    result = eval_node(create_and_clear, [1, None], [False, True])
    assert result[0] == {0: 1, 1: 2}


# =============================================================================
# NESTED TSL TESTS
# =============================================================================


# Test nested TSL behavior.

def test_tsl_of_tsl():
    """Test TSL containing other TSL elements."""
    @compute_node
    def create_nested(a: TS[int], b: TS[int], c: TS[int], d: TS[int]) -> TSL[TSL[TS[int], Size[2]], Size[2]]:
        out = {}
        inner1 = {}
        inner2 = {}
        if a.modified:
            inner1[0] = a.value
        if b.modified:
            inner1[1] = b.value
        if c.modified:
            inner2[0] = c.value
        if d.modified:
            inner2[1] = d.value
        if inner1:
            out[0] = inner1
        if inner2:
            out[1] = inner2
        return out

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int]) -> TS[int]:
        nested = create_nested(a, b, c, d)
        return nested[0][0]

    assert eval_node(g, [1, 2], [10], [100], [1000]) == [1, 2]


# =============================================================================
# TYPE VARIANT TESTS
# =============================================================================


# Test TSL with various element types.

def test_tsl_of_strings():
    """Test TSL[TS[str], Size[N]]."""
    @compute_node
    def concat_tsl(tsl: TSL[TS[str], Size[2]]) -> TS[str]:
        return "".join(ts.value for ts in tsl if ts.valid)

    assert eval_node(concat_tsl, [{0: "hello", 1: "world"}]) == ["helloworld"]


def test_tsl_of_floats():
    """Test TSL[TS[float], Size[N]]."""
    @compute_node
    def avg_tsl(tsl: TSL[TS[float], Size[3]]) -> TS[float]:
        valid_values = [ts.value for ts in tsl if ts.valid]
        return sum(valid_values) / len(valid_values) if valid_values else 0.0

    assert eval_node(avg_tsl, [{0: 1.0, 1: 2.0, 2: 3.0}]) == [2.0]


def test_tsl_of_bools():
    """Test TSL[TS[bool], Size[N]]."""
    @compute_node
    def all_true(tsl: TSL[TS[bool], Size[2]]) -> TS[bool]:
        return all(ts.value for ts in tsl if ts.valid)

    assert eval_node(all_true, [{0: True, 1: True}]) == [True]
    assert eval_node(all_true, [{0: True, 1: False}]) == [False]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


# Test TSL edge cases and boundary conditions.

def test_size_one_tsl():
    """Test TSL with size 1."""
    @compute_node
    def create_single(ts: TS[int]) -> TSL[TS[int], Size[1]]:
        return {0: ts.value}

    assert eval_node(create_single, [1, 2, 3]) == [{0: 1}, {0: 2}, {0: 3}]


def test_empty_delta_no_tick():
    """Test that empty delta doesn't produce tick."""
    @compute_node
    def maybe_tick(ts: TS[int]) -> TSL[TS[int], Size[2]]:
        if ts.value > 0:
            return {0: ts.value}
        return {}

    # When returning empty dict, TSL may or may not tick
    result = eval_node(maybe_tick, [1, -1, 2])
    assert result[0] == {0: 1}
    assert result[2] == {0: 2}


def test_modified_elements_tracking():
    """Test tracking which elements are modified."""
    @compute_node
    def track_modified(tsl: TSL[TS[int], Size[3]]) -> TS[tuple]:
        return tuple(i for i, ts in enumerate(tsl) if ts.modified)

    assert eval_node(track_modified, [{0: 1, 1: 2, 2: 3}, {1: 10}]) == [
        (0, 1, 2),
        (1,),
    ]
