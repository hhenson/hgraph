"""
Time-Series Dict (TSD) Behavior Tests

This file tests all behaviors of the TSD[K, V] time-series dictionary type.
TSD represents a dynamic keyed collection of time-series values.

Test Dependencies: TS, TSS (TSS for key_set)
Implementation Order: 5

Behaviors Tested:
1. Output key creation and deletion
2. key_set tracking (TSS of keys)
3. contains_() operation
4. valid, modified properties
5. delta_value with REMOVE markers
6. Key observer notifications
7. modified_keys(), modified_values()
8. removed_keys() tracking
9. get_or_create() behavior
10. Peered binding and rebinding
"""
import pytest
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSD,
    TSS,
    TSB,
    REMOVE,
    REMOVE_IF_EXISTS,
    contains_,
    TimeSeriesSchema,
    K,
    map_,
    feedback,
)
from hgraph.test import eval_node


# =============================================================================
# OUTPUT VALUE TESTS
# =============================================================================


# Test TSD output value setting and retrieval.

def test_output_set_key_value():
    """Test setting key-value pairs on TSD output."""
    @compute_node
    def create_tsd(k: TS[str], v: TS[int]) -> TSD[str, TS[int]]:
        return {k.value: v.delta_value}

    assert eval_node(create_tsd, k=["a", "b"], v=[1, 2]) == [{"a": 1}, {"b": 2}]


def test_output_multiple_keys():
    """Test setting multiple keys in same tick."""
    @compute_node
    def create_multi(trigger: TS[bool]) -> TSD[str, TS[int]]:
        return {"a": 1, "b": 2}

    assert eval_node(create_multi, [True]) == [{"a": 1, "b": 2}]


def test_output_update_existing_key():
    """Test updating value of existing key."""
    @compute_node
    def update_key(k: TS[str], v: TS[int]) -> TSD[str, TS[int]]:
        return {k.value: v.delta_value}

    result = eval_node(update_key, ["a", "a"], [1, 2])
    assert result == [{"a": 1}, {"a": 2}]


# Test TSD key removal behavior.

def test_remove_key_via_marker():
    """Test removing key using REMOVE marker."""
    @compute_node
    def remove_key(k: TS[str], remove: TS[bool]) -> TSD[str, TS[int]]:
        if remove.value:
            return {k.value: REMOVE}
        else:
            return {k.value: 1}

    result = eval_node(remove_key, ["a", "a"], [False, True])
    assert result[0] == {"a": 1}
    assert result[1] == {"a": REMOVE}


def test_removed_keys_tracking():
    """Test that removed keys are tracked."""
    @compute_node
    def check_removed(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.removed_keys())

    result = eval_node(check_removed, [{"a": 1}, {"b": 2}, {"a": REMOVE}])
    assert result[2] == frozenset({"a"})


def test_remove_if_exists():
    """Test REMOVE_IF_EXISTS doesn't error on missing key."""
    @compute_node
    def create_then_remove(trigger: TS[bool]) -> TSD[str, TS[int]]:
        if trigger.value:
            return {"a": 1}
        else:
            return {"a": REMOVE_IF_EXISTS}

    # First add, then remove
    result = eval_node(create_then_remove, [True, False])
    assert result[0] == {"a": 1}
    # REMOVE_IF_EXISTS should succeed
    assert result[1] == {"a": REMOVE}


# =============================================================================
# KEY_SET TESTS
# =============================================================================


# Test TSD key_set property (TSS of keys).

def test_key_set_adds_keys():
    """Test that key_set tracks added keys."""
    @graph
    def g(tsd: TSD[str, TS[int]]) -> TSS[str]:
        return tsd.key_set

    result = eval_node(g, [{"a": 1}, {"b": 2}])
    assert frozenset(result[0].added) == {"a"}
    assert frozenset(result[1].added) == {"b"}


def test_key_set_removes_keys():
    """Test that key_set tracks removed keys."""
    @graph
    def g(tsd: TSD[str, TS[int]]) -> TSS[str]:
        return tsd.key_set

    result = eval_node(g, [{"a": 1}, {"a": REMOVE}])
    assert frozenset(result[0].added) == {"a"}
    assert frozenset(result[1].removed) == {"a"}


# =============================================================================
# CONTAINS TESTS
# =============================================================================


# Test TSD contains operation.

def test_contains_existing_key():
    """Test contains returns True for existing key."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[bool]:
        return contains_(tsd, k)

    assert eval_node(g, [{"a": 1}], ["a"]) == [True]


def test_contains_missing_key():
    """Test contains returns False for missing key."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[bool]:
        return contains_(tsd, k)

    assert eval_node(g, [{"a": 1}], ["b"]) == [False]


def test_contains_after_remove():
    """Test contains updates after key removal."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[bool]:
        return contains_(tsd, k)

    result = eval_node(g, [{"a": 1}, {"b": 2}, {"a": REMOVE}], ["a", None, None])
    assert result == [True, None, False]


# =============================================================================
# GETITEM TESTS
# =============================================================================


# Test TSD item access via __getitem__.

def test_getitem_existing_key():
    """Test getting value for existing key."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[int]:
        return tsd[k]

    result = eval_node(g, [{"a": 1}, {"b": 2}], ["a", "b"])
    assert result == [1, 2]


def test_getitem_with_key_update():
    """Test getting value after key updates."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[int]:
        return tsd[k]

    result = eval_node(g, [{"a": 1}, {"a": 2}], ["a", None])
    assert result == [1, 2]


def test_getitem_missing_then_added():
    """Test getting value for key that gets added."""
    @graph
    def g(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[int]:
        return tsd[k]

    result = eval_node(g, [{"a": 1}, {"b": 2}, {"b": 3}, {}, {"a": REMOVE}], ["b", None, None, "a"])
    assert result == [None, 2, 3, 1, None]


# =============================================================================
# STATE PROPERTY TESTS
# =============================================================================


# Test TSD output state properties.

def test_valid_after_add():
    """Test that TSD is valid when it has keys."""
    @compute_node
    def check_valid(tsd: TSD[str, TS[int]]) -> TS[bool]:
        return tsd.valid

    assert eval_node(check_valid, [{"a": 1}]) == [True]


def test_modified_on_key_change():
    """Test that modified is True when keys change."""
    @compute_node
    def check_modified(tsd: TSD[str, TS[int]]) -> TS[bool]:
        return tsd.modified

    assert eval_node(check_modified, [{"a": 1}, {"b": 2}]) == [True, True]


def test_modified_keys_method():
    """Test modified_keys() returns recently modified keys."""
    @compute_node
    def get_modified(tsd: TSD[str, TS[int]]) -> TS[frozenset]:
        return frozenset(tsd.modified_keys())

    result = eval_node(get_modified, [{"a": 1, "b": 2}, {"a": 3}])
    assert result == [frozenset({"a", "b"}), frozenset({"a"})]


def test_modified_values_method():
    """Test modified_values() returns recently modified values."""
    @compute_node
    def sum_modified(tsd: TSD[str, TS[int]]) -> TS[int]:
        return sum(v.delta_value for v in tsd.modified_values())

    result = eval_node(sum_modified, [{"a": 1, "b": 2}, {"a": 3}])
    assert result == [3, 3]


def test_modified_items_method():
    """Test modified_items() returns recently modified key-value pairs."""
    @compute_node
    def get_items(tsd: TSD[str, TS[int]]) -> TS[dict]:
        return {k: v.delta_value for k, v in tsd.modified_items()}

    result = eval_node(get_items, [{"a": 1, "b": 2}, {"a": 3}])
    assert result == [{"a": 1, "b": 2}, {"a": 3}]


# =============================================================================
# DELTA_VALUE TESTS
# =============================================================================


# Test TSD delta_value behavior.

def test_delta_value_includes_adds():
    """Test delta_value includes added keys."""
    @compute_node
    def get_delta(tsd: TSD[str, TS[int]]) -> TS[dict]:
        return dict(tsd.delta_value)

    result = eval_node(get_delta, [{"a": 1, "b": 2}])
    assert result == [{"a": 1, "b": 2}]


def test_delta_value_includes_removes():
    """Test delta_value includes REMOVE markers."""
    @compute_node
    def get_delta(tsd: TSD[str, TS[int]]) -> TS[dict]:
        return dict(tsd.delta_value)

    result = eval_node(get_delta, [{"a": 1}, {"a": REMOVE}])
    assert result[1]["a"] is REMOVE


# =============================================================================
# GET_OR_CREATE TESTS
# =============================================================================


# Test TSD get_or_create behavior.

def test_get_or_create_new_key():
    """Test get_or_create creates new key."""
    @compute_node
    def create_key(trigger: TS[bool], _output: TSD[str, TS[int]] = None) -> TSD[str, TS[int]]:
        ts = _output.get_or_create("a")
        ts.value = 42
        return _output.delta_value if _output.modified else None

    assert eval_node(create_key, [True]) == [{"a": 42}]


def test_get_or_create_existing_key():
    """Test get_or_create returns existing key."""
    @compute_node
    def update_key(tsd: TSD[str, TS[int]], _output: TSD[str, TS[int]] = None) -> TSD[str, TS[int]]:
        for k in tsd.modified_keys():
            ts = _output.get_or_create(k)
            ts.value = tsd[k].value * 2
        return _output.delta_value if _output.modified else None

    result = eval_node(update_key, [{"a": 1}, {"a": 2}])
    assert result == [{"a": 2}, {"a": 4}]


# =============================================================================
# ADD/REMOVE SAME CYCLE TESTS
# =============================================================================


# Test TSD add/remove in same cycle.

def test_add_remove_same_cycle():
    """Test adding and removing key in same cycle."""
    @compute_node
    def add_remove(trigger: TS[bool], _output: TSD[str, TS[int]] = None) -> TSD[str, TS[int]]:
        _output.get_or_create("a").value = 1
        del _output["a"]
        # Key should not appear
        assert "a" not in _output.removed_keys()
        return _output.delta_value if _output.modified else None

    assert eval_node(add_remove, [True]) == [{}]


def test_add_then_update():
    """Test adding then updating key in subsequent ticks."""
    @compute_node
    def add_then_update(v: TS[int], _output: TSD[str, TS[int]] = None) -> TSD[str, TS[int]]:
        ts = _output.get_or_create("a")
        ts.value = v.value
        return _output.delta_value if _output.modified else None

    assert eval_node(add_then_update, [1, 2, 3]) == [{"a": 1}, {"a": 2}, {"a": 3}]


# =============================================================================
# NESTED TSD TESTS
# =============================================================================


# Test TSD with complex value types.

def test_tsd_with_tsb_values():
    """Test TSD with TSB as value type."""
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @compute_node
    def copy_tsd(tsd: TSD[str, TSB[AB]]) -> TSD[str, TSB[AB]]:
        return dict((k, v.delta_value) for k, v in tsd.modified_items())

    @graph
    def g(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TSD[str, TSB[AB]]:
        return copy_tsd(map_(lambda x, y: TSB[AB].from_ts(a=x, b=y), tsd1, tsd2))

    result = eval_node(g, [{"x": 1, "y": 2}, {}], [{}, {"x": 7, "y": 8}])
    assert result == [
        {"x": {"a": 1}, "y": {"a": 2}},
        {"x": {"b": 7}, "y": {"b": 8}},
    ]


# =============================================================================
# CLEAR TESTS
# =============================================================================


# Test TSD clear behavior.

def test_delete_key():
    """Test deleting a key from TSD."""
    @compute_node
    def create_tsd(k: TS[str], remove: TS[bool]) -> TSD[str, TS[int]]:
        if remove.valid and remove.value:
            return {k.value: REMOVE}
        else:
            return {k.value: 42}

    # First tick adds key, second tick removes
    result = eval_node(create_tsd, ["a", "a"], [False, True])
    assert result[0] == {"a": 42}
    assert "a" in result[1] and result[1]["a"] is REMOVE


# =============================================================================
# TYPE VARIANT TESTS
# =============================================================================


# Test TSD with various key and value types.

def test_tsd_int_keys():
    """Test TSD with integer keys."""
    @compute_node
    def create_int_tsd(k: TS[int], v: TS[str]) -> TSD[int, TS[str]]:
        return {k.value: v.delta_value}

    assert eval_node(create_int_tsd, [1, 2], ["a", "b"]) == [{1: "a"}, {2: "b"}]


def test_tsd_float_values():
    """Test TSD with float values."""
    @compute_node
    def avg_tsd(tsd: TSD[str, TS[float]]) -> TS[float]:
        values = [v.value for v in tsd.values() if v.valid]
        return sum(values) / len(values) if values else 0.0

    result = eval_node(avg_tsd, [{"a": 1.0, "b": 2.0, "c": 3.0}])
    assert result == [2.0]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


# Test TSD edge cases and boundary conditions.

def test_empty_tsd():
    """Test empty TSD behavior."""
    @compute_node
    def check_empty(tsd: TSD[str, TS[int]]) -> TS[int]:
        return len(list(tsd.keys()))

    assert eval_node(check_empty, [{}]) == [0]


def test_iterate_keys():
    """Test iterating over TSD keys."""
    @compute_node
    def list_keys(tsd: TSD[str, TS[int]]) -> TS[tuple]:
        return tuple(sorted(tsd.keys()))

    result = eval_node(list_keys, [{"c": 3, "a": 1, "b": 2}])
    assert result == [("a", "b", "c")]


def test_iterate_values():
    """Test iterating over TSD values."""
    @compute_node
    def sum_values(tsd: TSD[str, TS[int]]) -> TS[int]:
        return sum(v.value for v in tsd.values() if v.valid)

    result = eval_node(sum_values, [{"a": 1, "b": 2, "c": 3}])
    assert result == [6]
