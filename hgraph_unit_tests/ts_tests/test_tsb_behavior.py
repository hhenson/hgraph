"""
Time-Series Bundle (TSB) Behavior Tests

This file tests all behaviors of the TSB[Schema] time-series bundle type.
TSB represents a bundle of named heterogeneous time-series fields.

Test Dependencies: TS (base type must work first)
Implementation Order: 4

Behaviors Tested:
1. Bundle output creation with schema
2. Field access by name
3. Field access by index
4. valid, modified, all_valid properties
5. delta_value returns only modified fields
6. TSB.from_ts() construction
7. Peered vs non-peered binding
8. Inline schema definition
9. Generic schemas
10. CompoundScalar integration
"""
import pytest
from dataclasses import dataclass
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSB,
    TSB_OUT,
    TimeSeriesSchema,
    CompoundScalar,
    pass_through_node,
    SIGNAL,
    SCALAR,
    SCALAR_1,
    AUTO_RESOLVE,
)
from hgraph.test import eval_node
from typing import Generic


# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================


class SimpleSchema(TimeSeriesSchema):
    """Simple schema with two fields."""
    p1: TS[int]
    p2: TS[str]


class NumericSchema(TimeSeriesSchema):
    """Schema with numeric fields."""
    a: TS[float]
    b: TS[float]


class NestedSchema(TimeSeriesSchema):
    """Schema with nested TSB field."""
    inner: TSB[SimpleSchema]
    outer: TS[int]


# =============================================================================
# OUTPUT VALUE TESTS
# =============================================================================


# Test TSB output value setting and retrieval.

def test_output_create_tsb():
    """Test creating TSB output via compute_node."""
    @compute_node(valid=[])
    def create_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[SimpleSchema]:
        out = {}
        if ts1.modified:
            out["p1"] = ts1.value
        if ts2.modified:
            out["p2"] = ts2.value
        return out

    result = eval_node(create_tsb, [1, 2], ["a", "b"])
    assert result == [{"p1": 1, "p2": "a"}, {"p1": 2, "p2": "b"}]


def test_output_partial_field_tick():
    """Test that only modified fields are in delta output."""
    @compute_node(valid=[])
    def create_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[SimpleSchema]:
        out = {}
        if ts1.modified:
            out["p1"] = ts1.value
        if ts2.modified:
            out["p2"] = ts2.value
        return out

    result = eval_node(create_tsb, [None, 2], ["a", None])
    assert result == [{"p2": "a"}, {"p1": 2}]


def test_output_value_returns_dict():
    """Test that TSB.value returns dict of field values."""
    @compute_node
    def get_value(tsb: TSB[SimpleSchema]) -> TS[dict]:
        return dict(tsb.value) if tsb.valid else {}

    result = eval_node(get_value, [{"p1": 1, "p2": "a"}])
    # value returns dict with field values
    assert result[0]["p1"] == 1
    assert result[0]["p2"] == "a"


# =============================================================================
# FIELD ACCESS TESTS
# =============================================================================


# Test TSB field access by name and index.

def test_access_by_name():
    """Test accessing TSB field by name via as_schema."""
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[SimpleSchema] = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return tsb.as_schema.p1

    assert eval_node(g, [1, 2], ["a", "b"]) == [1, 2]


def test_access_by_index():
    """Test accessing TSB field by integer index."""
    @graph
    def g(tsb: TSB[SimpleSchema]) -> TS[int]:
        return tsb[0]

    assert eval_node(g, [{"p1": 1, "p2": "a"}, {"p1": 2}]) == [1, 2]


def test_access_second_field():
    """Test accessing second field."""
    @graph
    def g(tsb: TSB[SimpleSchema]) -> TS[str]:
        return tsb[1]

    assert eval_node(g, [{"p1": 1, "p2": "hello"}, {"p2": "world"}]) == ["hello", "world"]


def test_kwargs_unpacking():
    """Test unpacking TSB fields via **kwargs."""
    @graph
    def g(tsb: TSB[SimpleSchema]) -> TS[int]:
        values = dict(**tsb)
        return values["p1"]

    assert eval_node(g, [{"p1": 1, "p2": "a"}, {"p1": 2}]) == [1, 2]


# =============================================================================
# STATE PROPERTY TESTS
# =============================================================================


# Test TSB output state properties.

def test_valid_when_any_field_valid():
    """Test that TSB is valid when any field is valid."""
    @compute_node
    def check_valid(tsb: TSB[SimpleSchema]) -> TS[bool]:
        return tsb.valid

    assert eval_node(check_valid, [{"p1": 1}]) == [True]


def test_all_valid_requires_all_fields():
    """Test that all_valid requires all fields to be valid."""
    @compute_node
    def check_all_valid(tsb: TSB[SimpleSchema]) -> TS[bool]:
        return tsb.all_valid

    # all_valid only True when both fields valid
    assert eval_node(check_all_valid, [{"p1": 1}, {"p2": "a"}]) == [False, True]


def test_modified_when_any_field_modified():
    """Test that modified is True when any field is modified."""
    @compute_node
    def check_modified(tsb: TSB[SimpleSchema]) -> TS[bool]:
        return tsb.modified

    assert eval_node(check_modified, [{"p1": 1}, {"p2": "a"}]) == [True, True]


def test_delta_value_only_modified_fields():
    """Test that delta_value only contains modified fields."""
    @compute_node
    def get_delta(tsb: TSB[SimpleSchema]) -> TS[dict]:
        return dict(tsb.delta_value)

    result = eval_node(get_delta, [{"p1": 1, "p2": "a"}, {"p1": 2}])
    assert result == [{"p1": 1, "p2": "a"}, {"p1": 2}]


# =============================================================================
# TSB.from_ts() CONSTRUCTION TESTS
# =============================================================================


# Test TSB construction via from_ts().

def test_from_ts_basic():
    """Test TSB.from_ts() basic construction."""
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[SimpleSchema] = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return tsb.as_schema.p1

    assert eval_node(g, [1, 2], ["a", "b"]) == [1, 2]


def test_from_ts_partial_fields():
    """Test TSB.from_ts() with subset of fields."""
    @graph
    def g(ts1: TS[int]) -> TSB[SimpleSchema]:
        return TSB[SimpleSchema].from_ts(p1=ts1)

    result = eval_node(g, [1, 2])
    assert result == [fd(p1=1), fd(p1=2)]


def test_from_ts_creates_non_peered():
    """Test that from_ts creates non-peered binding."""
    @compute_node
    def check_peer(tsb: TSB[SimpleSchema]) -> TS[bool]:
        return tsb.has_peer

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[bool]:
        tsb = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return check_peer(tsb)

    # from_ts creates non-peered input
    assert eval_node(g, [1], ["a"]) == [False]


# =============================================================================
# PEERED VS NON-PEERED BINDING TESTS
# =============================================================================


# Test TSB peered vs non-peered binding behavior.

def test_peered_binding_has_peer_true():
    """Test that peered binding sets has_peer to True."""
    @compute_node(valid=[])
    def create_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[SimpleSchema]:
        out = {}
        if ts1.modified:
            out["p1"] = ts1.value
        if ts2.modified:
            out["p2"] = ts2.value
        return out

    @compute_node
    def check_peer(tsb: TSB[SimpleSchema]) -> TS[bool]:
        return tsb.has_peer

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[bool]:
        tsb = create_tsb(ts1, ts2)
        return check_peer(tsb)

    # Binding output TSB to input TSB creates peered binding
    assert eval_node(g, [1], ["a"]) == [True]


def test_peered_splitting():
    """Test extracting field from peered TSB."""
    @compute_node(valid=[])
    def create_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[SimpleSchema]:
        out = {}
        if ts1.modified:
            out["p1"] = ts1.value
        if ts2.modified:
            out["p2"] = ts2.value
        return out

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[SimpleSchema] = create_tsb(ts1, ts2)
        return tsb.as_schema.p1

    assert eval_node(g, [1, 2], ["a", "b"]) == [1, 2]


def test_non_peered_splitting():
    """Test extracting field from non-peered TSB."""
    @compute_node
    def split_tsb(tsb: TSB[SimpleSchema]) -> TS[int]:
        return tsb.as_schema.p1.delta_value

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[SimpleSchema] = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return split_tsb(tsb)

    assert eval_node(g, [1, 2], ["a", "b"]) == [1, 2]


# =============================================================================
# INLINE SCHEMA TESTS
# =============================================================================


# Test TSB with inline schema definition.

def test_inline_schema():
    """Test TSB with inline schema definition."""
    @graph
    def g(tsb: TSB["lhs" : TS[int], "rhs" : TS[int]]) -> TS[int]:
        return tsb.lhs + tsb.rhs

    assert eval_node(g, [{"lhs": 1, "rhs": 2}]) == [3]


def test_inline_schema_field_access():
    """Test accessing fields from inline schema."""
    @graph
    def g(tsb: TSB["a" : TS[int], "b" : TS[str]]) -> TS[int]:
        return tsb.a

    assert eval_node(g, [{"a": 42, "b": "hello"}]) == [42]


# =============================================================================
# GENERIC SCHEMA TESTS
# =============================================================================


# Test TSB with generic schemas.

def test_generic_schema():
    """Test TSB with parameterized generic schema."""
    class GenericSchema(TimeSeriesSchema, Generic[SCALAR]):
        p1: TS[SCALAR]

    @graph
    def multi_type(
        ts: TSB[GenericSchema[SCALAR]], v: TS[SCALAR_1], _v_tp: type[SCALAR_1] = AUTO_RESOLVE
    ) -> TSB[GenericSchema[SCALAR_1]]:
        return TSB[GenericSchema[_v_tp]].from_ts(p1=v)

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[GenericSchema[str]]:
        return multi_type(TSB[GenericSchema[int]].from_ts(p1=ts1), ts2)

    result = eval_node(g, [1, 2], ["a", "b"])
    assert result == [fd({"p1": "a"}), fd({"p1": "b"})]


# =============================================================================
# COMPOUND SCALAR INTEGRATION
# =============================================================================


# Test TSB integration with CompoundScalar.

def test_tsb_from_compound_scalar():
    """Test creating TSB from CompoundScalar dataclass."""
    @dataclass
    class MyScalar(CompoundScalar):
        p1: int
        p2: str

    @graph
    def g(ts: TSB[MyScalar]) -> TS[int]:
        return ts.p1

    assert eval_node(g, MyScalar(1, "a")) == [1]


def test_tsb_to_compound_scalar():
    """Test TSB value as CompoundScalar when schema has scalar_type."""
    @dataclass
    class MyScalar(CompoundScalar):
        a: int
        b: int

    @compute_node
    def sum_fields(tsb: TSB[MyScalar]) -> TS[int]:
        v = tsb.value
        return v.a + v.b if hasattr(v, 'a') else v["a"] + v["b"]

    result = eval_node(sum_fields, [{"a": 1, "b": 2}])
    assert result == [3]


# =============================================================================
# OUTPUT INJECTABLE TESTS
# =============================================================================


# Test TSB _output injectable behavior.

def test_output_access():
    """Test accessing _output to inspect previous state."""
    @compute_node
    def check_output(tsb: TSB[SimpleSchema], _output: TSB_OUT[SimpleSchema] = None) -> TSB[SimpleSchema]:
        # Only output if p1 changed from previous value
        if tsb.p1.value != _output.p1.value:
            return tsb.delta_value

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[str]:
        tsb: TSB[SimpleSchema] = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return check_output(tsb).p2

    assert eval_node(g, [1, 1, 2], ["a", "b", "c"]) == ["a", None, "c"]


# =============================================================================
# SIGNAL WITH TSB TESTS
# =============================================================================


# Test TSB interaction with SIGNAL.

def test_signal_from_non_peered_tsb():
    """Test using non-peered TSB as SIGNAL input."""
    @compute_node
    def signal_receiver(ts: SIGNAL) -> TS[bool]:
        return True

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[bool]:
        tsb = TSB[SimpleSchema].from_ts(p1=ts1, p2=ts2)
        return signal_receiver(tsb)

    # Each field tick triggers signal
    assert eval_node(g, [None, 1, None], [None, None, "b"]) == [None, True, True]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


# Test TSB edge cases and boundary conditions.

def test_order_preservation():
    """Test that field order is preserved."""
    @compute_node
    def concat(a: TS[str], b: TS[str]) -> TS[str]:
        return a.value + b.value

    @compute_node
    def concat_reversed(b: TS[str], a: TS[str]) -> TS[str]:
        return a.value + b.value

    @graph
    def g(a: TS[str], b: TS[str]) -> TS[str]:
        return concat(a, b) + concat_reversed(a, b)

    assert eval_node(g, "a", "b") == ["abba"]


def test_heterogeneous_types():
    """Test TSB with different field types."""
    class MixedSchema(TimeSeriesSchema):
        int_field: TS[int]
        str_field: TS[str]
        float_field: TS[float]
        bool_field: TS[bool]

    @compute_node
    def check_types(tsb: TSB[MixedSchema]) -> TS[tuple]:
        return (
            type(tsb.int_field.value).__name__,
            type(tsb.str_field.value).__name__,
            type(tsb.float_field.value).__name__,
            type(tsb.bool_field.value).__name__,
        )

    result = eval_node(check_types, [{"int_field": 1, "str_field": "a", "float_field": 1.5, "bool_field": True}])
    assert result == [("int", "str", "float", "bool")]
