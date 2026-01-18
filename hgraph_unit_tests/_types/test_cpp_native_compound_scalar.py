"""
Tests for cpp_native flag and CppNative[T] wrapper functionality.

This module tests:
1. The __cpp_native__ class attribute on CompoundScalar
2. The CppNative[T] type wrapper
3. Inheritance of cpp_native flag
4. Type metadata parsing and resolution
5. C++ type selection based on cpp_native flag
6. Integration tests with TS[CppNative[...]] in graph execution
"""

from dataclasses import dataclass
from typing import _GenericAlias

import pytest

from hgraph._types._scalar_types import CompoundScalar
from hgraph._types._cpp_native_type import CppNative
from hgraph._types._cpp_native_meta_data import HgCppNativeScalarType
from hgraph._types._scalar_type_meta_data import HgCompoundScalarType, HgScalarTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph import graph, TS, eq_
from hgraph.test import eval_node


# ============================================================================
# Test Fixtures and Helpers
# ============================================================================

def _skip_if_no_cpp():
    """Skip test if C++ module is not available."""
    try:
        from hgraph._feature_switch import is_feature_enabled
        if not is_feature_enabled("use_cpp"):
            pytest.skip("C++ not enabled")
        import hgraph._hgraph as _hgraph
        _ = _hgraph.value
    except (ImportError, AttributeError):
        pytest.skip("C++ module not available")


def _get_value_module():
    """Get the C++ value module."""
    import hgraph._hgraph as _hgraph
    return _hgraph.value


# ============================================================================
# Test CompoundScalar Classes
# ============================================================================

@dataclass(frozen=True)
class DefaultScalar(CompoundScalar):
    """Default compound scalar (cpp_native=False by default)."""
    x: int
    y: float


@dataclass(frozen=True)
class CppNativeScalar(CompoundScalar, cpp_native=True):
    """Compound scalar with cpp_native=True."""
    x: int
    y: float


@dataclass(frozen=True)
class DerivedFromDefault(DefaultScalar):
    """Inherits from default (should have cpp_native=False)."""
    z: str


@dataclass(frozen=True)
class DerivedFromCppNative(CppNativeScalar):
    """Inherits from cpp_native=True (should have cpp_native=True)."""
    z: str


@dataclass(frozen=True)
class OverriddenToFalse(CppNativeScalar, cpp_native=False):
    """Explicitly overrides cpp_native to False."""
    w: int


@dataclass(frozen=True)
class GraphTestScalar(CompoundScalar):
    """Test scalar for graph execution tests (default: opaque storage)."""
    value: int
    name: str = ""


@dataclass(frozen=True)
class GraphTestCppNativeScalar(CompoundScalar, cpp_native=True):
    """Test scalar for graph execution tests (cpp_native=True: expanded storage)."""
    value: int
    name: str = ""


# ============================================================================
# Tests for __cpp_native__ Flag
# ============================================================================

def test_default_scalar_has_cpp_native_false():
    """Default CompoundScalar should have __cpp_native__ = False."""
    assert DefaultScalar.__cpp_native__ is False


def test_cpp_native_true_on_class():
    """CompoundScalar with cpp_native=True should have __cpp_native__ = True."""
    assert CppNativeScalar.__cpp_native__ is True


def test_inheritance_from_default():
    """Derived class should inherit __cpp_native__ = False from default parent."""
    assert DerivedFromDefault.__cpp_native__ is False


def test_inheritance_from_cpp_native():
    """Derived class should inherit __cpp_native__ = True from cpp_native parent."""
    assert DerivedFromCppNative.__cpp_native__ is True


def test_explicit_override_to_false():
    """Derived class can explicitly override __cpp_native__ to False."""
    assert OverriddenToFalse.__cpp_native__ is False


def test_base_compound_scalar_has_false():
    """Base CompoundScalar class should have __cpp_native__ = False."""
    assert CompoundScalar.__cpp_native__ is False


# ============================================================================
# Tests for CppNative[T] Type Wrapper
# ============================================================================

def test_cpp_native_is_generic():
    """CppNative should be a generic type."""
    assert hasattr(CppNative, '__class_getitem__')


def test_cpp_native_wrapper_creates_generic_alias():
    """CppNative[T] should create a generic alias."""
    wrapper = CppNative[DefaultScalar]
    assert isinstance(wrapper, _GenericAlias)
    assert wrapper.__origin__ is CppNative
    assert wrapper.__args__ == (DefaultScalar,)


# ============================================================================
# Tests for HgCppNativeScalarType Metadata
# ============================================================================

def test_parse_cpp_native_wrapper():
    """CppNative[T] should parse to HgCppNativeScalarType."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert isinstance(meta, HgCppNativeScalarType)
    assert isinstance(meta.wrapped_type, HgCompoundScalarType)
    assert meta.wrapped_type.py_type is DefaultScalar


def test_parse_plain_compound_scalar():
    """Plain CompoundScalar should parse to HgCompoundScalarType."""
    meta = HgScalarTypeMetaData.parse_type(DefaultScalar)
    assert isinstance(meta, HgCompoundScalarType)
    assert not isinstance(meta, HgCppNativeScalarType)


def test_cpp_native_py_type():
    """HgCppNativeScalarType.py_type should return CppNative[T]."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert meta.py_type.__origin__ is CppNative
    assert meta.py_type.__args__ == (DefaultScalar,)


def test_cpp_native_scalar_py_type():
    """HgCppNativeScalarType.scalar_py_type should return the unwrapped type."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert meta.scalar_py_type is DefaultScalar


def test_cpp_native_is_resolved():
    """HgCppNativeScalarType should be resolved when wrapped type is resolved."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert meta.is_resolved is True


def test_cpp_native_str_representation():
    """String representation should be CppNative[TypeName]."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert str(meta) == "CppNative[DefaultScalar]"


def test_cpp_native_equality():
    """Two HgCppNativeScalarType with same wrapped type should be equal."""
    meta1 = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    meta2 = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert meta1 == meta2


def test_cpp_native_inequality():
    """Two HgCppNativeScalarType with different wrapped types should not be equal."""
    meta1 = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    meta2 = HgScalarTypeMetaData.parse_type(CppNative[CppNativeScalar])
    assert meta1 != meta2


def test_cpp_native_hash():
    """HgCppNativeScalarType should be hashable."""
    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert isinstance(hash(meta), int)


def test_cpp_native_matches_wrapped():
    """HgCppNativeScalarType should match the wrapped HgCompoundScalarType."""
    cpp_native_meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    compound_meta = HgScalarTypeMetaData.parse_type(DefaultScalar)
    assert cpp_native_meta.matches(compound_meta)


# ============================================================================
# Tests for C++ Type Selection
# ============================================================================

def test_default_scalar_uses_opaque_storage():
    """Default CompoundScalar should use opaque Python object storage."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(DefaultScalar)
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind.name == "Scalar"


def test_cpp_native_scalar_uses_expanded_storage():
    """CompoundScalar with cpp_native=True should use expanded Bundle storage."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(CppNativeScalar)
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind.name == "Bundle"


def test_cpp_native_wrapper_uses_expanded_storage():
    """CppNative[T] wrapper should always use expanded Bundle storage."""
    _skip_if_no_cpp()

    meta = HgScalarTypeMetaData.parse_type(CppNative[DefaultScalar])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind.name == "Bundle"


def test_derived_from_cpp_native_uses_expanded_storage():
    """Derived class inheriting cpp_native=True should use expanded storage."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(DerivedFromCppNative)
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind.name == "Bundle"


def test_overridden_to_false_uses_opaque_storage():
    """Class with cpp_native explicitly set to False should use opaque storage."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(OverriddenToFalse)
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind.name == "Scalar"


# ============================================================================
# Tests for Expanded vs Opaque Helper Methods
# ============================================================================

def test_get_expanded_cpp_type_returns_bundle():
    """_get_expanded_cpp_type should return Bundle TypeMeta."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(DefaultScalar)
    cpp_type = meta._get_expanded_cpp_type()

    assert cpp_type is not None
    assert cpp_type.kind.name == "Bundle"


def test_get_opaque_cpp_type_returns_scalar():
    """_get_opaque_cpp_type should return Scalar TypeMeta (nb::object)."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(DefaultScalar)
    cpp_type = meta._get_opaque_cpp_type()

    assert cpp_type is not None
    assert cpp_type.kind.name == "Scalar"


def test_expanded_type_has_correct_field_count():
    """Expanded TypeMeta should have correct number of fields."""
    _skip_if_no_cpp()

    meta = HgCompoundScalarType(DefaultScalar)
    cpp_type = meta._get_expanded_cpp_type()

    assert cpp_type is not None
    assert cpp_type.field_count == 2


# ============================================================================
# Integration Tests
# ============================================================================

def test_parse_and_cpp_type_for_default():
    """Parse default CompoundScalar and verify opaque cpp_type."""
    _skip_if_no_cpp()

    meta = HgTypeMetaData.parse_type(DefaultScalar)
    assert isinstance(meta, HgCompoundScalarType)
    assert meta.cpp_type.kind.name == "Scalar"


def test_parse_and_cpp_type_for_cpp_native_flag():
    """Parse cpp_native=True CompoundScalar and verify expanded cpp_type."""
    _skip_if_no_cpp()

    meta = HgTypeMetaData.parse_type(CppNativeScalar)
    assert isinstance(meta, HgCompoundScalarType)
    assert meta.cpp_type.kind.name == "Bundle"


def test_parse_and_cpp_type_for_wrapper():
    """Parse CppNative[T] wrapper and verify expanded cpp_type."""
    _skip_if_no_cpp()

    meta = HgTypeMetaData.parse_type(CppNative[DefaultScalar])
    assert isinstance(meta, HgCppNativeScalarType)
    assert meta.cpp_type.kind.name == "Bundle"


# ============================================================================
# Graph Execution Tests with TS[CppNative[...]]
# ============================================================================

def test_ts_default_scalar_basic():
    """TS with default CompoundScalar should work (opaque storage)."""
    @graph
    def g(ts: TS[GraphTestScalar]) -> TS[int]:
        return ts.value

    result = eval_node(g, [GraphTestScalar(value=42, name="test")])
    assert result == [42]


def test_ts_cpp_native_flag_scalar_basic():
    """TS with cpp_native=True CompoundScalar should work (expanded storage)."""
    @graph
    def g(ts: TS[GraphTestCppNativeScalar]) -> TS[int]:
        return ts.value

    result = eval_node(g, [GraphTestCppNativeScalar(value=42, name="test")])
    assert result == [42]


def test_ts_cpp_native_wrapper_basic():
    """TS[CppNative[...]] wrapper should work (expanded storage)."""
    @graph
    def g(ts: TS[CppNative[GraphTestScalar]]) -> TS[int]:
        return ts.value

    result = eval_node(g, [GraphTestScalar(value=42, name="test")])
    assert result == [42]


def test_ts_default_scalar_multiple_values():
    """TS with default CompoundScalar should handle multiple values."""
    @graph
    def g(ts: TS[GraphTestScalar]) -> TS[int]:
        return ts.value

    result = eval_node(g, [
        GraphTestScalar(value=1),
        GraphTestScalar(value=2),
        GraphTestScalar(value=3)
    ])
    assert result == [1, 2, 3]


def test_ts_cpp_native_wrapper_multiple_values():
    """TS[CppNative[...]] should handle multiple values."""
    @graph
    def g(ts: TS[CppNative[GraphTestScalar]]) -> TS[int]:
        return ts.value

    result = eval_node(g, [
        GraphTestScalar(value=1),
        GraphTestScalar(value=2),
        GraphTestScalar(value=3)
    ])
    assert result == [1, 2, 3]


def test_ts_default_scalar_string_field():
    """TS with default CompoundScalar should handle string fields."""
    @graph
    def g(ts: TS[GraphTestScalar]) -> TS[str]:
        return ts.name

    result = eval_node(g, [GraphTestScalar(value=1, name="hello")])
    assert result == ["hello"]


def test_ts_cpp_native_wrapper_string_field():
    """TS[CppNative[...]] should handle string fields."""
    @graph
    def g(ts: TS[CppNative[GraphTestScalar]]) -> TS[str]:
        return ts.name

    result = eval_node(g, [GraphTestScalar(value=1, name="hello")])
    assert result == ["hello"]


def test_ts_default_scalar_equality():
    """Equality comparison should work with default CompoundScalar."""
    @graph
    def g(lhs: TS[GraphTestScalar], rhs: TS[GraphTestScalar]) -> TS[bool]:
        return eq_(lhs, rhs)

    result = eval_node(g,
        lhs=[GraphTestScalar(value=1), GraphTestScalar(value=2)],
        rhs=[GraphTestScalar(value=1), GraphTestScalar(value=3)]
    )
    assert result == [True, False]


def test_ts_cpp_native_wrapper_equality():
    """Equality comparison should work with CppNative wrapper."""
    @graph
    def g(lhs: TS[CppNative[GraphTestScalar]], rhs: TS[CppNative[GraphTestScalar]]) -> TS[bool]:
        return eq_(lhs, rhs)

    result = eval_node(g,
        lhs=[GraphTestScalar(value=1), GraphTestScalar(value=2)],
        rhs=[GraphTestScalar(value=1), GraphTestScalar(value=3)]
    )
    assert result == [True, False]


def test_ts_mixed_default_and_cpp_native_same_behavior():
    """Default and cpp_native=True should produce same results."""
    @graph
    def g_default(ts: TS[GraphTestScalar]) -> TS[int]:
        return ts.value

    @graph
    def g_cpp_native(ts: TS[GraphTestCppNativeScalar]) -> TS[int]:
        return ts.value

    inputs_default = [GraphTestScalar(value=i) for i in range(5)]
    inputs_cpp_native = [GraphTestCppNativeScalar(value=i) for i in range(5)]

    result_default = eval_node(g_default, inputs_default)
    result_cpp_native = eval_node(g_cpp_native, inputs_cpp_native)

    assert result_default == result_cpp_native == [0, 1, 2, 3, 4]


def test_ts_cpp_native_wrapper_same_as_flag():
    """CppNative[T] wrapper should produce same results as cpp_native=True flag."""
    @graph
    def g_wrapper(ts: TS[CppNative[GraphTestScalar]]) -> TS[int]:
        return ts.value

    @graph
    def g_flag(ts: TS[GraphTestCppNativeScalar]) -> TS[int]:
        return ts.value

    inputs_wrapper = [GraphTestScalar(value=i) for i in range(5)]
    inputs_flag = [GraphTestCppNativeScalar(value=i) for i in range(5)]

    result_wrapper = eval_node(g_wrapper, inputs_wrapper)
    result_flag = eval_node(g_flag, inputs_flag)

    assert result_wrapper == result_flag == [0, 1, 2, 3, 4]


# ============================================================================
# Tests for TSB (TimeSeriesBundle) Schema Behavior
# ============================================================================

def test_tsb_from_ts_basic_execution():
    """TSB.from_ts should continue to work with default CompoundScalar."""
    from hgraph import graph, TS, TSB
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph.test import eval_node

    class MySchema(TimeSeriesSchema):
        x: TS[int]
        y: TS[str]

    @graph
    def g(a: TS[int], b: TS[str]) -> TSB[MySchema]:
        return TSB[MySchema].from_ts(x=a, y=b)

    result = eval_node(g, a=[1, 2], b=["hello", None, "world"])
    assert result[0]["x"] == 1
    assert result[0]["y"] == "hello"
    assert result[1]["x"] == 2
    assert result[2]["y"] == "world"
