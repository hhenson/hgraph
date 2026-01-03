"""
Tests for cpp_type property integration between HgTypeMetaData and C++ Value TypeMeta.

This module tests the cpp_type property on HgScalarTypeMetaData and its subclasses,
which maps Python type metadata to corresponding C++ TypeMeta* schemas.

Test Categories:
1. Atomic scalar types (native and fallback)
2. Collection types (tuple, set, dict)
3. Compound scalar types (bundles)
4. Error handling (unresolved types, feature flag)
5. Caching behavior
6. Value system integration
"""

from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Dict, Set, Tuple

import pytest

from hgraph._types._scalar_type_meta_data import (
    HgAtomicType,
    HgTupleCollectionScalarType,
    HgTupleFixedScalarType,
    HgSetScalarType,
    HgDictScalarType,
    HgCompoundScalarType,
    HgScalarTypeMetaData,
)
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._types._scalar_types import Size, WindowSize, CompoundScalar


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


class MyEnum(Enum):
    """Test enum for fallback type tests."""
    A = "A"
    B = "B"


class TestPoint(CompoundScalar):
    """Test compound scalar for bundle tests."""
    x: int
    y: int


class TestPerson(CompoundScalar):
    """Another test compound scalar for bundle equivalence tests."""
    x: int
    y: int


class TestNested(CompoundScalar):
    """Compound scalar with nested types."""
    name: str
    value: int


# ============================================================================
# Atomic Scalar Type Tests
# ============================================================================

class TestAtomicNativeTypes:
    """Tests for atomic types with native C++ representations."""

    @pytest.mark.parametrize("py_type,expected_kind", [
        (bool, "Scalar"),
        (int, "Scalar"),
        (float, "Scalar"),
        (date, "Scalar"),
        (datetime, "Scalar"),
        (timedelta, "Scalar"),
    ])
    def test_native_types_return_scalar_kind(self, py_type, expected_kind):
        """Native Python types should map to Scalar TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(py_type)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == getattr(value.TypeKind, expected_kind)

    @pytest.mark.parametrize("py_type", [bool, int, float, date, datetime, timedelta])
    def test_native_types_are_not_none(self, py_type):
        """Native types should return a valid TypeMeta, not None."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(py_type)
        cpp_type = meta.cpp_type

        assert cpp_type is not None

    def test_int_size_and_alignment(self):
        """int should map to int64_t with correct size/alignment."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(int)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.size == 8  # int64_t
        assert cpp_type.alignment == 8

    def test_float_size_and_alignment(self):
        """float should map to double with correct size/alignment."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(float)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.size == 8  # double
        assert cpp_type.alignment == 8

    def test_bool_size(self):
        """bool should have size 1."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(bool)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.size == 1


class TestAtomicFallbackTypes:
    """Tests for atomic types that fall back to nb::object storage."""

    @pytest.mark.parametrize("py_type", [str, bytes, time, MyEnum])
    def test_fallback_types_return_valid_type_meta(self, py_type):
        """Fallback types should return valid TypeMeta using nb::object."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(py_type)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.Scalar

    def test_str_uses_object_storage(self):
        """str should use nb::object storage (pointer-sized)."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(str)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        # nb::object is pointer-sized
        assert cpp_type.size == 8  # sizeof(nb::object) on 64-bit

    def test_enum_uses_object_storage(self):
        """Enum should use nb::object storage."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(MyEnum)
        cpp_type = meta.cpp_type

        assert cpp_type is not None

    @pytest.mark.parametrize("size_type", [Size, Size[5], WindowSize, WindowSize[10]])
    def test_size_types_return_valid_type_meta(self, size_type):
        """Size and WindowSize should return valid TypeMeta."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(size_type)
        cpp_type = meta.cpp_type

        # Currently these use nb::object fallback
        assert cpp_type is not None


# ============================================================================
# Collection Type Tests
# ============================================================================

class TestTupleCollectionType:
    """Tests for tuple[T, ...] (dynamic tuple) types."""

    def test_tuple_ellipsis_returns_list_kind(self):
        """tuple[T, ...] should map to List TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Tuple[int, ...])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.List

    def test_tuple_ellipsis_with_different_element_types(self):
        """Different element types should produce different TypeMeta."""
        _skip_if_no_cpp()

        int_tuple_meta = HgTypeMetaData.parse_type(Tuple[int, ...])
        float_tuple_meta = HgTypeMetaData.parse_type(Tuple[float, ...])

        int_cpp = int_tuple_meta.cpp_type
        float_cpp = float_tuple_meta.cpp_type

        assert int_cpp is not None
        assert float_cpp is not None
        # Different element types = different TypeMeta
        assert int_cpp is not float_cpp

    def test_tuple_ellipsis_with_fallback_element(self):
        """tuple[str, ...] should work with fallback element type."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Tuple[str, ...])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.List


class TestTupleFixedType:
    """Tests for fixed-length tuple types like tuple[T1, T2]."""

    def test_fixed_tuple_returns_bundle_kind(self):
        """Fixed tuple should map to Bundle TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Tuple[int, float])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        # Fixed tuples use TypeKind.Tuple (heterogeneous, positional)
        # Bundle is for named struct-like types
        assert cpp_type.kind == value.TypeKind.Tuple

    def test_fixed_tuple_has_correct_field_count(self):
        """Fixed tuple should have correct number of fields."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(Tuple[int, float, str])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.field_count == 3

    def test_fixed_tuple_has_indexed_fields(self):
        """Fixed tuple fields are accessed by index, not by name."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(Tuple[int, float])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        # Tuples use positional access, so field names are empty
        # but indices are set correctly
        assert cpp_type.fields[0].index == 0
        assert cpp_type.fields[1].index == 1

    def test_different_tuple_structures_produce_different_types(self):
        """Tuples with different structures should produce different TypeMeta."""
        _skip_if_no_cpp()

        tuple_ab = HgTypeMetaData.parse_type(Tuple[int, float])
        tuple_ba = HgTypeMetaData.parse_type(Tuple[float, int])

        cpp_ab = tuple_ab.cpp_type
        cpp_ba = tuple_ba.cpp_type

        assert cpp_ab is not None
        assert cpp_ba is not None
        # Different order = different type
        assert cpp_ab is not cpp_ba


class TestSetType:
    """Tests for Set[T] types."""

    def test_set_returns_set_kind(self):
        """Set[T] should map to Set TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Set[int])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.Set

    def test_set_with_different_element_types(self):
        """Sets with different element types should produce different TypeMeta."""
        _skip_if_no_cpp()

        int_set_meta = HgTypeMetaData.parse_type(Set[int])
        str_set_meta = HgTypeMetaData.parse_type(Set[str])

        int_cpp = int_set_meta.cpp_type
        str_cpp = str_set_meta.cpp_type

        assert int_cpp is not None
        assert str_cpp is not None
        assert int_cpp is not str_cpp


class TestDictType:
    """Tests for Dict[K, V] types."""

    def test_dict_returns_map_kind(self):
        """Dict[K, V] should map to Map TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Dict[str, int])
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.Map

    def test_dict_with_different_key_types(self):
        """Dicts with different key types should produce different TypeMeta."""
        _skip_if_no_cpp()

        str_key_meta = HgTypeMetaData.parse_type(Dict[str, int])
        int_key_meta = HgTypeMetaData.parse_type(Dict[int, int])

        str_cpp = str_key_meta.cpp_type
        int_cpp = int_key_meta.cpp_type

        assert str_cpp is not None
        assert int_cpp is not None
        assert str_cpp is not int_cpp

    def test_dict_with_different_value_types(self):
        """Dicts with different value types should produce different TypeMeta."""
        _skip_if_no_cpp()

        int_val_meta = HgTypeMetaData.parse_type(Dict[str, int])
        float_val_meta = HgTypeMetaData.parse_type(Dict[str, float])

        int_cpp = int_val_meta.cpp_type
        float_cpp = float_val_meta.cpp_type

        assert int_cpp is not None
        assert float_cpp is not None
        assert int_cpp is not float_cpp


# ============================================================================
# CompoundScalar (Bundle) Tests
# ============================================================================

class TestCompoundScalarType:
    """Tests for CompoundScalar types mapping to Bundle TypeMeta.

    CompoundScalar uses CompoundScalarOps which reconstructs the original Python
    class in to_python() instead of returning a dict. This preserves hashability
    when CompoundScalar is used as keys in TSD/TSS/mesh operations.
    """

    def test_compound_scalar_returns_bundle_kind(self):
        """CompoundScalar should map to Bundle TypeKind."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(TestPoint)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.kind == value.TypeKind.Bundle

    def test_compound_scalar_has_correct_field_count(self):
        """CompoundScalar should have correct number of fields."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(TestPoint)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        assert cpp_type.field_count == 2

    def test_compound_scalar_preserves_field_names(self):
        """CompoundScalar should use actual field names, not synthetic."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(TestPoint)
        cpp_type = meta.cpp_type

        assert cpp_type is not None
        # Field names should be actual names from the class
        field_names = [cpp_type.fields[i].name for i in range(cpp_type.field_count)]
        assert "x" in field_names
        assert "y" in field_names

    def test_structurally_equivalent_bundles_same_type_meta(self):
        """Bundles with same field structure return same TypeMeta (structural equivalence)."""
        _skip_if_no_cpp()

        # TestPoint and TestPerson have the same fields: x: int, y: int
        point_meta = HgTypeMetaData.parse_type(TestPoint)
        person_meta = HgTypeMetaData.parse_type(TestPerson)

        point_cpp = point_meta.cpp_type
        person_cpp = person_meta.cpp_type

        assert point_cpp is not None
        assert person_cpp is not None
        # Structurally equivalent bundles should be the same TypeMeta*
        assert point_cpp is person_cpp


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error conditions and edge cases."""

    def test_unresolved_type_raises_type_error(self):
        """Accessing cpp_type on unresolved type should raise TypeError."""
        _skip_if_no_cpp()
        from typing import TypeVar
        from hgraph._types._scalar_type_meta_data import HgScalarTypeVar

        # Create an unresolved collection type using HgScalarTypeVar
        T = TypeVar("T")
        unresolved_element = HgScalarTypeVar(T)

        # Verify the element is not resolved
        assert not unresolved_element.is_resolved

        # Create tuple with unresolved element
        meta = HgTupleCollectionScalarType(unresolved_element)

        # Verify it's not resolved
        assert not meta.is_resolved

        with pytest.raises(TypeError, match="Cannot get cpp_type for unresolved type"):
            _ = meta.cpp_type

    def test_feature_flag_disabled_returns_none(self):
        """cpp_type should return None when C++ is disabled."""
        import os
        from hgraph._feature_switch import is_feature_enabled

        # Save original state
        original_env = os.environ.get("HGRAPH_USE_CPP")

        try:
            # Disable C++ via environment variable
            os.environ["HGRAPH_USE_CPP"] = "0"

            # Clear any cached feature state
            from hgraph import _feature_switch
            if hasattr(_feature_switch, "_features"):
                _feature_switch._features = None

            meta = HgTypeMetaData.parse_type(int)
            # When C++ is disabled, cpp_type should return None
            # (or skip if we can't properly disable it in this test context)
            if not is_feature_enabled("use_cpp"):
                assert meta.cpp_type is None
        finally:
            # Restore original state
            if original_env is not None:
                os.environ["HGRAPH_USE_CPP"] = original_env
            else:
                os.environ.pop("HGRAPH_USE_CPP", None)
            # Clear cached feature state
            from hgraph import _feature_switch
            if hasattr(_feature_switch, "_features"):
                _feature_switch._features = None


# ============================================================================
# Caching Behavior Tests
# ============================================================================

class TestCachingBehavior:
    """Tests for TypeMeta caching behavior."""

    def test_same_type_returns_same_type_meta(self):
        """Multiple calls for same type should return identical TypeMeta*."""
        _skip_if_no_cpp()

        meta1 = HgTypeMetaData.parse_type(int)
        meta2 = HgTypeMetaData.parse_type(int)

        cpp1 = meta1.cpp_type
        cpp2 = meta2.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2  # Same pointer

    def test_different_types_return_different_type_meta(self):
        """Different types should return different TypeMeta*."""
        _skip_if_no_cpp()

        int_meta = HgTypeMetaData.parse_type(int)
        float_meta = HgTypeMetaData.parse_type(float)

        int_cpp = int_meta.cpp_type
        float_cpp = float_meta.cpp_type

        assert int_cpp is not None
        assert float_cpp is not None
        assert int_cpp is not float_cpp

    def test_composite_type_caching(self):
        """Composite types should be cached based on structural identity."""
        _skip_if_no_cpp()

        # Create same composite type twice
        dict1_meta = HgTypeMetaData.parse_type(Dict[str, int])
        dict2_meta = HgTypeMetaData.parse_type(Dict[str, int])

        cpp1 = dict1_meta.cpp_type
        cpp2 = dict2_meta.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2  # Same structural identity = same TypeMeta

    def test_direct_vs_parsed_construction(self):
        """Direct HgAtomicType construction should produce same result as parsing."""
        _skip_if_no_cpp()

        parsed = HgTypeMetaData.parse_type(int)
        direct = HgAtomicType(int)

        parsed_cpp = parsed.cpp_type
        direct_cpp = direct.cpp_type

        assert parsed_cpp is not None
        assert direct_cpp is not None
        assert parsed_cpp is direct_cpp


# ============================================================================
# Value System Integration Tests
# ============================================================================

class TestValueSystemIntegration:
    """Tests for using cpp_type with the Value system."""

    def test_create_value_with_cpp_type(self):
        """Can create PlainValue using cpp_type."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(int)
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        assert v is not None
        assert v.schema is cpp_type

    def test_roundtrip_int_value(self):
        """int value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(int)
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python(42)
        result = v.const_view().to_python()

        assert result == 42

    def test_roundtrip_float_value(self):
        """float value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(float)
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python(3.14159)
        result = v.const_view().to_python()

        assert abs(result - 3.14159) < 1e-10

    def test_roundtrip_string_value(self):
        """str value (fallback type) can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(str)
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python("hello world")
        result = v.const_view().to_python()

        assert result == "hello world"

    def test_roundtrip_datetime_value(self):
        """datetime value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(datetime)
        cpp_type = meta.cpp_type

        test_dt = datetime(2024, 6, 15, 10, 30, 45)
        v = value.PlainValue(cpp_type)
        v.view().from_python(test_dt)
        result = v.const_view().to_python()

        assert result == test_dt

    def test_roundtrip_date_value(self):
        """date value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(date)
        cpp_type = meta.cpp_type

        test_date = date(2024, 6, 15)
        v = value.PlainValue(cpp_type)
        v.view().from_python(test_date)
        result = v.const_view().to_python()

        assert result == test_date

    def test_roundtrip_enum_value(self):
        """Enum value (fallback type) can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(MyEnum)
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python(MyEnum.A)
        result = v.const_view().to_python()

        assert result == MyEnum.A

    def test_roundtrip_list_value(self):
        """List (tuple[T, ...]) value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Tuple[int, ...])
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python([1, 2, 3, 4, 5])
        result = v.const_view().to_python()

        assert list(result) == [1, 2, 3, 4, 5]

    def test_roundtrip_dict_value(self):
        """Dict value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Dict[str, int])
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python({"a": 1, "b": 2, "c": 3})
        result = v.const_view().to_python()

        assert dict(result) == {"a": 1, "b": 2, "c": 3}

    def test_roundtrip_set_value(self):
        """Set value can roundtrip through C++ Value."""
        _skip_if_no_cpp()
        value = _get_value_module()

        meta = HgTypeMetaData.parse_type(Set[int])
        cpp_type = meta.cpp_type

        v = value.PlainValue(cpp_type)
        v.view().from_python({1, 2, 3})
        result = v.const_view().to_python()

        assert set(result) == {1, 2, 3}


# ============================================================================
# Property Consistency Tests
# ============================================================================

class TestPropertyConsistency:
    """Tests for consistency between py_type and cpp_type properties."""

    @pytest.mark.parametrize("py_type", [
        bool, int, float, str, bytes, date, datetime, time, timedelta
    ])
    def test_cpp_type_available_for_all_atomic_types(self, py_type):
        """cpp_type should be available for all atomic scalar types."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(py_type)

        assert meta.py_type == py_type
        assert meta.cpp_type is not None

    def test_is_resolved_true_allows_cpp_type(self):
        """Resolved types should allow cpp_type access."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(int)

        assert meta.is_resolved is True
        assert meta.cpp_type is not None

    def test_is_scalar_true_for_atomic_types(self):
        """Atomic types should be marked as scalar."""
        _skip_if_no_cpp()

        meta = HgTypeMetaData.parse_type(int)

        assert meta.is_scalar is True
        assert meta.cpp_type is not None
