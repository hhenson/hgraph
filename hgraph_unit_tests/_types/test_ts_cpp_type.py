"""
Tests for cpp_type property integration on time-series type metadata classes.

This module tests the cpp_type property on HgTimeSeriesTypeMetaData subclasses,
which maps Python time-series type metadata to corresponding C++ TSMeta* schemas.

Test Categories:
1. Basic cpp_type tests for each TS type (TS, TSS, TSD, TSL, TSW, TSB, REF, SIGNAL)
2. Deduplication tests (same type returns same TSMeta*)
3. Feature flag tests (cpp_type returns None when C++ disabled)
4. Unresolved type tests (cpp_type returns None for generic types)
5. Nested type tests (TSD[str, TS[int]], TSL[TS[int], Size[3]])
"""

from datetime import timedelta
from typing import TypeVar

import pytest

from hgraph._feature_switch import is_feature_enabled


# ============================================================================
# Test Fixtures and Helpers
# ============================================================================


def _skip_if_no_cpp():
    """Skip test if C++ module is not available."""
    try:
        if not is_feature_enabled("use_cpp"):
            pytest.skip("C++ not enabled")
        import hgraph._hgraph as _hgraph
        # Check if TSTypeRegistry is available
        _ = _hgraph.TSTypeRegistry
    except (ImportError, AttributeError):
        pytest.skip("C++ module or TSTypeRegistry not available")


def _get_ts_kind():
    """Get the C++ TSKind enum."""
    import hgraph._hgraph as _hgraph
    return _hgraph.TSKind


def _get_ts_type_registry():
    """Get the C++ TSTypeRegistry instance."""
    import hgraph._hgraph as _hgraph
    return _hgraph.TSTypeRegistry.instance()


# ============================================================================
# Basic cpp_type Tests for Each TS Type
# ============================================================================


class TestTSTypeCppType:
    """Tests for TS[T] cpp_type property."""

    def test_ts_int_cpp_type_returns_ts_value_kind(self):
        """TS[int] cpp_type should return TSMeta with TSKind.TSValue."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts_int = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        cpp = ts_int.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSValue

    def test_ts_str_cpp_type(self):
        """TS[str] cpp_type should return valid TSMeta."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts_str = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(str))
        cpp = ts_str.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSValue

    def test_ts_float_cpp_type(self):
        """TS[float] cpp_type should return valid TSMeta."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts_float = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
        cpp = ts_float.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSValue
        assert cpp.value_type is not None


class TestTSSTypeCppType:
    """Tests for TSS[T] cpp_type property."""

    def test_tss_int_cpp_type_returns_tss_kind(self):
        """TSS[int] cpp_type should return TSMeta with TSKind.TSS."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tss_meta_data import HgTSSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        tss_int = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        cpp = tss_int.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSS

    def test_tss_str_cpp_type(self):
        """TSS[str] cpp_type should return valid TSMeta."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tss_meta_data import HgTSSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        tss_str = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(str))
        cpp = tss_str.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSS
        assert cpp.value_type is not None


class TestTSDTypeCppType:
    """Tests for TSD[K, V] cpp_type property."""

    def test_tsd_str_ts_int_cpp_type_returns_tsd_kind(self):
        """TSD[str, TS[int]] cpp_type should return TSMeta with TSKind.TSD."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tsd = HgTSDTypeMetaData(key_type, value_ts)
        cpp = tsd.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSD

    def test_tsd_cpp_type_has_key_and_element_types(self):
        """TSD cpp_type should have key_type and element_ts properties."""
        _skip_if_no_cpp()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tsd = HgTSDTypeMetaData(key_type, value_ts)
        cpp = tsd.cpp_type

        assert cpp is not None
        assert cpp.key_type is not None
        assert cpp.element_ts is not None


class TestTSLTypeCppType:
    """Tests for TSL[TS, Size] cpp_type property."""

    def test_tsl_ts_int_size3_cpp_type_returns_tsl_kind(self):
        """TSL[TS[int], Size[3]] cpp_type should return TSMeta with TSKind.TSL."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import Size

        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        size_tp = HgScalarTypeMetaData.parse_type(Size[3])
        tsl = HgTSLTypeMetaData(element_ts, size_tp)
        cpp = tsl.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSL

    def test_tsl_cpp_type_has_element_ts_and_fixed_size(self):
        """TSL cpp_type should have element_ts and fixed_size properties."""
        _skip_if_no_cpp()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import Size

        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        size_tp = HgScalarTypeMetaData.parse_type(Size[5])
        tsl = HgTSLTypeMetaData(element_ts, size_tp)
        cpp = tsl.cpp_type

        assert cpp is not None
        assert cpp.element_ts is not None
        assert cpp.fixed_size == 5

    def test_tsl_dynamic_size_cpp_type(self):
        """TSL with dynamic size (SIZE type var) should have fixed_size=0."""
        _skip_if_no_cpp()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import Size

        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        # Size[0] represents dynamic size
        size_tp = HgScalarTypeMetaData.parse_type(Size[0])
        tsl = HgTSLTypeMetaData(element_ts, size_tp)
        cpp = tsl.cpp_type

        assert cpp is not None
        assert cpp.fixed_size == 0  # Dynamic size


class TestTSWTypeCppType:
    """Tests for TSW[T, size, min_size] cpp_type property."""

    def test_tsw_int_tick_based_cpp_type_returns_tsw_kind(self):
        """Tick-based TSW cpp_type should return TSMeta with TSKind.TSW."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsw_meta_data import HgTSWTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import WindowSize

        scalar_type = HgScalarTypeMetaData.parse_type(int)
        size_tp = HgScalarTypeMetaData.parse_type(WindowSize[10])
        min_size_tp = HgScalarTypeMetaData.parse_type(WindowSize[5])
        tsw = HgTSWTypeMetaData(scalar_type, size_tp, min_size_tp)
        cpp = tsw.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSW

    def test_tsw_tick_based_cpp_type_properties(self):
        """Tick-based TSW should have period and min_period properties."""
        _skip_if_no_cpp()

        from hgraph._types._tsw_meta_data import HgTSWTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import WindowSize

        scalar_type = HgScalarTypeMetaData.parse_type(float)
        size_tp = HgScalarTypeMetaData.parse_type(WindowSize[20])
        min_size_tp = HgScalarTypeMetaData.parse_type(WindowSize[10])
        tsw = HgTSWTypeMetaData(scalar_type, size_tp, min_size_tp)
        cpp = tsw.cpp_type

        assert cpp is not None
        assert cpp.value_type is not None
        # Check tick-based window properties
        assert cpp.is_duration_based == False
        assert cpp.period == 20
        assert cpp.min_period == 10


class TestTSBTypeCppType:
    """Tests for TSB[Schema] cpp_type property."""

    def test_tsb_simple_schema_cpp_type_returns_tsb_kind(self):
        """TSB with simple schema should return TSMeta with TSKind.TSB."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
        from hgraph._types._tsb_type import TimeSeriesSchema
        from hgraph._types._ts_type import TS

        class SimpleSchema(TimeSeriesSchema):
            value: TS[int]

        schema_meta = HgTimeSeriesSchemaTypeMetaData(SimpleSchema)
        tsb = HgTSBTypeMetaData(schema_meta)
        cpp = tsb.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSB

    def test_tsb_cpp_type_has_fields(self):
        """TSB cpp_type should have fields with correct structure."""
        _skip_if_no_cpp()

        from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
        from hgraph._types._tsb_type import TimeSeriesSchema
        from hgraph._types._ts_type import TS

        class TwoFieldSchema(TimeSeriesSchema):
            x: TS[int]
            y: TS[float]

        schema_meta = HgTimeSeriesSchemaTypeMetaData(TwoFieldSchema)
        tsb = HgTSBTypeMetaData(schema_meta)
        cpp = tsb.cpp_type

        assert cpp is not None
        assert cpp.field_count == 2
        assert len(cpp.fields) == 2

        # Verify field names are present
        field_names = [f.name for f in cpp.fields]
        assert "x" in field_names
        assert "y" in field_names


class TestTimeSeriesSchemaTypeCppType:
    """Tests for HgTimeSeriesSchemaTypeMetaData cpp_type property."""

    def test_schema_cpp_type_returns_tsb_kind(self):
        """Schema type cpp_type should return TSMeta with TSKind.TSB."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData
        from hgraph._types._tsb_type import TimeSeriesSchema
        from hgraph._types._ts_type import TS

        class MySchema(TimeSeriesSchema):
            name: TS[str]

        schema_meta = HgTimeSeriesSchemaTypeMetaData(MySchema)
        cpp = schema_meta.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSB


class TestREFTypeCppType:
    """Tests for REF[TS] cpp_type property."""

    def test_ref_ts_int_cpp_type_returns_ref_kind(self):
        """REF[TS[int]] cpp_type should return TSMeta with TSKind.REF."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ref_meta_data import HgREFTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        inner_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        ref = HgREFTypeMetaData(inner_ts)
        cpp = ref.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.REF

    def test_ref_cpp_type_has_element_ts(self):
        """REF cpp_type should have element_ts pointing to referenced type."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ref_meta_data import HgREFTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        inner_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(str))
        ref = HgREFTypeMetaData(inner_ts)
        cpp = ref.cpp_type

        assert cpp is not None
        assert cpp.element_ts is not None
        assert cpp.element_ts.kind == TSKind.TSValue


class TestSignalTypeCppType:
    """Tests for SIGNAL cpp_type property."""

    def test_signal_cpp_type_returns_signal_kind(self):
        """SIGNAL cpp_type should return TSMeta with TSKind.SIGNAL."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ts_signal_meta_data import HgSignalMetaData

        signal = HgSignalMetaData()
        cpp = signal.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.SIGNAL


# ============================================================================
# Deduplication Tests
# ============================================================================


class TestCppTypeDeduplication:
    """Tests that same Python type metadata returns same C++ TSMeta pointer."""

    def test_ts_int_deduplication(self):
        """Same TS[int] type should return identical TSMeta*."""
        _skip_if_no_cpp()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts1 = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        ts2 = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))

        cpp1 = ts1.cpp_type
        cpp2 = ts2.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2  # Same pointer

    def test_tss_deduplication(self):
        """Same TSS[int] type should return identical TSMeta*."""
        _skip_if_no_cpp()

        from hgraph._types._tss_meta_data import HgTSSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        tss1 = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tss2 = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(int))

        cpp1 = tss1.cpp_type
        cpp2 = tss2.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2

    def test_tsd_deduplication(self):
        """Same TSD[str, TS[int]] type should return identical TSMeta*."""
        _skip_if_no_cpp()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))

        tsd1 = HgTSDTypeMetaData(key_type, value_ts)
        tsd2 = HgTSDTypeMetaData(key_type, value_ts)

        cpp1 = tsd1.cpp_type
        cpp2 = tsd2.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2

    def test_signal_deduplication(self):
        """Multiple SIGNAL instances should return identical TSMeta*."""
        _skip_if_no_cpp()

        from hgraph._types._ts_signal_meta_data import HgSignalMetaData

        signal1 = HgSignalMetaData()
        signal2 = HgSignalMetaData()

        cpp1 = signal1.cpp_type
        cpp2 = signal2.cpp_type

        assert cpp1 is not None
        assert cpp2 is not None
        assert cpp1 is cpp2  # SIGNAL is a singleton

    def test_different_types_return_different_ts_meta(self):
        """Different TS types should return different TSMeta*."""
        _skip_if_no_cpp()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        ts_int = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        ts_float = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))

        cpp_int = ts_int.cpp_type
        cpp_float = ts_float.cpp_type

        assert cpp_int is not None
        assert cpp_float is not None
        assert cpp_int is not cpp_float


# ============================================================================
# Feature Flag Tests
# ============================================================================


class TestFeatureFlagDisabled:
    """Tests that cpp_type returns None when C++ is disabled."""

    def test_ts_cpp_type_returns_none_when_cpp_disabled(self, monkeypatch):
        """cpp_type should return None when HGRAPH_USE_CPP=0."""
        import os

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        # Skip if cpp_type property is not yet implemented
        ts_int = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        if not hasattr(ts_int, 'cpp_type'):
            pytest.skip("cpp_type property not yet implemented")

        # Save original state
        original_env = os.environ.get("HGRAPH_USE_CPP")

        try:
            # Disable C++ via environment variable
            monkeypatch.setenv("HGRAPH_USE_CPP", "0")

            # Clear any cached feature state
            from hgraph import _feature_switch
            if hasattr(_feature_switch, "_feature_switch"):
                _feature_switch._feature_switch._cache.clear()

            # When C++ is disabled, cpp_type should return None
            if not is_feature_enabled("use_cpp"):
                assert ts_int.cpp_type is None
        finally:
            # Restore original state
            if original_env is not None:
                os.environ["HGRAPH_USE_CPP"] = original_env
            else:
                os.environ.pop("HGRAPH_USE_CPP", None)
            # Clear cached feature state
            from hgraph import _feature_switch
            if hasattr(_feature_switch, "_feature_switch"):
                _feature_switch._feature_switch._cache.clear()


# ============================================================================
# Unresolved Type Tests
# ============================================================================


class TestUnresolvedTypes:
    """Tests that cpp_type returns None for unresolved (generic) types."""

    def test_unresolved_ts_cpp_type_returns_none(self):
        """TS with unresolved type variable should return None for cpp_type."""
        _skip_if_no_cpp()

        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeVar

        T = TypeVar("T")
        unresolved_scalar = HgScalarTypeVar(T)
        ts_unresolved = HgTSTypeMetaData(unresolved_scalar)

        # Verify it's not resolved
        assert not ts_unresolved.is_resolved

        # cpp_type should return None for unresolved types
        assert ts_unresolved.cpp_type is None

    def test_unresolved_tss_cpp_type_returns_none(self):
        """TSS with unresolved type variable should return None for cpp_type."""
        _skip_if_no_cpp()

        from hgraph._types._tss_meta_data import HgTSSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeVar

        T = TypeVar("T")
        unresolved_scalar = HgScalarTypeVar(T)
        tss_unresolved = HgTSSTypeMetaData(unresolved_scalar)

        assert not tss_unresolved.is_resolved
        assert tss_unresolved.cpp_type is None

    def test_unresolved_tsd_key_cpp_type_returns_none(self):
        """TSD with unresolved key type should return None for cpp_type."""
        _skip_if_no_cpp()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeVar, HgScalarTypeMetaData

        K = TypeVar("K")
        unresolved_key = HgScalarTypeVar(K)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tsd_unresolved = HgTSDTypeMetaData(unresolved_key, value_ts)

        assert not tsd_unresolved.is_resolved
        assert tsd_unresolved.cpp_type is None

    def test_unresolved_tsl_size_cpp_type_returns_none(self):
        """TSL with unresolved size should return None for cpp_type."""
        _skip_if_no_cpp()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeVar, HgScalarTypeMetaData

        SIZE = TypeVar("SIZE")
        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        unresolved_size = HgScalarTypeVar(SIZE)
        tsl_unresolved = HgTSLTypeMetaData(element_ts, unresolved_size)

        assert not tsl_unresolved.is_resolved
        assert tsl_unresolved.cpp_type is None


# ============================================================================
# Nested Type Tests
# ============================================================================


class TestNestedTypes:
    """Tests for nested time-series types."""

    def test_tsd_nested_ts_cpp_type(self):
        """TSD[str, TS[int]] should have correct nested element type."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        tsd = HgTSDTypeMetaData(key_type, value_ts)
        cpp = tsd.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSD
        # element_ts should be TS[int]
        assert cpp.element_ts is not None
        assert cpp.element_ts.kind == TSKind.TSValue

    def test_tsl_nested_ts_cpp_type(self):
        """TSL[TS[float], Size[3]] should have correct nested element type."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import Size

        element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
        size_tp = HgScalarTypeMetaData.parse_type(Size[3])
        tsl = HgTSLTypeMetaData(element_ts, size_tp)
        cpp = tsl.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSL
        # element_ts should be TS[float]
        assert cpp.element_ts is not None
        assert cpp.element_ts.kind == TSKind.TSValue
        assert cpp.fixed_size == 3

    def test_ref_nested_ts_cpp_type(self):
        """REF[TS[str]] should have correct nested element type."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._ref_meta_data import HgREFTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        inner_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(str))
        ref = HgREFTypeMetaData(inner_ts)
        cpp = ref.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.REF
        # element_ts should be TS[str]
        assert cpp.element_ts is not None
        assert cpp.element_ts.kind == TSKind.TSValue

    def test_deeply_nested_tsd_tsl_cpp_type(self):
        """TSD[int, TSL[TS[str], Size[2]]] should have correct nested types."""
        _skip_if_no_cpp()
        TSKind = _get_ts_kind()

        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._scalar_types import Size

        # Build TSL[TS[str], Size[2]]
        inner_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(str))
        size_tp = HgScalarTypeMetaData.parse_type(Size[2])
        tsl = HgTSLTypeMetaData(inner_ts, size_tp)

        # Build TSD[int, TSL[...]]
        key_type = HgScalarTypeMetaData.parse_type(int)
        tsd = HgTSDTypeMetaData(key_type, tsl)
        cpp = tsd.cpp_type

        assert cpp is not None
        assert cpp.kind == TSKind.TSD

        # element_ts should be TSL
        assert cpp.element_ts is not None
        assert cpp.element_ts.kind == TSKind.TSL
        assert cpp.element_ts.fixed_size == 2

        # TSL's element_ts should be TS[str]
        assert cpp.element_ts.element_ts is not None
        assert cpp.element_ts.element_ts.kind == TSKind.TSValue


# ============================================================================
# Cross-Type Consistency Tests
# ============================================================================


class TestCrossTypeConsistency:
    """Tests for consistency across different ways of creating the same type."""

    def test_ts_via_parse_type_vs_direct_construction(self):
        """TS type created via parse_type should match direct construction."""
        _skip_if_no_cpp()

        from hgraph._types._type_meta_data import HgTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._ts_type import TS

        # Via parse_type
        parsed = HgTypeMetaData.parse_type(TS[int])

        # Direct construction
        direct = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))

        cpp_parsed = parsed.cpp_type
        cpp_direct = direct.cpp_type

        assert cpp_parsed is not None
        assert cpp_direct is not None
        assert cpp_parsed is cpp_direct  # Same TSMeta* due to deduplication

    def test_tsd_via_parse_type_vs_direct_construction(self):
        """TSD type created via parse_type should match direct construction."""
        _skip_if_no_cpp()

        from hgraph._types._type_meta_data import HgTypeMetaData
        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
        from hgraph._types._ts_meta_data import HgTSTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._tsd_type import TSD
        from hgraph._types._ts_type import TS

        # Via parse_type
        parsed = HgTypeMetaData.parse_type(TSD[str, TS[int]])

        # Direct construction
        key_type = HgScalarTypeMetaData.parse_type(str)
        value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
        direct = HgTSDTypeMetaData(key_type, value_ts)

        cpp_parsed = parsed.cpp_type
        cpp_direct = direct.cpp_type

        assert cpp_parsed is not None
        assert cpp_direct is not None
        assert cpp_parsed is cpp_direct
