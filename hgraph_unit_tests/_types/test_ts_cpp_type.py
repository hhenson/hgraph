"""
Tests for cpp_type property on time-series metadata classes.

Tests that each HgTimeSeriesTypeMetaData subclass correctly bridges to
the C++ TSTypeRegistry via its cpp_type property.
"""

from datetime import timedelta

import pytest

from hgraph._types._type_meta_data import HgTypeMetaData


def _skip_if_no_cpp():
    """Skip test if C++ module is not available."""
    try:
        from hgraph._feature_switch import is_feature_enabled
        if not is_feature_enabled("use_cpp"):
            pytest.skip("C++ not enabled")
        import hgraph._hgraph as _hgraph
        _ = _hgraph.TSTypeRegistry
    except (ImportError, AttributeError):
        pytest.skip("C++ module not available")


# ============================================================================
# TS[T] - Scalar time-series
# ============================================================================

def test_ts_int_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(int))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSValue


def test_ts_float_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(float))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSValue


def test_ts_bool_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(bool))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSValue


def test_ts_str_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(str))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSValue


def test_ts_caching():
    """Same TS[int] should return the same TSMeta pointer."""
    _skip_if_no_cpp()
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(int))
    result1 = meta.cpp_type
    result2 = meta.cpp_type
    assert result1 is result2


# ============================================================================
# TSS[T] - Time-series set
# ============================================================================

def test_tss_int_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tss_meta_data import HgTSSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSSTypeMetaData(HgAtomicType(int))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSS


def test_tss_str_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tss_meta_data import HgTSSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSSTypeMetaData(HgAtomicType(str))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSS


# ============================================================================
# SIGNAL
# ============================================================================

def test_signal_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ts_signal_meta_data import HgSignalMetaData

    meta = HgSignalMetaData()
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.SIGNAL


def test_signal_singleton():
    """Signal should always return the same TSMeta instance."""
    _skip_if_no_cpp()
    from hgraph._types._ts_signal_meta_data import HgSignalMetaData

    meta1 = HgSignalMetaData()
    meta2 = HgSignalMetaData()
    assert meta1.cpp_type is meta2.cpp_type


# ============================================================================
# TSD[K, V] - Time-series dict
# ============================================================================

def test_tsd_int_ts_float_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSDTypeMetaData(HgAtomicType(int), HgTSTypeMetaData(HgAtomicType(float)))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSD


def test_tsd_str_ts_int_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSDTypeMetaData(HgAtomicType(str), HgTSTypeMetaData(HgAtomicType(int)))
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSD


# ============================================================================
# TSL[TS, Size] - Time-series list
# ============================================================================

def test_tsl_ts_int_fixed_size_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._types._scalar_types import Size

    ts_int = HgTSTypeMetaData(HgAtomicType(int))
    size_meta = HgAtomicType(Size[5])
    meta = HgTSLTypeMetaData(ts_int, size_meta)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSL
    assert result.fixed_size == 5


def test_tsl_dynamic_size_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._types._scalar_types import Size

    ts_int = HgTSTypeMetaData(HgAtomicType(int))
    size_meta = HgAtomicType(Size)  # dynamic (not fixed)
    meta = HgTSLTypeMetaData(ts_int, size_meta)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSL
    assert result.fixed_size == 0


# ============================================================================
# TSW[T, size, min_size] - Time-series window
# ============================================================================

def test_tsw_tick_based_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsw_meta_data import HgTSWTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._types._scalar_types import WindowSize

    value_meta = HgAtomicType(float)
    size_meta = HgAtomicType(WindowSize[10])
    min_size_meta = HgAtomicType(WindowSize[5])
    meta = HgTSWTypeMetaData(value_meta, size_meta, min_size_meta)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSW
    assert not result.is_duration_based
    assert result.period == 10
    assert result.min_period == 5


def test_tsw_duration_based_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsw_meta_data import HgTSWTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._types._scalar_types import WindowSize

    value_meta = HgAtomicType(float)
    size_meta = HgAtomicType(WindowSize[timedelta(minutes=5)])
    min_size_meta = HgAtomicType(WindowSize[timedelta(minutes=1)])
    meta = HgTSWTypeMetaData(value_meta, size_meta, min_size_meta)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSW
    assert result.is_duration_based


# ============================================================================
# TSB[Schema] - Time-series bundle
# ============================================================================

def test_tsb_simple_schema_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS

    class MySchema(TimeSeriesSchema):
        price: TS[float]
        volume: TS[int]

    meta = HgTypeMetaData.parse_type(MySchema)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSB
    assert result.field_count == 2


def test_tsb_via_tsb_wrapper_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS
    from hgraph import TSB

    class MySchema(TimeSeriesSchema):
        x: TS[int]
        y: TS[float]

    meta = HgTypeMetaData.parse_type(TSB[MySchema])
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSB
    assert result.field_count == 2


def test_tsb_with_ref_field_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS
    from hgraph._types._ref_type import REF

    class MySchema(TimeSeriesSchema):
        ref_value: REF[TS[int]]
        price: TS[float]

    meta = HgTypeMetaData.parse_type(MySchema)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSB
    assert result.field_count == 2
    field_names = [f.name for f in result.fields]
    assert "ref_value" in field_names
    assert "price" in field_names


def test_tsb_field_names():
    _skip_if_no_cpp()
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS

    class QuoteSchema(TimeSeriesSchema):
        bid: TS[float]
        ask: TS[float]

    meta = HgTypeMetaData.parse_type(QuoteSchema)
    result = meta.cpp_type
    assert result is not None
    fields = result.fields
    field_names = [f.name for f in fields]
    assert "bid" in field_names
    assert "ask" in field_names


# ============================================================================
# REF[TS] - Time-series reference
# ============================================================================

def test_ref_ts_int_cpp_type():
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    ts_int = HgTSTypeMetaData(HgAtomicType(int))
    meta = HgREFTypeMetaData(ts_int)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.REF
    assert result.element_ts is ts_int.cpp_type
    assert result.value_type is not None
    assert result.value_type.name == "TimeSeriesReference"


# ============================================================================
# Nested types
# ============================================================================

def test_tsd_nested_tsl_cpp_type():
    """TSD[str, TSL[TS[int], Size[3]]]"""
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._types._scalar_types import Size

    ts_int = HgTSTypeMetaData(HgAtomicType(int))
    tsl = HgTSLTypeMetaData(ts_int, HgAtomicType(Size[3]))
    meta = HgTSDTypeMetaData(HgAtomicType(str), tsl)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.TSD


def test_ref_nested_tsd_cpp_type():
    """REF[TSD[int, TS[float]]]"""
    _skip_if_no_cpp()
    import hgraph._hgraph as _hgraph
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    ts_float = HgTSTypeMetaData(HgAtomicType(float))
    tsd = HgTSDTypeMetaData(HgAtomicType(int), ts_float)
    meta = HgREFTypeMetaData(tsd)
    result = meta.cpp_type
    assert result is not None
    assert result.kind == _hgraph.TSKind.REF
    assert result.element_ts is tsd.cpp_type
    assert result.value_type is not None
    assert result.value_type.name == "TimeSeriesReference"


# ============================================================================
# Base class returns None
# ============================================================================

def test_base_class_cpp_type_is_none():
    """The base HgTimeSeriesTypeMetaData.cpp_type should return None."""
    from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData

    # Create a minimal concrete subclass to test the base property
    class _Stub(HgTimeSeriesTypeMetaData):
        @property
        def is_resolved(self): return True
        @property
        def py_type(self): return None
        @property
        def type_vars(self): return set()
        @property
        def generic_rank(self): return {}
        def resolve(self, *a, **k): return self
        def do_build_resolution_dict(self, *a, **k): pass
        def matches(self, tp): return False
        def scalar_type(self): return None

    stub = _Stub()
    assert stub.cpp_type is None


# ============================================================================
# Python-only mode: cpp_type returns None
# ============================================================================

def test_ts_cpp_type_returns_none_when_cpp_disabled(monkeypatch):
    """When use_cpp is disabled, cpp_type should return None."""
    monkeypatch.setenv("HGRAPH_USE_CPP", "0")
    # Clear the feature switch cache
    from hgraph._feature_switch import _feature_switch
    _feature_switch._cache.clear()

    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    meta = HgTSTypeMetaData(HgAtomicType(int))
    assert meta.cpp_type is None

    # Clean up cache
    _feature_switch._cache.clear()
