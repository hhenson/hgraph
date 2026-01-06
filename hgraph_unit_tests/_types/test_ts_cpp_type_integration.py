"""
Tests for cpp_type property integration between HgTimeSeriesTypeMetaData and C++ TSMeta.

This module tests the cpp_type property on time-series metadata classes,
which maps Python time-series type metadata to corresponding C++ TSMeta* schemas.
"""

from datetime import timedelta

import pytest

from hgraph import TS, TSB, TSL, TSD, TSS, REF, SIGNAL, CompoundScalar, Size
from hgraph._types._scalar_types import WindowSize
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._types._tsw_type import TSW


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


def _get_hgraph_module():
    """Get the C++ hgraph module."""
    import hgraph._hgraph as _hgraph
    return _hgraph


class TestBundle(CompoundScalar):
    """Test bundle schema for TSB tests."""
    x: int
    y: float


# Size types for TSW tests
Size5 = Size[5]
Size10 = Size[10]
MinSize2 = Size[2]

# Duration types for TSW tests
Duration1s = WindowSize[timedelta(seconds=1)]
Duration2s = WindowSize[timedelta(seconds=2)]
Duration5s = WindowSize[timedelta(seconds=5)]
MinDuration0 = WindowSize[timedelta(0)]
MinDuration500ms = WindowSize[timedelta(milliseconds=500)]
MinDuration2s = WindowSize[timedelta(seconds=2)]


# ============================================================================
# TS[T] Tests
# ============================================================================

@pytest.mark.parametrize("scalar_type", [int, float, str, bool])
def test_ts_returns_ts_value_meta(scalar_type):
    """TS[T] should return TSValueMeta with correct kind."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TS[scalar_type])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TS
    assert cpp_type.is_scalar_ts


def test_ts_caching_same_type():
    """Same TS type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    meta1 = HgTypeMetaData.parse_type(TS[int])
    meta2 = HgTypeMetaData.parse_type(TS[int])

    assert meta1.cpp_type is meta2.cpp_type


def test_ts_different_types_different_meta():
    """Different TS types should return different TSMeta pointers."""
    _skip_if_no_cpp()

    cpp_int = HgTypeMetaData.parse_type(TS[int]).cpp_type
    cpp_float = HgTypeMetaData.parse_type(TS[float]).cpp_type

    assert cpp_int is not cpp_float


# ============================================================================
# TSW Size-based Tests
# ============================================================================

def test_tsw_size_based_returns_tsw_type_meta():
    """Size-based TSW should return TSWTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSW


def test_tsw_size_based_is_not_time_based():
    """Size-based TSW should have is_time_based = False."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type

    assert cpp_type.is_time_based is False


@pytest.mark.parametrize("size_type,expected_size", [
    (Size5, 5),
    (Size10, 10),
])
def test_tsw_size_based_has_correct_size(size_type, expected_size):
    """Size-based TSW should have correct size value."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[int, size_type, MinSize2]).cpp_type

    assert cpp_type.size == expected_size


def test_tsw_size_based_has_correct_min_size():
    """Size-based TSW should have correct min_size value."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type

    assert cpp_type.min_size == 2


def test_tsw_size_based_different_sizes_different_meta():
    """TSW with different sizes should return different TSMeta pointers."""
    _skip_if_no_cpp()

    cpp5 = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type
    cpp10 = HgTypeMetaData.parse_type(TSW[int, Size10, MinSize2]).cpp_type

    assert cpp5 is not cpp10


def test_tsw_size_based_caching():
    """Same size-based TSW should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type

    assert cpp1 is cpp2


# ============================================================================
# TSW Duration-based Tests
# ============================================================================

def test_tsw_duration_based_returns_tsw_type_meta():
    """Duration-based TSW should return TSWTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration500ms])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSW


def test_tsw_duration_based_is_time_based():
    """Duration-based TSW should have is_time_based = True."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration500ms]).cpp_type

    assert cpp_type.is_time_based is True


@pytest.mark.parametrize("duration_type,expected_duration", [
    (Duration1s, timedelta(seconds=1)),
    (Duration2s, timedelta(seconds=2)),
])
def test_tsw_duration_based_has_correct_time_range(duration_type, expected_duration):
    """Duration-based TSW should have correct time_range value."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[float, duration_type, MinDuration0]).cpp_type

    assert cpp_type.time_range == expected_duration


def test_tsw_duration_based_has_correct_min_time_range():
    """Duration-based TSW should have correct min_time_range value."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration500ms]).cpp_type

    assert cpp_type.min_time_range == timedelta(milliseconds=500)


def test_tsw_duration_based_different_durations_different_meta():
    """TSW with different durations should return different TSMeta pointers."""
    _skip_if_no_cpp()

    cpp1s = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration0]).cpp_type
    cpp2s = HgTypeMetaData.parse_type(TSW[float, Duration2s, MinDuration0]).cpp_type

    assert cpp1s is not cpp2s


def test_tsw_duration_based_caching():
    """Same duration-based TSW should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration500ms]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSW[float, Duration1s, MinDuration500ms]).cpp_type

    assert cpp1 is cpp2


def test_tsw_size_vs_duration_different_meta():
    """Size-based and duration-based TSW should return different TSMeta."""
    _skip_if_no_cpp()

    size_cpp = HgTypeMetaData.parse_type(TSW[int, Size5, MinSize2]).cpp_type
    duration_cpp = HgTypeMetaData.parse_type(TSW[int, Duration5s, MinDuration2s]).cpp_type

    assert size_cpp is not duration_cpp
    assert size_cpp.is_time_based is False
    assert duration_cpp.is_time_based is True


# ============================================================================
# TSB Tests
# ============================================================================

def test_tsb_returns_tsb_type_meta():
    """TSB should return TSBTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSB[TestBundle])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSB
    assert cpp_type.is_bundle


def test_tsb_has_correct_field_count():
    """TSB should have correct number of fields."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSB[TestBundle]).cpp_type

    assert cpp_type.field_count == 2


def test_tsb_caching():
    """Same TSB type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSB[TestBundle]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSB[TestBundle]).cpp_type

    assert cpp1 is cpp2


# ============================================================================
# TSL Tests
# ============================================================================

def test_tsl_returns_tsl_type_meta():
    """TSL should return TSLTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSL[TS[int], Size[3]])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSL
    assert cpp_type.is_collection


def test_tsl_has_correct_fixed_size():
    """TSL should have correct fixed_size value."""
    _skip_if_no_cpp()

    cpp_type = HgTypeMetaData.parse_type(TSL[TS[int], Size[3]]).cpp_type

    assert cpp_type.fixed_size == 3
    assert cpp_type.is_fixed_size


def test_tsl_caching():
    """Same TSL type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSL[TS[int], Size[3]]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSL[TS[int], Size[3]]).cpp_type

    assert cpp1 is cpp2


# ============================================================================
# TSD Tests
# ============================================================================

def test_tsd_returns_tsd_type_meta():
    """TSD should return TSDTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSD[str, TS[int]])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSD
    assert cpp_type.is_collection


def test_tsd_caching():
    """Same TSD type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSD[str, TS[int]]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSD[str, TS[int]]).cpp_type

    assert cpp1 is cpp2


def test_tsd_different_key_types_different_meta():
    """TSD with different key types should return different TSMeta."""
    _skip_if_no_cpp()

    cpp_str = HgTypeMetaData.parse_type(TSD[str, TS[int]]).cpp_type
    cpp_int = HgTypeMetaData.parse_type(TSD[int, TS[int]]).cpp_type

    assert cpp_str is not cpp_int


# ============================================================================
# TSS Tests
# ============================================================================

def test_tss_returns_tss_type_meta():
    """TSS should return TSSTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(TSS[int])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.TSS
    assert cpp_type.is_collection


def test_tss_caching():
    """Same TSS type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(TSS[int]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(TSS[int]).cpp_type

    assert cpp1 is cpp2


# ============================================================================
# REF Tests
# ============================================================================

def test_ref_returns_ref_type_meta():
    """REF should return REFTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.REF
    assert cpp_type.is_reference


def test_ref_caching():
    """Same REF type should return same TSMeta pointer."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(REF[TS[int]]).cpp_type
    cpp2 = HgTypeMetaData.parse_type(REF[TS[int]]).cpp_type

    assert cpp1 is cpp2


# ============================================================================
# SIGNAL Tests
# ============================================================================

def test_signal_returns_signal_type_meta():
    """SIGNAL should return SignalTypeMeta."""
    _skip_if_no_cpp()
    _hgraph = _get_hgraph_module()

    meta = HgTypeMetaData.parse_type(SIGNAL)
    cpp_type = meta.cpp_type

    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSTypeKind.SIGNAL
    assert cpp_type.is_signal


def test_signal_caching():
    """SIGNAL should always return same TSMeta pointer (singleton)."""
    _skip_if_no_cpp()

    cpp1 = HgTypeMetaData.parse_type(SIGNAL).cpp_type
    cpp2 = HgTypeMetaData.parse_type(SIGNAL).cpp_type

    assert cpp1 is cpp2
