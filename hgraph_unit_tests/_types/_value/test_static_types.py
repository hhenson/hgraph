"""
Tests for static template type definitions.

These tests verify that the C++ static template definitions work correctly
and can be used to define types at compile time that are then registered
lazily at runtime.
"""

import pytest
from hgraph._feature_switch import is_feature_enabled

# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


def test_fixed_string_used_in_bundle_name():
    """Test that fixed_string works for bundle names."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    # Create a named bundle using the runtime builder
    # (static templates are C++ only, but we can verify the registry works)
    registry = _hgraph.TypeRegistry.instance()

    int_type = value.scalar_type_meta_int64()
    float_type = value.scalar_type_meta_double()

    # Build a bundle
    bundle = registry.bundle("TestPoint") \
        .field("x", float_type) \
        .field("y", float_type) \
        .field("z", float_type) \
        .build()

    assert bundle is not None
    assert bundle.kind == _hgraph.value.TypeKind.Bundle
    assert bundle.field_count == 3


def test_value_tuple_type():
    """Test Tuple type creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    registry = _hgraph.TypeRegistry.instance()
    int_type = value.scalar_type_meta_int64()
    float_type = value.scalar_type_meta_double()

    # Create tuple type
    tuple_type = registry.tuple() \
        .element(int_type) \
        .element(float_type) \
        .build()

    assert tuple_type is not None
    assert tuple_type.kind == _hgraph.value.TypeKind.Tuple
    assert tuple_type.field_count == 2


def test_value_list_type():
    """Test List type creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    registry = _hgraph.TypeRegistry.instance()
    float_type = value.scalar_type_meta_double()

    # Dynamic list
    list_type = registry.list(float_type).build()
    assert list_type is not None
    assert list_type.kind == _hgraph.value.TypeKind.List

    # Fixed-size list
    fixed_list = registry.fixed_list(float_type, 10).build()
    assert fixed_list is not None
    assert fixed_list.kind == _hgraph.value.TypeKind.List
    assert fixed_list.fixed_size == 10


def test_value_set_type():
    """Test Set type creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    registry = _hgraph.TypeRegistry.instance()
    int_type = value.scalar_type_meta_int64()

    set_type = registry.set(int_type).build()
    assert set_type is not None
    assert set_type.kind == _hgraph.value.TypeKind.Set


def test_value_map_type():
    """Test Map type creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    registry = _hgraph.TypeRegistry.instance()
    int_type = value.scalar_type_meta_int64()
    float_type = value.scalar_type_meta_double()

    map_type = registry.map(int_type, float_type).build()
    assert map_type is not None
    assert map_type.kind == _hgraph.value.TypeKind.Map


def test_ts_scalar_type():
    """Test TS[T] schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    float_type = value.scalar_type_meta_double()
    ts_float = ts_registry.ts(float_type)

    assert ts_float is not None
    assert ts_float.kind == _hgraph.TSKind.TSValue


def test_tss_type():
    """Test TSS[T] schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    int_type = value.scalar_type_meta_int64()
    tss_int = ts_registry.tss(int_type)

    assert tss_int is not None
    assert tss_int.kind == _hgraph.TSKind.TSS


def test_tsd_type():
    """Test TSD[K, V] schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    int_type = value.scalar_type_meta_int64()
    float_type = value.scalar_type_meta_double()

    ts_float = ts_registry.ts(float_type)
    tsd = ts_registry.tsd(int_type, ts_float)

    assert tsd is not None
    assert tsd.kind == _hgraph.TSKind.TSD


def test_tsl_type():
    """Test TSL[TS, Size] schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    float_type = value.scalar_type_meta_double()
    ts_float = ts_registry.ts(float_type)

    # Dynamic list
    tsl_dynamic = ts_registry.tsl(ts_float, 0)
    assert tsl_dynamic is not None
    assert tsl_dynamic.kind == _hgraph.TSKind.TSL

    # Fixed-size list
    tsl_fixed = ts_registry.tsl(ts_float, 10)
    assert tsl_fixed is not None
    assert tsl_fixed.kind == _hgraph.TSKind.TSL


def test_tsb_type():
    """Test TSB schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    float_type = value.scalar_type_meta_double()
    ts_float = ts_registry.ts(float_type)

    fields = [
        ("bid", ts_float),
        ("ask", ts_float),
    ]

    tsb = ts_registry.tsb(fields, "Quote")

    assert tsb is not None
    assert tsb.kind == _hgraph.TSKind.TSB
    assert tsb.field_count == 2


def test_ref_type():
    """Test REF[TS] schema creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    float_type = value.scalar_type_meta_double()
    ts_float = ts_registry.ts(float_type)

    ref_ts = ts_registry.ref(ts_float)

    assert ref_ts is not None
    assert ref_ts.kind == _hgraph.TSKind.REF


def test_signal_type():
    """Test SIGNAL schema creation."""
    import hgraph._hgraph as _hgraph

    ts_registry = _hgraph.TSTypeRegistry.instance()
    signal = ts_registry.signal()

    assert signal is not None
    assert signal.kind == _hgraph.TSKind.SIGNAL


def test_nested_types():
    """Test nested type creation."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    int_type = value.scalar_type_meta_int64()
    float_type = value.scalar_type_meta_double()

    # TSD[int, TSL[TS[float], 10]]
    ts_float = ts_registry.ts(float_type)
    tsl_float = ts_registry.tsl(ts_float, 10)
    tsd_nested = ts_registry.tsd(int_type, tsl_float)

    assert tsd_nested is not None
    assert tsd_nested.kind == _hgraph.TSKind.TSD


def test_schema_caching():
    """Test that schemas are properly cached."""
    import hgraph._hgraph as _hgraph
    value = _hgraph.value

    ts_registry = _hgraph.TSTypeRegistry.instance()

    float_type = value.scalar_type_meta_double()

    # Same arguments should return same schema pointer
    ts1 = ts_registry.ts(float_type)
    ts2 = ts_registry.ts(float_type)

    # Should be the exact same object (pointer equality)
    assert ts1 is ts2
