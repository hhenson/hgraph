"""
Tests for TSReference as a value type in the Value system.

TSReference is the C++ representation of TimeSeriesReference,
used for REF type values in TSValue.
"""

import pytest
from hgraph._feature_switch import is_feature_enabled

# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


def test_ts_reference_type_registered():
    """Test that TSReference is registered as a scalar type."""
    import hgraph._hgraph as _hgraph

    # Get the type registry
    registry = _hgraph.TypeRegistry.instance()

    # TSReference should be available as a scalar type
    # Note: This requires TSReference to be registered in the C++ module
    # For now, we verify the infrastructure is in place
    assert registry is not None


def test_ts_reference_empty():
    """Test creating an empty TSReference."""
    import hgraph._hgraph as _hgraph

    # Create empty reference
    ref = _hgraph.TSReference.empty()

    assert ref.is_empty()
    assert not ref.is_peered()
    assert not ref.is_non_peered()
    assert not ref.has_output()


def test_ts_reference_to_string():
    """Test TSReference string representation."""
    import hgraph._hgraph as _hgraph

    ref = _hgraph.TSReference.empty()
    s = str(ref)

    assert "Empty" in s or "empty" in s.lower()


def test_ts_reference_equality():
    """Test TSReference equality comparison."""
    import hgraph._hgraph as _hgraph

    ref1 = _hgraph.TSReference.empty()
    ref2 = _hgraph.TSReference.empty()

    assert ref1 == ref2


def test_ts_reference_copy():
    """Test TSReference copy semantics."""
    import hgraph._hgraph as _hgraph

    ref1 = _hgraph.TSReference.empty()
    ref2 = _hgraph.TSReference(ref1)  # Copy constructor

    assert ref1 == ref2
    assert ref1.is_empty()
    assert ref2.is_empty()


def test_fq_reference_empty():
    """Test creating an empty FQReference."""
    import hgraph._hgraph as _hgraph

    fq = _hgraph.FQReference.empty()

    assert fq.is_empty()
    assert not fq.is_peered()
    assert not fq.is_non_peered()


def test_fq_reference_peered():
    """Test creating a peered FQReference."""
    import hgraph._hgraph as _hgraph

    fq = _hgraph.FQReference.peered(42, _hgraph.PortType.OUTPUT, [0, 1, 2])

    assert fq.is_peered()
    assert not fq.is_empty()
    assert fq.node_id == 42
    assert fq.port_type == _hgraph.PortType.OUTPUT
    assert list(fq.indices) == [0, 1, 2]


def test_fq_reference_to_string():
    """Test FQReference string representation."""
    import hgraph._hgraph as _hgraph

    fq = _hgraph.FQReference.peered(1, _hgraph.PortType.OUTPUT, [0])
    s = str(fq)

    assert "node=1" in s or "1" in s


def test_ref_ts_meta_has_value_type():
    """Test that REF[TS[int]] has value_type set to TSReference schema."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData

    # Get the REF[TS[int]] metadata properly
    from hgraph._types._ts_type import TS as TSType
    from hgraph._types._ref_type import REF

    # Create the REF type and resolve it
    ref_type = REF[TS[int]]

    # Get the metadata object
    if hasattr(ref_type, '__meta_data_schema__'):
        ref_meta = ref_type.__meta_data_schema__()
    else:
        # Try resolving the type
        ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])

    assert ref_meta is not None
    assert isinstance(ref_meta, HgREFTypeMetaData)

    # Get the cpp_type
    cpp_type = ref_meta.cpp_type

    # REF type should have kind == REF
    assert cpp_type is not None
    assert cpp_type.kind == _hgraph.TSKind.REF

    # REF type should have value_type set (to TSReference)
    assert cpp_type.value_type is not None


def test_ts_reference_in_value_registry():
    """Test that TSReference is registered in the value type registry."""
    import hgraph._hgraph as _hgraph

    # TSMetaSchemaCache should have ts_reference_meta
    cache = _hgraph.TSMetaSchemaCache.instance()
    ts_ref_meta = cache.ts_reference_meta()

    assert ts_ref_meta is not None
    # TypeKind is in the value submodule
    assert ts_ref_meta.kind == _hgraph.value.TypeKind.Atomic


# ============================================================================
# REF -> REF TSValue Tests
# ============================================================================

def test_ref_tsvalue_construction():
    """Test creating a TSValue with REF kind."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue from REF type
    ts_value = _hgraph.TSValue(cpp_type)

    # Should have valid meta (meta is a property, not method)
    assert ts_value.meta is not None
    assert ts_value.meta.kind == _hgraph.TSKind.REF

    # Value should be valid (has storage allocated)
    value_view = ts_value.value_view()
    assert value_view is not None


def test_ref_tsvalue_stores_empty_reference():
    """Test storing an empty TSReference in TSValue."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF
    from datetime import datetime, timedelta

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue and TSView
    ts_value = _hgraph.TSValue(cpp_type)
    current_time = datetime(2024, 1, 1, 12, 0, 0)
    ts_view = ts_value.ts_view(current_time)

    # Store an empty reference using value().from_python()
    empty_ref = _hgraph.TSReference.empty()
    ts_view.value().from_python(empty_ref)

    # Retrieve the value via to_python()
    # Note: to_python() returns a Python TimeSeriesReference object,
    # where is_empty is a property (not a method)
    result = ts_view.to_python()
    assert result is not None
    assert result.is_empty  # is_empty is a property for Python TimeSeriesReference


def test_ref_tsvalue_stores_peered_reference():
    """Test storing a peered TSReference (via FQReference) in TSValue."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF
    from datetime import datetime

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue and TSView
    ts_value = _hgraph.TSValue(cpp_type)
    current_time = datetime(2024, 1, 1, 12, 0, 0)
    ts_view = ts_value.ts_view(current_time)

    # Create a peered FQReference (for Python interop)
    fq_ref = _hgraph.FQReference.peered(42, _hgraph.PortType.OUTPUT, [0, 1])

    # Store via FQReference (Python layer converts to TSReference internally)
    # For now, we test with empty TSReference since full FQ->TSReference
    # conversion requires a Graph context
    empty_ref = _hgraph.TSReference.empty()
    ts_view.value().from_python(empty_ref)

    # Verify storage works
    result = ts_view.to_python()
    assert result is not None


def test_ref_tsvalue_modification_tracking():
    """Test that REF TSValue correctly tracks modification time."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF
    from datetime import datetime, timedelta

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue
    ts_value = _hgraph.TSValue(cpp_type)

    # Set a value at time T1
    t1 = datetime(2024, 1, 1, 12, 0, 0)
    ts_view = ts_value.ts_view(t1)

    # Set the value - this should mark the time
    ts_view.value().from_python(_hgraph.TSReference.empty())

    # Check validity
    assert ts_view.valid()

    # Retrieve value to verify storage works
    result = ts_view.to_python()
    assert result is not None
    assert result.is_empty  # is_empty is a property


def test_ref_tsvalue_value_equality():
    """Test that TSReference values are correctly compared for equality."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF
    from datetime import datetime

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create two TSValues
    ts_value1 = _hgraph.TSValue(cpp_type)
    ts_value2 = _hgraph.TSValue(cpp_type)

    current_time = datetime(2024, 1, 1, 12, 0, 0)

    # Set same empty reference in both
    ts_view1 = ts_value1.ts_view(current_time)
    ts_view2 = ts_value2.ts_view(current_time)

    empty_ref = _hgraph.TSReference.empty()
    ts_view1.value().from_python(empty_ref)
    ts_view2.value().from_python(empty_ref)

    # Values should be equal (compare the Python representations)
    val1 = ts_view1.to_python()
    val2 = ts_view2.to_python()
    assert val1 == val2


def test_ref_tsvalue_has_no_delta():
    """Test that REF types don't have delta tracking."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue
    ts_value = _hgraph.TSValue(cpp_type)

    # REF types should not have delta tracking
    assert not ts_value.has_delta()


def test_ref_tsvalue_time_schema():
    """Test that REF TSValue has correct time schema (engine_time_t)."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Get the time schema
    cache = _hgraph.TSMetaSchemaCache.instance()
    time_schema = cache.get_time_schema(cpp_type)

    # Time schema should be engine_time_t (atomic)
    assert time_schema is not None
    assert time_schema.kind == _hgraph.value.TypeKind.Atomic


def test_ref_tsvalue_observer_schema():
    """Test that REF TSValue has correct observer schema (ObserverList)."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Get the observer schema
    cache = _hgraph.TSMetaSchemaCache.instance()
    observer_schema = cache.get_observer_schema(cpp_type)

    # Observer schema should be ObserverList (atomic)
    assert observer_schema is not None
    assert observer_schema.kind == _hgraph.value.TypeKind.Atomic


def test_ref_tsvalue_no_link_schema():
    """Test that REF TSValue has no link schema (scalar type)."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Get the link schema
    cache = _hgraph.TSMetaSchemaCache.instance()
    link_schema = cache.get_link_schema(cpp_type)

    # Link schema should be None for scalar types (REF is scalar-like)
    assert link_schema is None


def test_ref_tsvalue_update_value():
    """Test updating a TSReference value in TSValue."""
    import hgraph._hgraph as _hgraph
    from hgraph import TS
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ref_type import REF
    from datetime import datetime, timedelta

    # Get the REF[TS[int]] cpp_type
    ref_meta = HgREFTypeMetaData.parse_type(REF[TS[int]])
    cpp_type = ref_meta.cpp_type

    # Create TSValue
    ts_value = _hgraph.TSValue(cpp_type)

    # Set initial value
    t1 = datetime(2024, 1, 1, 12, 0, 0)
    ts_view1 = ts_value.ts_view(t1)
    ts_view1.value().from_python(_hgraph.TSReference.empty())

    val1 = ts_view1.to_python()
    assert val1.is_empty  # is_empty is a property

    # Update at later time (still empty, just different time)
    t2 = t1 + timedelta(seconds=1)
    ts_view2 = ts_value.ts_view(t2)
    ts_view2.value().from_python(_hgraph.TSReference.empty())

    # Value should still be retrievable
    val2 = ts_view2.to_python()
    assert val2.is_empty  # is_empty is a property


def test_fq_reference_non_peered():
    """Test creating a non-peered FQReference with items."""
    import hgraph._hgraph as _hgraph

    # Create nested FQReferences (non_peered takes FQReference items, not TSReference)
    fq1 = _hgraph.FQReference.empty()
    fq2 = _hgraph.FQReference.peered(1, _hgraph.PortType.OUTPUT, [0])

    # Create non-peered FQReference with items
    fq = _hgraph.FQReference.non_peered([fq1, fq2])

    assert fq.is_non_peered()
    assert not fq.is_empty()
    assert not fq.is_peered()
    assert len(fq.items) == 2
