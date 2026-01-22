# Task 04: Python cpp_type Properties

## Objective

Re-add `cpp_type` property to all Python TS metadata classes.

## Files to Modify

| File | Class(es) |
|------|-----------|
| `hgraph/_types/_time_series_meta_data.py` | `HgTimeSeriesTypeMetaData` |
| `hgraph/_types/_ts_meta_data.py` | `HgTSTypeMetaData` |
| `hgraph/_types/_tss_meta_data.py` | `HgTSSTypeMetaData` |
| `hgraph/_types/_tsd_meta_data.py` | `HgTSDTypeMetaData` |
| `hgraph/_types/_tsl_meta_data.py` | `HgTSLTypeMetaData` |
| `hgraph/_types/_tsw_meta_data.py` | `HgTSWTypeMetaData` |
| `hgraph/_types/_tsb_meta_data.py` | `HgTimeSeriesSchemaTypeMetaData`, `HgTSBTypeMetaData` |
| `hgraph/_types/_ref_meta_data.py` | `HgREFTypeMetaData` |
| `hgraph/_types/_ts_signal_meta_data.py` | `HgSignalMetaData` |

## Implementation Pattern

All implementations follow this pattern:

```python
@property
def cpp_type(self):
    """Get the C++ TSMeta for this time-series type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        # Type-specific logic here
        return _hgraph.TSTypeRegistry.instance().xxx(...)
    except (ImportError, AttributeError):
        return None
```

## Specific Implementations

### 1. HgTimeSeriesTypeMetaData (Base Class)

```python
# hgraph/_types/_time_series_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this time-series type.

    Returns the corresponding C++ TSMeta* that describes the time-series
    type structure. This is used by the C++ runtime to understand the
    time-series type system.

    Subclasses should override this to provide type-specific implementations.
    """
    return None  # Default - subclasses override
```

### 2. HgTSTypeMetaData

```python
# hgraph/_types/_ts_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TS[T] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        scalar_cpp = self.value_scalar_tp.cpp_type
        if scalar_cpp is None:
            return None
        return _hgraph.TSTypeRegistry.instance().ts(scalar_cpp)
    except (ImportError, AttributeError):
        return None
```

### 3. HgTSSTypeMetaData

```python
# hgraph/_types/_tss_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TSS[T] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        element_cpp = self.value_scalar_tp.cpp_type
        if element_cpp is None:
            return None
        return _hgraph.TSTypeRegistry.instance().tss(element_cpp)
    except (ImportError, AttributeError):
        return None
```

### 4. HgTSDTypeMetaData

```python
# hgraph/_types/_tsd_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TSD[K, V] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        key_cpp = self.key_tp.cpp_type
        value_cpp = self.value_tp.cpp_type  # This is a TSMeta (TS type)
        if key_cpp is None or value_cpp is None:
            return None
        return _hgraph.TSTypeRegistry.instance().tsd(key_cpp, value_cpp)
    except (ImportError, AttributeError):
        return None
```

### 5. HgTSLTypeMetaData

```python
# hgraph/_types/_tsl_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TSL[TS, Size] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        element_cpp = self.value_tp.cpp_type  # This is a TSMeta
        if element_cpp is None:
            return None
        # Get fixed size from Size type
        fixed_size = 0
        if self.size_tp.is_resolved:
            size_type = self.size_tp.py_type
            if hasattr(size_type, 'SIZE') and size_type.SIZE is not None:
                fixed_size = size_type.SIZE
        return _hgraph.TSTypeRegistry.instance().tsl(element_cpp, fixed_size)
    except (ImportError, AttributeError):
        return None
```

### 6. HgTSWTypeMetaData

```python
# hgraph/_types/_tsw_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TSW[T, size, min_size] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        from datetime import timedelta
        import hgraph._hgraph as _hgraph
        value_cpp = self.value_scalar_tp.cpp_type
        if value_cpp is None:
            return None

        # Determine if this is a time-based or size-based window
        size_type = self.size_tp.py_type
        is_time_based = hasattr(size_type, 'FIXED_SIZE') and not size_type.FIXED_SIZE

        if is_time_based:
            # Duration-based window
            time_range = timedelta(0)
            min_time_range = timedelta(0)
            if hasattr(size_type, 'TIME_RANGE') and size_type.TIME_RANGE is not None:
                time_range = size_type.TIME_RANGE
            if self.min_size_tp.is_resolved:
                min_size_type = self.min_size_tp.py_type
                if hasattr(min_size_type, 'TIME_RANGE') and min_size_type.TIME_RANGE is not None:
                    min_time_range = min_size_type.TIME_RANGE
            return _hgraph.TSTypeRegistry.instance().tsw_duration(
                value_cpp, time_range, min_time_range
            )
        else:
            # Tick-based window
            period = 0
            min_period = 0
            if hasattr(size_type, 'SIZE') and size_type.SIZE is not None:
                period = size_type.SIZE
            if self.min_size_tp.is_resolved:
                min_size_type = self.min_size_tp.py_type
                if hasattr(min_size_type, 'SIZE') and min_size_type.SIZE is not None:
                    min_period = min_size_type.SIZE
            return _hgraph.TSTypeRegistry.instance().tsw(
                value_cpp, period, min_period
            )
    except (ImportError, AttributeError):
        return None
```

### 7. HgTimeSeriesSchemaTypeMetaData

```python
# hgraph/_types/_tsb_meta_data.py (first class)

@property
def cpp_type(self):
    """Get the C++ TSMeta for this schema type (treated as anonymous TSB)."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        # Build the fields list with (name, ts_meta) pairs
        fields = []
        for name, ts_type_meta in self.meta_data_schema.items():
            ts_cpp = ts_type_meta.cpp_type
            if ts_cpp is None:
                return None
            fields.append((name, ts_cpp))
        # Get the Python scalar type (CompoundScalar) if available
        python_type = self.py_type.scalar_type()
        return _hgraph.TSTypeRegistry.instance().tsb(
            fields, self.py_type.__name__, python_type
        )
    except (ImportError, AttributeError):
        return None
```

### 8. HgTSBTypeMetaData

```python
# hgraph/_types/_tsb_meta_data.py (second class)

@property
def cpp_type(self):
    """Get the C++ TSMeta for this TSB[Schema] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        # Build the fields list with (name, ts_meta) pairs
        fields = []
        for name, ts_type_meta in self.bundle_schema_tp.meta_data_schema.items():
            ts_cpp = ts_type_meta.cpp_type
            if ts_cpp is None:
                return None
            fields.append((name, ts_cpp))
        # Get the Python scalar type (CompoundScalar) if available
        python_type = self.bundle_schema_tp.py_type.scalar_type()
        return _hgraph.TSTypeRegistry.instance().tsb(
            fields, self.bundle_schema_tp.py_type.__name__, python_type
        )
    except (ImportError, AttributeError):
        return None
```

### 9. HgREFTypeMetaData

```python
# hgraph/_types/_ref_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for this REF[TS] type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        referenced_cpp = self.value_tp.cpp_type
        if referenced_cpp is None:
            return None
        return _hgraph.TSTypeRegistry.instance().ref(referenced_cpp)
    except (ImportError, AttributeError):
        return None
```

### 10. HgSignalMetaData

```python
# hgraph/_types/_ts_signal_meta_data.py

@property
def cpp_type(self):
    """Get the C++ TSMeta for SIGNAL type."""
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        return _hgraph.TSTypeRegistry.instance().signal()
    except (ImportError, AttributeError):
        return None
```

## Test File Location

Create `hgraph_unit_tests/_types/test_ts_cpp_type.py`

## Testing Approach

```python
# hgraph_unit_tests/_types/test_ts_cpp_type.py
import pytest
from hgraph._feature_switch import is_feature_enabled


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ not enabled")
def test_ts_cpp_type():
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._hgraph import TSKind

    ts_int = HgTSTypeMetaData(HgAtomicType.parse_type(int))
    cpp = ts_int.cpp_type

    assert cpp is not None
    assert cpp.kind == TSKind.TSValue
    assert cpp.value_type is not None


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ not enabled")
def test_tss_cpp_type():
    from hgraph._types._tss_meta_data import HgTSSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._hgraph import TSKind

    tss_int = HgTSSTypeMetaData(HgAtomicType.parse_type(int))
    cpp = tss_int.cpp_type

    assert cpp is not None
    assert cpp.kind == TSKind.TSS
    assert cpp.value_type is not None


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ not enabled")
def test_tsd_cpp_type():
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType
    from hgraph._hgraph import TSKind

    key_type = HgAtomicType.parse_type(str)
    value_ts = HgTSTypeMetaData(HgAtomicType.parse_type(int))
    tsd = HgTSDTypeMetaData(key_type, value_ts)
    cpp = tsd.cpp_type

    assert cpp is not None
    assert cpp.kind == TSKind.TSD
    assert cpp.key_type is not None
    assert cpp.element_ts is not None


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ not enabled")
def test_signal_cpp_type():
    from hgraph._types._ts_signal_meta_data import HgSignalMetaData
    from hgraph._hgraph import TSKind

    signal = HgSignalMetaData()
    cpp = signal.cpp_type

    assert cpp is not None
    assert cpp.kind == TSKind.SIGNAL


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ not enabled")
def test_cpp_type_deduplication():
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    ts1 = HgTSTypeMetaData(HgAtomicType.parse_type(int))
    ts2 = HgTSTypeMetaData(HgAtomicType.parse_type(int))

    # Same schema should return same pointer
    assert ts1.cpp_type is ts2.cpp_type


def test_feature_flag_disabled(monkeypatch):
    monkeypatch.setenv("HGRAPH_USE_CPP", "0")

    # Need to reload feature switch to pick up new env var
    import hgraph._feature_switch
    import importlib
    importlib.reload(hgraph._feature_switch)

    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgAtomicType

    ts_int = HgTSTypeMetaData(HgAtomicType.parse_type(int))
    assert ts_int.cpp_type is None
```

## Implementation Order

1. Base class `HgTimeSeriesTypeMetaData` (returns None)
2. Simple types: `HgTSTypeMetaData`, `HgSignalMetaData`
3. Collection types: `HgTSSTypeMetaData`, `HgTSLTypeMetaData`
4. Complex types: `HgTSDTypeMetaData`, `HgTSWTypeMetaData`
5. Bundle types: `HgTimeSeriesSchemaTypeMetaData`, `HgTSBTypeMetaData`
6. Reference: `HgREFTypeMetaData`
