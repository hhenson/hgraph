# Schema Integration Design

## Overview

This document describes the integration between Python's `HgTypeMetaData` hierarchy and the C++ `TypeMeta`/`TSMeta` schema system. The goal is to provide seamless interop where Python type metadata can obtain corresponding C++ schemas for use in the C++ runtime.

## Current State

### Value Schema Integration (Implemented)

The scalar/value type integration is **implemented** on the current branch. The `cpp_type` property on scalar metadata classes returns the corresponding C++ `TypeMeta*`.

#### Implemented Classes

| Python Class | C++ Function | Returns |
|--------------|--------------|---------|
| `HgAtomicType` | `_hgraph.value.get_scalar_type_meta(py_type)` | `TypeMeta*` |
| `HgTupleCollectionScalarType` | `_hgraph.value.get_dynamic_list_type_meta(element_cpp)` | `TypeMeta*` |
| `HgTupleFixedScalarType` | `_hgraph.value.get_tuple_type_meta(element_types)` | `TypeMeta*` |
| `HgSetScalarType` | `_hgraph.value.get_set_type_meta(element_cpp)` | `TypeMeta*` |
| `HgDictScalarType` | `_hgraph.value.get_dict_type_meta(key_cpp, value_cpp)` | `TypeMeta*` |
| `HgCompoundScalarType` | `_hgraph.value.get_compound_scalar_type_meta(fields, py_type, name)` | `TypeMeta*` |
| `HgCppNativeScalarType` | `wrapped_type._get_expanded_cpp_type()` | `TypeMeta*` |

#### Pattern

```python
@property
def cpp_type(self):
    """Get the C++ TypeMeta for this scalar type."""
    if not self.is_resolved:
        return None
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        return None
    try:
        import hgraph._hgraph as _hgraph
        return _hgraph.value.get_scalar_type_meta(self.py_type)
    except (ImportError, AttributeError):
        return None
```

### Time-Series Schema Integration (Removed - Needs Re-integration)

The TS type integration was **implemented on ts_value_25** but has been **removed** from the current branch (ts_value_26). This needs to be re-integrated.

#### ts_value_25 Implementation

The ts_value_25 branch had `cpp_type` properties on all TS metadata classes that used a `TSTypeRegistry`:

| Python Class | C++ Registry Method | Returns |
|--------------|---------------------|---------|
| `HgTSTypeMetaData` | `TSTypeRegistry.instance().ts(scalar_cpp)` | `TSMeta*` |
| `HgTSSTypeMetaData` | `TSTypeRegistry.instance().tss(element_cpp)` | `TSMeta*` |
| `HgTSDTypeMetaData` | `TSTypeRegistry.instance().tsd(key_cpp, value_cpp)` | `TSMeta*` |
| `HgTSLTypeMetaData` | `TSTypeRegistry.instance().tsl(element_cpp, fixed_size)` | `TSMeta*` |
| `HgTSWTypeMetaData` | `TSTypeRegistry.instance().tsw_duration(value_cpp, time_range, min_time_range)` | `TSMeta*` |
| `HgREFTypeMetaData` | `TSTypeRegistry.instance().ref(referenced_cpp)` | `TSMeta*` |
| `HgSignalMetaData` | `TSTypeRegistry.instance().signal()` | `TSMeta*` |
| `HgTimeSeriesSchemaTypeMetaData` | `TSTypeRegistry.instance().tsb(fields, name, python_type)` | `TSMeta*` |
| `HgTSBTypeMetaData` | `TSTypeRegistry.instance().tsb(fields, name, python_type)` | `TSMeta*` |

## Required Changes

### 1. C++ TSTypeRegistry Implementation

Create a `TSTypeRegistry` class in C++ that provides factory methods for TS schemas:

```cpp
// cpp/include/hgraph/types/time_series/ts_type_registry.h

class TSTypeRegistry {
public:
    static TSTypeRegistry& instance();

    // Scalar TS
    const TSMeta* ts(const TypeMeta* value_type);

    // Collection TS
    const TSMeta* tss(const TypeMeta* element_type);
    const TSMeta* tsd(const TypeMeta* key_type, const TSMeta* value_ts);
    const TSMeta* tsl(const TSMeta* element_ts, size_t fixed_size = 0);

    // Window TS (size-based and duration-based)
    const TSMeta* tsw(const TypeMeta* element_type, size_t period, size_t min_period = 0);
    const TSMeta* tsw_duration(const TypeMeta* element_type,
                               engine_time_delta_t time_range,
                               engine_time_delta_t min_time_range = 0);

    // Bundle TS
    const TSMeta* tsb(const std::vector<std::pair<std::string, const TSMeta*>>& fields,
                      const std::string& name,
                      nb::object python_type = nb::none());

    // Reference TS
    const TSMeta* ref(const TSMeta* referenced_ts);

    // Signal
    const TSMeta* signal();

private:
    // Caching for deduplication
    std::unordered_map<...> ts_cache_;
    std::unordered_map<...> tsb_cache_;
    // ... etc
};
```

### 2. Python Bindings for TSTypeRegistry

Expose the `TSTypeRegistry` to Python:

```cpp
// cpp/src/cpp/api/python/py_ts_type_registry.cpp

static void register_ts_type_registry(nb::module_& m) {
    nb::class_<TSTypeRegistry>(m, "TSTypeRegistry", "Registry for time-series type schemas")
        .def_static("instance", &TSTypeRegistry::instance, nb::rv_policy::reference)
        .def("ts", &TSTypeRegistry::ts, nb::rv_policy::reference)
        .def("tss", &TSTypeRegistry::tss, nb::rv_policy::reference)
        .def("tsd", &TSTypeRegistry::tsd, nb::rv_policy::reference)
        .def("tsl", &TSTypeRegistry::tsl, nb::rv_policy::reference,
             "element_ts"_a, "fixed_size"_a = 0)
        .def("tsw", &TSTypeRegistry::tsw, nb::rv_policy::reference,
             "element_type"_a, "period"_a, "min_period"_a = 0)
        .def("tsw_duration", &TSTypeRegistry::tsw_duration, nb::rv_policy::reference,
             "element_type"_a, "time_range"_a, "min_time_range"_a = engine_time_delta_t{0})
        .def("tsb", &TSTypeRegistry::tsb, nb::rv_policy::reference,
             "fields"_a, "name"_a, "python_type"_a = nb::none())
        .def("ref", &TSTypeRegistry::ref, nb::rv_policy::reference)
        .def("signal", &TSTypeRegistry::signal, nb::rv_policy::reference);
}
```

### 3. Re-add cpp_type Properties to Python TS Metadata Classes

Re-add the `cpp_type` property to each TS metadata class. Example for `HgTSTypeMetaData`:

```python
# hgraph/_types/_ts_meta_data.py

class HgTSTypeMetaData(HgTimeSeriesTypeMetaData):
    # ... existing code ...

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

### 4. Files to Modify

#### Python Files (re-add cpp_type):

| File | Class | Notes |
|------|-------|-------|
| `hgraph/_types/_time_series_meta_data.py` | `HgTimeSeriesTypeMetaData` | Add base `cpp_type` returning `None` |
| `hgraph/_types/_ts_meta_data.py` | `HgTSTypeMetaData` | `TSTypeRegistry.ts()` |
| `hgraph/_types/_tss_meta_data.py` | `HgTSSTypeMetaData` | `TSTypeRegistry.tss()` |
| `hgraph/_types/_tsd_meta_data.py` | `HgTSDTypeMetaData` | `TSTypeRegistry.tsd()` |
| `hgraph/_types/_tsl_meta_data.py` | `HgTSLTypeMetaData` | `TSTypeRegistry.tsl()` |
| `hgraph/_types/_tsw_meta_data.py` | `HgTSWTypeMetaData` | `TSTypeRegistry.tsw()` or `tsw_duration()` |
| `hgraph/_types/_tsb_meta_data.py` | `HgTimeSeriesSchemaTypeMetaData`, `HgTSBTypeMetaData` | `TSTypeRegistry.tsb()` |
| `hgraph/_types/_ref_meta_data.py` | `HgREFTypeMetaData` | `TSTypeRegistry.ref()` |
| `hgraph/_types/_ts_signal_meta_data.py` | `HgSignalMetaData` | `TSTypeRegistry.signal()` |

#### C++ Files (new):

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/time_series/ts_type_registry.h` | TSTypeRegistry declaration |
| `cpp/src/cpp/types/time_series/ts_type_registry.cpp` | TSTypeRegistry implementation |
| `cpp/src/cpp/api/python/py_ts_type_registry.cpp` | Python bindings |

## Type Mapping Summary

### Python to C++ Type Mapping

```
HgTypeMetaData Hierarchy          C++ Schema System
─────────────────────────         ─────────────────
HgScalarTypeMetaData         →    TypeMeta (via TypeRegistry)
├── HgAtomicType             →    Scalar TypeMeta
├── HgTupleCollectionScalarType → List TypeMeta (dynamic)
├── HgTupleFixedScalarType   →    Tuple TypeMeta
├── HgSetScalarType          →    Set TypeMeta
├── HgDictScalarType         →    Map TypeMeta
└── HgCompoundScalarType     →    Bundle TypeMeta

HgTimeSeriesTypeMetaData     →    TSMeta (via TSTypeRegistry)
├── HgTSTypeMetaData         →    TSValueMeta
├── HgTSSTypeMetaData        →    TSSTypeMeta
├── HgTSDTypeMetaData        →    TSDTypeMeta
├── HgTSLTypeMetaData        →    TSLTypeMeta
├── HgTSWTypeMetaData        →    TSWTypeMeta
├── HgTSBTypeMetaData        →    TSBTypeMeta
├── HgREFTypeMetaData        →    TSRefMeta
└── HgSignalMetaData         →    SIGNALMeta
```

## Design Decisions

### 1. Registry Singleton Pattern

Both `TypeRegistry` (value) and `TSTypeRegistry` (time-series) use singleton pattern:
- Ensures schema deduplication
- Thread-safe initialization
- Consistent schema identity (pointer equality)

### 2. Lazy Construction

Schemas are constructed on first request and cached:
- Avoids upfront cost of building all possible schemas
- Python feature flag check before any C++ calls
- Graceful fallback when C++ unavailable

### 3. Reference Semantics

Registry methods return raw pointers (`const TSMeta*`):
- Schemas are owned by the registry
- Lifetime matches process lifetime
- Python bindings use `rv_policy::reference`

### 4. Python Type Binding

TSB schemas optionally bind a Python type:
- Enables proper `to_python()` conversion (returns dataclass, not dict)
- Stored in schema for runtime access
- `None` is valid (returns dict on conversion)

## Testing Strategy

### Unit Tests

1. **Schema creation**: Verify each registry method creates correct schema
2. **Deduplication**: Same inputs return same schema pointer
3. **Python round-trip**: `cpp_type` property returns valid schema
4. **Feature flag**: Returns `None` when C++ disabled

### Integration Tests

1. **Wiring**: TS types with `cpp_type` wire correctly
2. **Runtime**: C++ runtime can use schemas from Python metadata
3. **Mixed mode**: Python and C++ schemas interoperate

## References

- User Guide: `01_SCHEMA.md`
- Design: `01_SCHEMA.md`, `03_TIME_SERIES.md`
- Implementation: ts_value_25 branch (reference for cpp_type implementations)
