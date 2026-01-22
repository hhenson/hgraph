# Schema Integration Implementation Plan

## Overview

This plan details the implementation of TS schema integration between Python's `HgTimeSeriesTypeMetaData` hierarchy and C++ `TSMeta` schemas via a `TSTypeRegistry`.

## Current State Analysis

### Value Schema (DONE)
- `TypeRegistry` singleton in `cpp/include/hgraph/types/value/type_registry.h`
- Python bindings in `cpp/src/cpp/types/value/type_meta_bindings.cpp`
- Functions: `get_scalar_type_meta()`, `get_dict_type_meta()`, etc.
- Python scalar metadata classes have working `cpp_type` properties

### TS Schema (TO IMPLEMENT)
- ts_value_25 branch had `cpp_type` on all TS metadata classes
- Used `TSTypeRegistry.instance()` methods
- Current branch has no TSTypeRegistry - need to create it

## Implementation Phases

### Phase 1: C++ TSMeta Schema Structures
**Files**: `cpp/include/hgraph/types/time_series/ts_meta.h`

Define schema metadata structures for time-series types:
- `TSKind` enum
- `TSMeta` struct
- `TSBFieldInfo` struct

### Phase 2: C++ TSTypeRegistry Implementation
**Files**:
- `cpp/include/hgraph/types/time_series/ts_type_registry.h`
- `cpp/src/cpp/types/time_series/ts_type_registry.cpp`

Singleton registry with:
- Factory methods for each TS kind
- Caching for deduplication
- TSB field array management

### Phase 3: Python Bindings
**Files**: `cpp/src/cpp/api/python/py_ts_type_registry.cpp`

Expose to Python:
- `TSMeta` class (read-only properties)
- `TSKind` enum
- `TSTypeRegistry` class with factory methods

### Phase 4: Python cpp_type Properties
**Files**: 9 Python metadata files in `hgraph/_types/`

Re-add `cpp_type` property to each TS metadata class.

## Implementation Order

```
1. ts_meta.h (TSMeta structures)
         │
         ▼
2. ts_type_registry.h (Registry declaration)
         │
         ▼
3. ts_type_registry.cpp (Registry implementation)
         │
         ▼
4. py_ts_type_registry.cpp (Python bindings)
         │
         ▼
5. Python cpp_type properties (9 files)
         │
         ▼
6. Tests (C++ unit tests + Python integration tests)
```

## Dependencies

| Component | Depends On |
|-----------|------------|
| TSMeta | TypeMeta (value system) |
| TSTypeRegistry | TSMeta, TypeRegistry pattern |
| Python bindings | TSTypeRegistry |
| Python cpp_type | Python bindings, scalar cpp_type |

## Risk Areas

### 1. TSB Field Lifecycle
**Risk**: TSBFieldInfo stores TSMeta pointers that must remain valid.
**Mitigation**: Registry owns all field arrays, stable addresses via arena/pool.

### 2. Python Type Storage
**Risk**: nb::object in TSMeta for TSB Python type binding.
**Mitigation**: Use optional, return dict when None.

### 3. TSW Complexity
**Risk**: Two variants (tick-based, duration-based) with different parameters.
**Mitigation**: Separate factory methods `tsw()` and `tsw_duration()`.

### 4. Cache Key Design
**Risk**: TSMeta identity must match Python metadata equality.
**Mitigation**: Use structural identity with pointer equality for nested types.

## Testing Strategy

### C++ Tests
- Schema creation for each TS kind
- Deduplication (same inputs = same pointer)
- Field access for TSB
- Duration conversion for TSW

### Python Tests
- `cpp_type` returns valid TSMeta
- Feature flag respected (None when disabled)
- Roundtrip through wiring
- Integration with existing scalar cpp_type

## Files Summary

### New C++ Files
| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/time_series/ts_meta.h` | TSMeta structures |
| `cpp/include/hgraph/types/time_series/ts_type_registry.h` | TSTypeRegistry declaration |
| `cpp/src/cpp/types/time_series/ts_type_registry.cpp` | TSTypeRegistry implementation |
| `cpp/src/cpp/api/python/py_ts_type_registry.cpp` | Python bindings |

### Modified Python Files
| File | Change |
|------|--------|
| `hgraph/_types/_time_series_meta_data.py` | Add base cpp_type (returns None) |
| `hgraph/_types/_ts_meta_data.py` | Add cpp_type using ts() |
| `hgraph/_types/_tss_meta_data.py` | Add cpp_type using tss() |
| `hgraph/_types/_tsd_meta_data.py` | Add cpp_type using tsd() |
| `hgraph/_types/_tsl_meta_data.py` | Add cpp_type using tsl() |
| `hgraph/_types/_tsw_meta_data.py` | Add cpp_type using tsw_duration() |
| `hgraph/_types/_tsb_meta_data.py` | Add cpp_type using tsb() (2 classes) |
| `hgraph/_types/_ref_meta_data.py` | Add cpp_type using ref() |
| `hgraph/_types/_ts_signal_meta_data.py` | Add cpp_type using signal() |

## Acceptance Criteria

1. All TS metadata classes have `cpp_type` property
2. `cpp_type` returns None when C++ disabled
3. Same schema inputs return same TSMeta pointer (deduplication)
4. TSB fields accessible by name and index
5. All existing tests pass
6. New tests for cpp_type integration pass
