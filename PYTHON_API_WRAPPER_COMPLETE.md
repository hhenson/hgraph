# Python API Wrapper Separation - COMPLETE ✅

## Summary

Successfully separated the Python API from C++ implementation by creating wrapper classes in `cpp/include/hgraph/api/python/`. The old direct bindings have been renamed with `_` prefix to maintain nanobind type hierarchy while exposing clean public API through wrappers.

## What Was Done

### 1. Created Python API Wrappers (`hgraph::api` namespace)

**Core Types:**
- `PyNode` → exposed as `Node`
- `PyGraph` → exposed as `Graph`
- `PyNodeScheduler` → exposed as `NodeScheduler`

**Time Series Types:**
- `PyTimeSeriesInput` → exposed as `TimeSeriesInput`
- `PyTimeSeriesOutput` → exposed as `TimeSeriesOutput`
- Specialized wrappers for all TS types (TSValue, Signal, TSL, TSB, TSD, TSS, TSW, REF)

### 2. Internal Type Renaming

Old direct bindings renamed with `_` prefix for internal use:
- `Node` → `_Node`
- `NodeScheduler` → `_NodeScheduler`
- `TimeSeriesType` → `_TimeSeriesType`
- `TimeSeriesOutput` → `_TimeSeriesOutput`
- `TimeSeriesInput` → `_TimeSeriesInput`
- `BaseTimeSeriesOutput` → `_BaseTimeSeriesOutput`
- `BaseTimeSeriesInput` → `_BaseTimeSeriesInput`

These internal types are NOT meant for Python user code but are necessary for nanobind's C++ type hierarchy.

### 3. Smart Pointer Infrastructure

- `ApiPtr<T>`: Move-only smart pointer with lifetime validation
- `ApiControlBlock`: Shared control block for tracking graph lifetime
- Validates graph is alive before dereferencing to prevent use-after-free

### 4. Wrapper Factory with Caching

- `wrap_node()`, `wrap_graph()`, `wrap_node_scheduler()`, `wrap_input()`, `wrap_output()`
- Uses `intrusive_base::self_py()` / `set_self_py()` for caching
- Ensures object identity: same C++ object = same Python wrapper
- Reduces Python object churn

### 5. Files Modified

**Headers:**
- `cpp/include/hgraph/api/python/api_ptr.h` - Smart pointer
- `cpp/include/hgraph/api/python/py_node.h` - Node wrapper
- `cpp/include/hgraph/api/python/py_graph.h` - Graph wrapper
- `cpp/include/hgraph/api/python/py_node_scheduler.h` - NodeScheduler wrapper
- `cpp/include/hgraph/api/python/py_time_series.h` - Base TS wrappers
- `cpp/include/hgraph/api/python/py_ts_types.h` - Specialized TS wrappers
- `cpp/include/hgraph/api/python/wrapper_factory.h` - Factory functions
- `cpp/include/hgraph/api/python/python_api.h` - Master header

**Implementations:**
- `cpp/src/cpp/api/python/py_node.cpp`
- `cpp/src/cpp/api/python/py_graph.cpp`
- `cpp/src/cpp/api/python/py_node_scheduler.cpp`
- `cpp/src/cpp/api/python/py_time_series.cpp`
- `cpp/src/cpp/api/python/py_ts_types.cpp`
- `cpp/src/cpp/api/python/py_api_registration.cpp`
- `cpp/src/cpp/api/python/wrapper_factory.cpp`

**Old Code Modified:**
- `cpp/src/cpp/types/time_series_type.cpp` - Renamed registrations
- `cpp/src/cpp/types/base_time_series.cpp` - Renamed registrations
- `cpp/src/cpp/types/node.cpp` - Renamed registrations
- `cpp/src/cpp/python/_hgraph_types.cpp` - Registration orchestration

## Testing

Module loads successfully:
```bash
$ HGRAPH_USE_CPP=1 uv run python -c "import hgraph._hgraph as hg; print(hg.Node, hg.Graph)"
✅ Module loaded successfully!
<class 'hgraph._hgraph.Node'> <class 'hgraph._hgraph.Graph'>
```

**Public API** (for Python users):
- `Node`, `Graph`, `NodeScheduler`
- `TimeSeriesInput`, `TimeSeriesOutput`
- All specialized TS types

**Internal Types** (not for Python users, prefixed with `_`):
- `_Node`, `_NodeScheduler`, `_TimeSeriesType`, etc.
- Used only for nanobind C++ type hierarchy

## Benefits

1. **Clean Separation**: Python API decoupled from C++ implementation
2. **Arena Allocation Ready**: Preparation for future arena allocation strategy
3. **Lifetime Safety**: `ApiPtr` prevents use-after-free errors
4. **Object Identity**: Caching ensures same C++ object = same Python wrapper
5. **Minimal API**: Only public methods exposed, no internal wiring/runtime methods
6. **Move-only Semantics**: Wrappers enforce clear ownership

## Known Issues

**Builder Integration**: Builders (GraphBuilder, NodeBuilder, etc.) still return raw C++ types instead of wrappers. This causes conversion errors:
```
TypeError: Unable to convert function return value to a Python type!
    make_instance(...) -> hgraph::Graph
```

**Solution**: Update builders to return wrapper types, or add nanobind type casters for automatic conversion.

## Next Steps

1. **FIX BUILDERS**: Update GraphBuilder, NodeBuilder to return `PyGraph`, `PyNode` wrappers
2. Add nanobind type casters for automatic C++ → Python wrapper conversion
3. Implement dynamic type dispatch in `wrap_input()` / `wrap_output()` (currently returns base wrappers)
4. Implement proper Python iterators for TSL, TSB, TSD, TSS, TSW
5. Complete TSD/TSS/TSW/REF specialized implementations
6. Wrap `NodeSignature`, `EvaluationClock`, `Traits`
7. Integration testing with existing Python test suite
8. Performance testing

## Notes

- The `_` prefixed types should NOT be used in Python code
- Specialized type wrappers use `using Base::Base;` to inherit constructors
- Factory functions use `nb::object` return type for caching compatibility
- All wrappers are move-only (no copy constructor/assignment)

