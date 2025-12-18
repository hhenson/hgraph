# Python API Wrappers for Node-owned Time-Series

## Problem Statement
Node owns `ts::TSInput` and `ts::TSOutput` as value members (`std::optional`), not as shared_ptr. The Python wrappers need lifetime management tied to the Node, but there's no shared_ptr to the time-series objects themselves.

## Constraints
- **API is fixed** - existing Python class names must remain unchanged:
  - `PyTimeSeriesType`, `PyTimeSeriesOutput`, `PyTimeSeriesInput`
  - `PyTimeSeriesValueOutput`, `PyTimeSeriesValueInput`
  - `PyTimeSeriesBundleOutput`, `PyTimeSeriesBundleInput`, etc.
- Only the implementation can change

## Current Implementation
The existing wrappers use `ApiPtr<TimeSeriesOutput>` / `ApiPtr<TimeSeriesInput>` which wrap a `shared_ptr`. This works for V1 types that are heap-allocated with shared ownership.

## Challenge
V2 `ts::TSOutput` and `ts::TSInput` are:
- Value types owned by Node (via `std::optional`)
- No shared_ptr available for them directly
- Lifetime tied to the owning Node

## Key Insight: ApiPtr Aliasing Constructor
`ApiPtr` already supports aliasing (line 33-34 in api_ptr.h):
```cpp
ApiPtr(const T* impl, control_block_ptr donor)
    : _impl{donor, const_cast<T*>(impl)} {}
```
This allows wrapping a raw pointer while using a different shared_ptr for lifetime.

## Type Mismatch Problem
- `ApiPtr<TimeSeriesOutput>` expects a `TimeSeriesOutput*`
- V2 `ts::TSOutput` does NOT inherit from `TimeSeriesOutput`
- They are completely separate types in different namespaces

## Chosen Approach: View-based Wrappers with Schema-driven Factory

### Core Concept
The API wrappers become wrappers around **views** (`TSOutputView` / `TSInputView`), not the root objects.

### Implementation Details

1. **Wrapper internals**:
   - Hold `shared_ptr<Node>` for lifetime management
   - Hold `TSOutputView` or `TSInputView` by value
   - Delegate/adapt public methods to the view

2. **Schema `as_api()` method**:
   - `TimeSeriesTypeMeta` gets an `as_api()` method
   - Takes a view + node shared_ptr
   - Returns the appropriate wrapper type based on schema kind:
     - TSB schema → `PyTimeSeriesBundleOutput`
     - TSD schema → `PyTimeSeriesDictOutput`
     - etc.
   - Wrapper can be cast to Python via `nb::cast(api)`

3. **Navigation via views**:
   - `__getitem__` / field access delegates to view's `field()` / `element()` methods
   - Returns result of `schema->field_meta(name)->as_api(field_view, node)`

4. **API contract**:
   - Public method signatures unchanged
   - Move-only semantics preserved (no copy constructors)
   - Only constructors and private members change

5. **Wrapper factory**:
   - `wrap_output(ts::TSOutput*, shared_ptr<Node>)`
   - `wrap_input(ts::TSInput*, shared_ptr<Node>)`
   - Returns `nb::none()` for null pointers
   - Node shared_ptr serves as the control block for lifetime

### Example Flow
```
Node::output()  →  ts::TSOutput*
     ↓
wrap_output(output, node->shared_from_this())
     ↓
output->view()  →  TSOutputView
     ↓
meta->as_api(view, node)  →  PyTimeSeriesBundleOutput (or appropriate type)
     ↓
nb::cast(wrapper)  →  Python object
```

### Files to Modify
- `cpp/include/hgraph/api/python/v2/py_time_series.h` - Base wrapper changes
- `cpp/include/hgraph/api/python/v2/py_tsb.h` - Bundle wrapper
- `cpp/include/hgraph/api/python/v2/py_ts.h` - Value wrapper
- etc. for each type
- `cpp/include/hgraph/types/time_series/ts_type_meta.h` - Add `as_api()` method
- `cpp/src/cpp/api/python/v2/wrapper_factory.cpp` - New factory overloads
