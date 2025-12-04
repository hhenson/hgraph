# API Extraction Review

## Overview
This document reviews the Python wrapper extraction pattern in `hgraph/api/python/` and identifies remaining items that need to be extracted from C++ implementation files.

## Extraction Pattern

### Pattern Summary
1. **Py* Wrapper Classes**: Create wrapper classes in `cpp/include/hgraph/api/python/` that:
   - Use `ApiPtr<T>` to hold a reference to the C++ implementation
   - Expose a Python-friendly API
   - Delegate to the C++ implementation via `ApiPtr`
   - Have a `register_with_nanobind()` method for Python binding

2. **Wrapper Factory Functions**: In `wrapper_factory.h/cpp`:
   - `wrap_*()` functions that create wrappers with caching via `intrusive_base::self_py()`
   - `unwrap_*()` functions to extract raw pointers from wrappers
   - Use `get_or_create_wrapper()` template to check for cached wrappers before creating new ones

3. **ApiPtr**: Smart pointer that:
   - Validates graph lifetime before dereferencing
   - Holds a `control_block_ptr` for lifetime tracking
   - Move-only (not copyable)

### Example Pattern
```cpp
// In py_node.h
struct PyNode {
    using api_ptr = ApiPtr<Node>;
    explicit PyNode(api_ptr node);
    // ... Python API methods ...
private:
    api_ptr _impl;
};

// In wrapper_factory.cpp
nb::object wrap_node(const Node *impl, const control_block_ptr &control_block) {
    return get_or_create_wrapper(impl, control_block, 
        [](auto impl, const auto &cb) { 
            return PyNode({impl, cb}); 
        });
}
```

## Already Extracted ✅

1. **PyNode** (`py_node.h/cpp`)
   - Wraps `Node` and its subclasses
   - Includes `PyNestedNode`, `PyMapNestedNode`, `PyMeshNestedNode`
   - Includes `PyNodeScheduler`

2. **PyGraph** (`py_graph.h/cpp`)
   - Wraps `Graph`
   - **NOTE**: Currently returns `nb::ref<EvaluationEngineApi>` and `EvaluationClock::ptr` directly (needs extraction)

3. **PyTraits** (`py_graph.h/cpp`)
   - Wraps `Traits`

4. **PyTimeSeries* Wrappers** (multiple files)
   - `PyTimeSeriesInput`, `PyTimeSeriesOutput` (base classes)
   - `PyTimeSeriesValueInput/Output`
   - `PyTimeSeriesSignalInput`
   - `PyTimeSeriesListInput/Output`
   - `PyTimeSeriesBundleInput/Output`
   - `PyTimeSeriesDictInput/Output`
   - `PyTimeSeriesSetInput/Output`
   - `PyTimeSeriesWindowInput/Output`
   - `PyTimeSeries*ReferenceInput/Output` (all REF types)

## NOT Yet Extracted ❌

### 1. EvaluationEngineApi and EvaluationEngine
**Location**: `cpp/include/hgraph/runtime/evaluation_engine.h`
**Current Status**: Direct nanobind registration in `cpp/src/cpp/runtime/evaluation_engine.cpp`

**Classes to Extract**:
- `EvaluationEngineApi` (base class)
- `EvaluationEngine` (extends EvaluationEngineApi)
- `EvaluationEngineDelegate` (extends EvaluationEngine)
- `EvaluationEngineImpl` (concrete implementation)

**Current Issues**:
- `PyGraph::evaluation_engine_api()` returns `nb::ref<EvaluationEngineApi>` directly (line 56 in `py_graph.cpp`)
- `base_python_node.cpp` uses `nb::cast(engine_api)` directly (line 57)
- Direct registration in `evaluation_engine.cpp` lines 98-119, 121-143

**Files to Create**:
- `cpp/include/hgraph/api/python/py_evaluation_engine.h`
- `cpp/src/cpp/api/python/py_evaluation_engine.cpp`

**Wrapper Functions Needed**:
- `wrap_evaluation_engine_api()` in `wrapper_factory.h/cpp` (currently commented out, lines 375-380)

### 2. EvaluationClock and Subclasses
**Location**: `cpp/include/hgraph/runtime/evaluation_engine.h`
**Current Status**: Direct nanobind registration in `cpp/src/cpp/runtime/evaluation_engine.cpp`

**Classes to Extract**:
- `EvaluationClock` (base class)
- `EngineEvaluationClock` (extends EvaluationClock)
- `EngineEvaluationClockDelegate` (extends EngineEvaluationClock)
- `BaseEvaluationClock` (extends EngineEvaluationClockDelegate)
- `SimulationEvaluationClock` (extends BaseEvaluationClock)
- `RealTimeEvaluationClock` (extends BaseEvaluationClock)
- `NestedEngineEvaluationClock` (extends EngineEvaluationClockDelegate)
- `MapNestedEngineEvaluationClock<T>` (template, extends NestedEngineEvaluationClock)
- `MeshNestedEngineEvaluationClock<T>` (template, extends NestedEngineEvaluationClock)

**Current Issues**:
- `PyGraph::evaluation_clock()` returns `EvaluationClock::ptr` directly (line 58 in `py_graph.cpp`)
- `base_python_node.cpp` uses `nb::cast(clock)` directly (line 61)
- Direct registration in `evaluation_engine.cpp` lines 12-26, 28-52, 92-95, 307-486
- Additional registrations in `nested_evaluation_engine.cpp`, `tsd_map_node.cpp`, `mesh_node.cpp`

**Files to Create**:
- `cpp/include/hgraph/api/python/py_evaluation_clock.h`
- `cpp/src/cpp/api/python/py_evaluation_clock.cpp`

**Wrapper Functions Needed**:
- `wrap_evaluation_clock()` in `wrapper_factory.h/cpp` (currently commented out, lines 382-387)

### 3. GraphExecutor and GraphExecutorImpl
**Location**: `cpp/include/hgraph/runtime/graph_executor.h`
**Current Status**: Direct nanobind registration in `cpp/src/cpp/runtime/graph_executor.cpp`

**Classes to Extract**:
- `GraphExecutor` (base class)
- `GraphExecutorImpl` (concrete implementation)

**Current Issues**:
- Direct registration in `graph_executor.cpp` lines 146-150
- Exported in `_hgraph_runtime.cpp` lines 14-15

**Files to Create**:
- `cpp/include/hgraph/api/python/py_graph_executor.h`
- `cpp/src/cpp/api/python/py_graph_executor.cpp`

**Wrapper Functions Needed**:
- `wrap_graph_executor()` in `wrapper_factory.h/cpp`

### 4. EvaluationLifeCycleObserver
**Location**: `cpp/include/hgraph/runtime/graph_executor.h`
**Current Status**: Direct nanobind registration (if any)

**Note**: This is an abstract base class for Python to inherit from. May not need a wrapper if it's only used as a base class for Python implementations.

## Files That Need Updates

### High Priority (Blocking ApiPtr migration)

1. **`cpp/src/cpp/api/python/py_graph.cpp`**
   - Line 56: `evaluation_engine_api()` - should return wrapped `PyEvaluationEngineApi`
   - Line 58: `evaluation_clock()` - should return wrapped `PyEvaluationClock`

2. **`cpp/src/cpp/nodes/base_python_node.cpp`**
   - Lines 54-57: `ENGINE_API` injection - should use `wrap_evaluation_engine_api()`
   - Lines 58-61: `CLOCK` injection - should use `wrap_evaluation_clock()`

3. **`cpp/src/cpp/runtime/evaluation_engine.cpp`**
   - Remove direct `nb::class_` registrations
   - Move to `py_evaluation_engine.cpp` and `py_evaluation_clock.cpp`

4. **`cpp/src/cpp/runtime/graph_executor.cpp`**
   - Remove direct `nb::class_` registrations
   - Move to `py_graph_executor.cpp`

5. **`cpp/include/hgraph/api/python/wrapper_factory.h`**
   - Uncomment and implement `wrap_evaluation_engine_api()` (lines 375-380)
   - Uncomment and implement `wrap_evaluation_clock()` (lines 382-387)
   - Add `wrap_graph_executor()`

6. **`cpp/src/cpp/api/python/wrapper_factory.cpp`**
   - Implement the wrapper functions above

### Medium Priority

7. **`cpp/src/cpp/python/_hgraph_runtime.cpp`**
   - Update to use wrapper registrations instead of direct registrations

8. **`cpp/src/cpp/nodes/nested_evaluation_engine.cpp`**
   - May need updates for `NestedEngineEvaluationClock` wrapper

9. **`cpp/src/cpp/nodes/tsd_map_node.cpp`**
   - May need updates for `MapNestedEngineEvaluationClock<T>` wrapper

10. **`cpp/src/cpp/nodes/mesh_node.cpp`**
    - May need updates for `MeshNestedEngineEvaluationClock<T>` wrapper

## Implementation Notes

### Template Specializations
Some clock types are templates (e.g., `MapNestedEngineEvaluationClock<T>`, `MeshNestedEngineEvaluationClock<T>`). These will need:
- Template wrapper classes or
- Type-erased wrapper with dynamic dispatch

### Inheritance Hierarchy
The clock and engine classes have complex inheritance hierarchies. The wrappers should mirror this:
- `PyEvaluationClock` (base)
- `PyEngineEvaluationClock : PyEvaluationClock`
- `PyBaseEvaluationClock : PyEngineEvaluationClock`
- etc.

### Control Block Access
All wrappers need access to the graph's `control_block_ptr`. For clocks and engines:
- Clocks are owned by engines
- Engines are owned by graphs
- Need to trace ownership to get the control block

## Next Steps

1. Create `PyEvaluationEngineApi` and `PyEvaluationEngine` wrappers
2. Create `PyEvaluationClock` and subclasses wrappers
3. Create `PyGraphExecutor` wrapper
4. Update `wrapper_factory` to include new wrapper functions
5. Update `PyGraph` to return wrapped types
6. Update `base_python_node.cpp` to use wrapper functions
7. Remove direct nanobind registrations from implementation files
8. Test thoroughly to ensure no double-wrapping issues




