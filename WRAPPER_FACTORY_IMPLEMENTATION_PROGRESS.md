# Wrapper Factory Implementation Progress

**Date:** 2025-11-11  
**Branch:** `py-obj-separation`  
**Status:** 🔧 In Progress - Compilation Issues

## What Was Accomplished

### 1. Factory Function Infrastructure ✅
- **Created `wrapper_factory.h` and `wrapper_factory.cpp`**
- Factory functions use `dynamic_cast` to inspect runtime types
- Return appropriate wrapper based on actual type (polymorphism)
- Const-correct signatures accepting `const T*` and using `const_cast` internally

### 2. Concrete Return Types in Wrappers ✅
Updated wrapper methods to return concrete wrapper types where known:

**PyNode:**
- `graph()` → `PyGraph`
- `input()` → `PyTimeSeriesBundleInput`
- `output()` → `PyTimeSeriesOutput` (uses factory)
- `scheduler()` → `PyNodeScheduler`
- `inputs()` → `nb::dict` (values use factory)

**PyGraph:**
- `nodes()` → `nb::tuple` of `PyNode` (uses factory)
- `parent_node()` → `nb::object` (optional `PyNode`)
- `copy_with()` → `PyGraph`

**PyTimeSeriesInput:**
- `owning_node()` → `PyNode`
- `parent_input()` → `PyTimeSeriesInput` (uses factory)
- `output()` → `PyTimeSeriesOutput` (uses factory)

**PyTimeSeriesOutput:**
- `owning_node()` → `PyNode`
- `parent_output()` → `PyTimeSeriesOutput` (uses factory)

### 3. API Corrections ✅
- Fixed `PyNodeScheduler` to match actual `NodeScheduler` API
- Removed non-existent methods (`last_scheduled_time`, `schedule_with_tag`, `set_alarm`)
- Updated `schedule()` signature to match actual API
- Updated methods to use correct base class API (`py_value()`, `py_delta_value()`, `py_set_value()`)

### 4. Smart Pointer Enhancements ✅
- Added `control_block()` accessor to `ApiPtr` for passing to factory functions
- Factory functions accept const pointers from `ref.get()`

## Current Compilation Issues

### Problem 1: Naming Conflicts
- Wrapper class names in `py_ts_types.h` conflict with actual C++ implementation types
- E.g., `TimeSeriesValueInput` (actual type) vs intended `PyTimeSeriesValueInput` (wrapper)

### Problem 2: Constructor Issues  
- Specialized wrappers in `py_ts_types.cpp` trying to construct base `PyTimeSeriesInput/Output`
- Base constructors are protected in current implementation
- Need proper constructor delegation

### Problem 3: Header Include Order
- Including actual type headers (`ts.h`, `tsw.h`, etc.) creates conflicts with wrapper names
- Need to carefully manage namespace and naming

## Files Created/Modified

### New Files (7)
1. `cpp/include/hgraph/api/python/wrapper_factory.h`
2. `cpp/src/cpp/api/python/wrapper_factory.cpp`
3. `WRAPPER_FACTORY_IMPLEMENTATION_PROGRESS.md` (this file)

### Modified Files (16)
4. `cpp/include/hgraph/api/python/api_ptr.h` - Added `control_block()` getter
5. `cpp/include/hgraph/api/python/py_node.h` - Concrete return types
6. `cpp/src/cpp/api/python/py_node.cpp` - Factory usage, ref handling
7. `cpp/include/hgraph/api/python/py_graph.h` - Concrete return types
8. `cpp/src/cpp/api/python/py_graph.cpp` - Factory usage, ref handling
9. `cpp/include/hgraph/api/python/py_node_scheduler.h` - API corrections
10. `cpp/src/cpp/api/python/py_node_scheduler.cpp` - API corrections
11. `cpp/include/hgraph/api/python/py_time_series.h` - Concrete return types, forward declarations
12. `cpp/src/cpp/api/python/py_time_series.cpp` - Factory usage, correct method names
13. `cpp/include/hgraph/api/python/python_api.h` - Added wrapper_factory.h include
14. `cpp/src/cpp/CMakeLists.txt` - Added wrapper_factory.cpp
15. `cpp/include/hgraph/api/python/py_ts_types.h` - (needs fixes for naming conflicts)
16. `cpp/src/cpp/api/python/py_ts_types.cpp` - (needs fixes for constructor issues)

## Next Steps

### Immediate Fixes Needed

1. **Fix Naming in `py_ts_types.h`:**
   - Ensure all wrapper classes have `Py` prefix
   - Check namespace usage and `using` directives
   - May need to avoid including actual type headers directly

2. **Fix Specialized Wrapper Constructors:**
   - Specialized wrappers (e.g., `PyTimeSeriesValueInput`) should:
     - Initialize base `PyTimeSeriesInput` with impl and control_block
     - Not try to static_cast incompatible types
     - Use proper constructor delegation

3. **Test Compilation:**
   - Fix all naming conflicts
   - Ensure constructors work properly
   - Verify factory functions compile

### Phase 2 (After Compilation Success)

1. **Runtime Testing:**
   - Test factory type dispatch works correctly
   - Verify wrappers are created with correct types
   - Test control block lifetime tracking

2. **Integration:**
   - Wire factory functions into existing Python bindings
   - Test with actual Python code
   - Verify no regressions

## Architecture Summary

### Factory Pattern
```cpp
// Factory inspects runtime type and returns correct wrapper
PyTimeSeriesOutput wrap_output(const TimeSeriesOutput* impl, control_block_ptr cb) {
    if (auto* tsv = dynamic_cast<TimeSeriesValueOutput<T>*>(mutable_impl)) {
        return PyTimeSeriesValueOutput(tsv, std::move(cb));
    }
    // ... check other types ...
    return PyTimeSeriesOutput(mutable_impl, std::move(cb));  // fallback
}
```

### Usage Pattern
```cpp
// Wrapper methods use factories for polymorphic returns
PyTimeSeriesOutput PyNode::output() const {
    return wrap_output(_impl->output().get(), _impl.control_block());
}

// Concrete types are returned directly
PyGraph PyNode::graph() const {
    return wrap_graph(_impl->graph(), _impl.control_block());
}
```

### Benefits
- Preserves actual runtime type when wrapping
- Python sees correct specialized type (not base)
- Type-specific methods available in Python
- No need for manual type checking in Python

## Design Notes

- **Const Safety**: Factory functions accept `const T*` but use `const_cast` internally since wrappers need mutable access
- **Move-Only**: Wrappers remain move-only value types
- **Control Block Sharing**: All wrappers from same graph share the same control block
- **Type Erasure**: Base wrappers can hold any specialized type via `ApiPtr<BaseType>`

## Compiler Errors Reference

Key errors to watch for:
- "redefinition as different kind of symbol" → naming conflict
- "no matching constructor" → constructor delegation issue
- "not related by inheritance" → incorrect static_cast
- "expected member name" → forward declaration or include issue

---

**Status**: Ready for debugging/fixing compilation issues in specialized wrapper types.

