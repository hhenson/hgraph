# Python API Exposure Map

## Overview

This document shows what C++ types are currently exposed to Python and how they're categorized.

---

## ✅ NEW PUBLIC API WRAPPERS (via `hgraph::api` namespace)

These are the clean, minimal API wrappers for Python users:

### Core Types
- **`Node`** → `PyNode` wrapper (replaces old `_Node`)
- **`Graph`** → `PyGraph` wrapper (new, no old equivalent)
- **`NodeScheduler`** → `PyNodeScheduler` wrapper (replaces old `_NodeScheduler`)

### Base Time Series Types  
- **`TimeSeriesInput`** → `PyTimeSeriesInput` wrapper (replaces old `_TimeSeriesInput`)
- **`TimeSeriesOutput`** → `PyTimeSeriesOutput` wrapper (replaces old `_TimeSeriesOutput`)

### Key Features
- Move-only semantics (no accidental copies)
- Lifetime validation via `ApiPtr` and `ApiControlBlock`
- Caching via `intrusive_base::self_py()` for object identity
- Minimal public API (no internal wiring/runtime methods)

---

## 🔧 OLD TYPES STILL EXPOSED (for wiring only)

Most old direct bindings have been removed. Only reference types remain because Python wiring code uses them:

### Reference Infrastructure (needed by wiring)
- `TimeSeriesReference` (has `.make()` static method used by wiring)
- `TimeSeriesReferenceInput` / `TimeSeriesReferenceOutput`
- Specialized reference types: `TimeSeriesValueReferenceInput`, `TimeSeriesListReferenceInput`, `TimeSeriesBundleReferenceInput`, `TimeSeriesDictReferenceInput`, `TimeSeriesSetReferenceInput`, `TimeSeriesWindowReferenceInput`
- Specialized reference output types: `TimeSeriesValueReferenceOutput`, `TimeSeriesListReferenceOutput`, etc.

**Note**: These will remain until Python wiring code in `hgraph/_use_cpp_runtime.py` is updated.

### ✅ Successfully Removed
- ~~`TS_Bool`, `TS_Int`, `TS_Float`, etc.~~ → Replaced by `TimeSeriesValueInput` wrapper
- ~~`TS_Out_Bool`, `TS_Out_Int`, etc.~~ → Replaced by `TimeSeriesValueOutput` wrapper  
- ~~`TimeSeriesListInput/Output`~~ → Replaced by wrapper (old direct binding removed)
- ~~`TimeSeriesBundleInput/Output`~~ → Replaced by wrapper (old direct binding removed)
- ~~`TimeSeriesSetInput/Output`~~ → Replaced by wrapper (old direct binding removed)
- ~~`TimeSeriesDictInput/Output`~~ → Replaced by wrapper (old direct binding removed)
- ~~`TimeSeriesWindowInput/Output`~~ → Replaced by wrapper (old direct binding removed)
- ~~`IndexedTimeSeriesInput/Output`~~ → Internal type, not exposed
- ~~`SetDelta`~~ → Internal type, not exposed

---

## 🔒 INTERNAL TYPES (prefixed with `_`)

These are internal implementation types, NOT for Python user code:

- `_Node` - Internal Node binding (needed for NestedNode inheritance)
- `_NodeScheduler` - Internal NodeScheduler binding
- `_TimeSeriesType` - Base type in hierarchy
- `_TimeSeriesInput` - Internal input binding
- `_TimeSeriesOutput` - Internal output binding
- `_BaseTimeSeriesInput` - Concrete base class
- `_BaseTimeSeriesOutput` - Concrete base class

**Purpose**: These maintain nanobind's C++ type hierarchy for specialized types that inherit from them.

---

## 📋 REGISTRATION FLOW

### File: `cpp/src/cpp/python/_hgraph_types.cpp`

```cpp
// 1. OLD DIRECT BINDINGS - Renamed to _ prefix (internal)
TimeSeriesType::register_with_nanobind(m);     // → "_TimeSeriesType"
TimeSeriesOutput::register_with_nanobind(m);   // → "_TimeSeriesOutput"  
TimeSeriesInput::register_with_nanobind(m);    // → "_TimeSeriesInput"
BaseTimeSeriesOutput::register_with_nanobind(m); // → "_BaseTimeSeriesOutput"
BaseTimeSeriesInput::register_with_nanobind(m);  // → "_BaseTimeSeriesInput"

// 2. Reference types - needed by wiring
TimeSeriesReference::register_with_nanobind(m);
// ... all specialized reference types ...

// 3. Specialized types - needed by builders (for now)
IndexedTimeSeriesOutput::register_with_nanobind(m);
TimeSeriesListInput::register_with_nanobind(m);
TimeSeriesBundleInput::register_with_nanobind(m);
// ... etc ...

// 4. Node/NodeScheduler - renamed to _ prefix (internal)
NodeScheduler::register_with_nanobind(m);  // → "_NodeScheduler"
Node::register_with_nanobind(m);           // → "_Node"

// 5. NEW PUBLIC API WRAPPERS
hgraph::api::register_python_api(m);  // Registers Node, Graph, NodeScheduler, TimeSeriesInput/Output
```

---

## 🎯 MIGRATION STATUS

### COMPLETED ✅
- [x] `Node`, `Graph`, `NodeScheduler` wrappers registered
- [x] `TimeSeriesInput`, `TimeSeriesOutput` base wrappers registered
- [x] Old types renamed with `_` prefix to avoid naming conflicts
- [x] Module loads successfully
- [x] Caching infrastructure in place via `intrusive_base`

### IN PROGRESS 🚧
- [ ] Specialized wrapper implementations (currently stubbed out)
- [ ] Dynamic type dispatch in `wrap_input()` / `wrap_output()`
- [ ] Python iterator implementations for TSL/TSB/TSD/TSS/TSW

### BLOCKED ⛔
- [ ] **GraphBuilder integration** - Currently returns raw `hgraph::Graph*` instead of `PyGraph`
  - Error: `TypeError: Unable to convert function return value to a Python type!`
  - Solution: Add nanobind type caster or modify builders to return wrappers

---

## 🔄 NEXT STEPS

1. **Add Type Casters** - Enable automatic conversion from `Graph*` → `PyGraph`, `Node*` → `PyNode`
2. **Implement Dynamic Dispatch** - `wrap_input/output` should return specialized wrappers
3. **Enable Specialized Wrappers** - Uncomment in `py_api_registration.cpp` once implemented
4. **Remove Old Types** - Once all wiring/builders use new wrappers, remove old registrations
5. **Full Test Suite** - Ensure all tests pass with new wrappers

---

## 📝 FILES MODIFIED

### Registration
- `cpp/src/cpp/python/_hgraph_types.cpp` - Commented out/renamed old registrations, added new API
- `cpp/src/cpp/api/python/py_api_registration.cpp` - Registers new wrappers (specialized types commented out)

### Type Renames
- `cpp/src/cpp/types/time_series_type.cpp` - TimeSeriesType/Output/Input → `_` prefix
- `cpp/src/cpp/types/base_time_series.cpp` - BaseTimeSeriesOutput/Input → `_` prefix  
- `cpp/src/cpp/types/node.cpp` - Node/NodeScheduler → `_` prefix

