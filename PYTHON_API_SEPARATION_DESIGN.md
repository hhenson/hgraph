# Python API Separation Design

**Date:** 2025-11-11  
**Branch:** `py-obj-separation`  
**Goal:** Prepare for arena allocation by separating Python interface from C++ implementation

## Overview

This refactoring separates the Python-visible API from the C++ implementation to enable future arena allocation without breaking Python bindings. The key insight is that Python code should interact with stable wrapper classes (`PyNode`, `PyGraph`, etc.) while the implementation objects can be moved to arena memory.

## Architecture

### 1. Smart Pointer with Lifetime Validation (`ApiPtr<T>`)

**Location:** `cpp/include/hgraph/api/python/api_ptr.h`

**Features:**
- Control block tracks graph lifetime
- Validates graph is alive before dereferencing
- Move-only (not copyable)
- Throws exception if graph destroyed

**Usage:**
```cpp
ApiPtr<Node> node_impl;
node_impl->eval();  // Validates before calling
```

### 2. Python API Wrapper Classes

**Location:** `cpp/include/hgraph/api/python/py_*.h`

**Naming Convention:**
- Internal C++ class: `PyNode`, `PyGraph`, `PyTimeSeriesInput`, etc.
- Python binding name: `Node`, `Graph`, `TimeSeriesInput`, etc.

**Key Properties:**
- Move-only (not copyable)
- Do NOT inherit from `intrusive_base` (Python constructs wrapper directly)
- Delegate all operations to implementation via `ApiPtr<T>`
- Expose only public API used by Python code (not wiring/runtime internals)

### 3. Wrapper Classes To Implement

#### Core Types
- ✅ **PyNode** - wraps `Node` implementation
- ⏳ **PyGraph** - wraps `Graph` implementation  
- ⏳ **PyNodeScheduler** - wraps `NodeScheduler` implementation

#### Time Series Base Types
- ⏳ **PyTimeSeriesInput** - wraps `TimeSeriesInput`
- ⏳ **PyTimeSeriesOutput** - wraps `TimeSeriesOutput`

#### Specialized Time Series Input Types
- ⏳ **PyTimeSeriesValueInput** - wraps `TimeSeriesValueInput` (TS)
- ⏳ **PyTimeSeriesSignalInput** - wraps `TimeSeriesSignalInput`
- ⏳ **PyTimeSeriesListInput** - wraps `TimeSeriesListInput` (TSL)
- ⏳ **PyTimeSeriesBundleInput** - wraps `TimeSeriesBundleInput` (TSB)
- ⏳ **PyTimeSeriesDictInput** - wraps `TimeSeriesDictInput` (TSD)
- ⏳ **PyTimeSeriesSetInput** - wraps `TimeSeriesSetInput` (TSS)
- ⏳ **PyTimeSeriesWindowInput** - wraps `TimeSeriesWindowInput` (TSW)
- ⏳ **PyTimeSeriesReferenceInput** - wraps `TimeSeriesReferenceInput` (REF)

#### Specialized Time Series Output Types
- ⏳ **PyTimeSeriesValueOutput** - wraps `TimeSeriesValueOutput` (TS)
- ⏳ **PyTimeSeriesSignalOutput** - wraps `TimeSeriesSignalOutput`
- ⏳ **PyTimeSeriesListOutput** - wraps `TimeSeriesListOutput` (TSL)
- ⏳ **PyTimeSeriesBundleOutput** - wraps `TimeSeriesBundleOutput` (TSB)
- ⏳ **PyTimeSeriesDictOutput** - wraps `TimeSeriesDictOutput` (TSD)
- ⏳ **PyTimeSeriesSetOutput** - wraps `TimeSeriesSetOutput` (TSS)
- ⏳ **PyTimeSeriesWindowOutput** - wraps `TimeSeriesWindowOutput` (TSW)
- ⏳ **PyTimeSeriesReferenceOutput** - wraps `TimeSeriesReferenceOutput` (REF)

#### Specialized Reference Types (as needed)
- ⏳ **PyTimeSeriesValueReferenceInput/Output**
- ⏳ **PyTimeSeriesListReferenceInput/Output**
- ⏳ **PyTimeSeriesBundleReferenceInput/Output**
- ⏳ **PyTimeSeriesDictReferenceInput/Output**
- ⏳ **PyTimeSeriesSetReferenceInput/Output**
- ⏳ **PyTimeSeriesWindowReferenceInput/Output**

## Example: PyNode Wrapper

```cpp
class PyNode {
public:
    PyNode(Node* impl, control_block_ptr control_block);
    
    // Move-only
    PyNode(PyNode&&) noexcept = default;
    PyNode& operator=(PyNode&&) noexcept = default;
    PyNode(const PyNode&) = delete;
    PyNode& operator=(const PyNode&) = delete;
    
    // Public API (delegates to impl)
    int64_t node_ndx() const;
    nb::object graph() const;
    void eval();
    
    // Register as "Node" in Python
    static void register_with_nanobind(nb::module_& m);
    
private:
    ApiPtr<Node> _impl;  // Lifetime-validated pointer
};
```

## API Extraction Methodology

For each wrapper class, we extract the public API by examining:

1. **Python abstract base classes** (`hgraph/_runtime/` and `hgraph/_types/`)
   - These define the contract Python code expects
   
2. **Nanobind registrations** (`cpp/src/cpp/types/*.cpp`)
   - Properties exposed via `.def_prop_ro()`, `.def_prop_rw()`
   - Methods exposed via `.def()`
   
3. **Python usage patterns** (`hgraph/_impl/` excluding wiring/runtime)
   - What methods are actually called from Python code
   
4. **Exclude internal APIs:**
   - Wiring logic (only used during graph construction)
   - Runtime internals (only used by evaluation engine)
   - Builder methods (only used during construction)

## Implementation Strategy

### Phase 1: Infrastructure ✅
- ✅ Create `ApiPtr<T>` smart pointer with control block
- ✅ Create `PyNode` wrapper as template/example

### Phase 2: Core Wrappers ⏳
- ⏳ Implement `PyGraph`
- ⏳ Implement `PyNodeScheduler`
- ⏳ Implement `PyTimeSeriesInput` base
- ⏳ Implement `PyTimeSeriesOutput` base

### Phase 3: Specialized Wrappers ⏳
- ⏳ Implement all specialized time series input wrappers
- ⏳ Implement all specialized time series output wrappers
- ⏳ Implement specialized reference wrappers

### Phase 4: Integration ⏳
- ⏳ Update nanobind registrations to use wrapper classes
- ⏳ Update factory functions to create wrappers
- ⏳ Update existing C++ code to work with wrappers

### Phase 5: Testing & Validation ⏳
- ⏳ Run Python unit tests with wrapper classes
- ⏳ Verify no performance regression
- ⏳ Validate lifetime management works correctly

## Benefits

1. **Arena Allocation Ready:** Implementation objects can be moved to arena memory without breaking Python bindings
2. **Lifetime Safety:** Control block prevents use-after-free when graphs are destroyed
3. **API Clarity:** Clear separation between public API and internal implementation
4. **Move Semantics:** Wrappers are move-only, preventing accidental copies
5. **Type Safety:** Compile-time enforcement of wrapper usage

## Migration Path

After this refactoring:
1. **Current:** Python → nanobind → `Node*` → implementation
2. **After:** Python → nanobind → `PyNode` → `ApiPtr<Node>` → implementation
3. **Future (arena):** Python → nanobind → `PyNode` → `ApiPtr<Node>` → **arena memory**

## Files Created

- `cpp/include/hgraph/api/python/api_ptr.h` - Smart pointer infrastructure
- `cpp/include/hgraph/api/python/py_node.h` - Node wrapper (template)
- More wrappers to follow...

## Next Steps

1. Implement `PyNode::register_with_nanobind()` and method bodies
2. Create `PyGraph` wrapper following same pattern
3. Continue with other core wrappers
4. Integrate into existing codebase

