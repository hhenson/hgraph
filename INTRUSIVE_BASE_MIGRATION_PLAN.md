# Intrusive Base Migration Plan

## Overview
This document outlines the plan to remove `intrusive_base` from the codebase and convert objects to either:
1. **`shared_ptr`** - For objects that need reference counting and are passed around/stored in containers
2. **Raw Python exports** - For objects like `GraphExecutor` that are created in Python and don't need intrusive ref counting

## Current State Analysis

### Objects Using `intrusive_base`:

1. **EvaluationClock** (and subclasses)
   - **Status**: Already wrapped with `PyEvaluationClock` wrapper
   - **Current**: Uses `nb::ref<EvaluationClock>` (intrusive_base)
   - **Recommendation**: ✅ **Already handled** - Base class not exposed, wrapper uses `ApiPtr`

2. **EvaluationLifeCycleObserver**
   - **Current**: `struct EvaluationLifeCycleObserver : nb::intrusive_base`
   - **Usage**: Stored in `std::vector<EvaluationLifeCycleObserver::ptr>` in `GraphExecutor`, passed as callbacks
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Stored in containers, passed around, needs reference counting

3. **NodeSignature**
   - **Current**: `struct NodeSignature : nanobind::intrusive_base`
   - **Usage**: Stored in `Node` (as `NodeSignature::ptr`), passed to constructors, shared metadata across node instances
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Shared metadata, stored in Node, passed around, needs reference counting

4. **Traits**
   - **Current**: `struct Traits : nb::intrusive_base`
   - **Usage**: Stored in `Graph` (as `traits_ptr`), has optional parent traits (`std::optional<ptr> _parent_traits`), passed around
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Stored in Graph, has parent relationship, needs reference counting

5. **TimeSeriesType** (and subclasses)
   - **Current**: `struct TimeSeriesType : nb::intrusive_base`
   - **Usage**: Base class for all time series types, used extensively throughout runtime, stored in nodes
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Core runtime type, used extensively, stored in containers, needs reference counting

6. **Builder** (and subclasses: InputBuilder, OutputBuilder, NodeBuilder, GraphBuilder)
   - **Current**: `struct Builder : nb::intrusive_base`
   - **Usage**: Base class for builders, used during graph construction, passed around
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Used during construction, passed around, stored in containers, needs reference counting

7. **ComponentLifeCycle**
   - **Current**: `struct ComponentLifeCycle : nb::intrusive_base`
   - **Usage**: Base class for `Graph` and `Node` (runtime objects)
   - **Recommendation**: ⚠️ **Keep as base class, but Graph/Node should use `shared_ptr`**
   - **Reason**: Base class only, Graph and Node are the actual runtime objects that need reference counting

8. **AbstractSchema**
   - **Current**: `struct AbstractSchema : nb::intrusive_base`
   - **Usage**: Base class for schema types (CompoundScalar, TimeSeriesSchema), metadata
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Metadata passed around, needs reference counting

9. **NodeScheduler**
   - **Current**: `struct NodeScheduler : nanobind::intrusive_base`
   - **Usage**: Stored in `Node`, used for scheduling
   - **Recommendation**: 🔄 **Convert to `shared_ptr`**
   - **Reason**: Runtime object stored in Node, needs reference counting

## Recommendations Summary

### Convert to `shared_ptr`:
- ✅ **EvaluationLifeCycleObserver** - Callbacks/observers
- ✅ **NodeSignature** - Shared metadata
- ✅ **Traits** - Configuration with parent relationship
- ✅ **TimeSeriesType** (and all subclasses) - Core runtime types
- ✅ **Builder** (and all subclasses) - Construction-time objects
- ✅ **AbstractSchema** - Metadata
- ✅ **NodeScheduler** - Runtime scheduling object

### Keep as base class (not exposed directly):
- ⚠️ **ComponentLifeCycle** - Base class only, Graph/Node inherit from it
- ⚠️ **EvaluationClock** - Already wrapped, base class not exposed

### Already handled:
- ✅ **GraphExecutor** - Already raw Python export (no intrusive_base)
- ✅ **EvaluationClock/EvaluationEngineApi** - Already wrapped with PyEvaluationClock/PyEvaluationEngineApi

## Migration Strategy

1. **Phase 1: Convert simple metadata types**
   - Start with `NodeSignature`, `Traits`, `AbstractSchema`
   - These are relatively isolated and easier to convert

2. **Phase 2: Convert builder types**
   - Convert `Builder` and all subclasses
   - Update all builder factory functions

3. **Phase 3: Convert runtime types**
   - Convert `TimeSeriesType` and all subclasses
   - This is the most complex as it's used extensively

4. **Phase 4: Convert observer and scheduler types**
   - Convert `EvaluationLifeCycleObserver`
   - Convert `NodeScheduler`

5. **Phase 5: Clean up base classes**
   - Remove `intrusive_base` from `ComponentLifeCycle` (if Graph/Node are using shared_ptr)
   - Ensure Graph/Node use `shared_ptr` properly

## Key Considerations

1. **Type aliases**: Update all `using ptr = nb::ref<T>` to `using ptr = std::shared_ptr<T>`
2. **Forward declarations**: Update `hgraph_forward_declarations.h` to use `std::shared_ptr`
3. **Python bindings**: Update nanobind registrations to use `shared_ptr` holders
4. **Wrapper factory**: Update `get_or_create_wrapper` to work with `shared_ptr` instead of `intrusive_base::self_py()`
5. **ApiPtr**: Consider if `ApiPtr` pattern should be extended to these types or if `shared_ptr` is sufficient

## Example: GraphExecutor Pattern (Raw Python Export)

```cpp
// GraphExecutor is NOT intrusive_base
struct HGRAPH_EXPORT GraphExecutor {
    GraphExecutor(...);  // Regular constructor
    // ... methods
};

// Registration - no intrusive_base
void GraphExecutor::register_with_nanobind(nb::module_ &m) {
    nb::class_<GraphExecutor>(m, "GraphExecutor")  // No intrusive_base!
        .def(nb::init<...>())
        // ... methods
}
```

## Example: Shared Pointer Pattern

```cpp
// Before
struct NodeSignature : nb::intrusive_base {
    using ptr = nb::ref<NodeSignature>;
    // ...
};

// After
struct NodeSignature {
    // No intrusive_base
    // ...
};
using node_signature_ptr = std::shared_ptr<NodeSignature>;

// Registration
void NodeSignature::register_with_nanobind(nb::module_ &m) {
    nb::class_<NodeSignature, std::shared_ptr<NodeSignature>>(m, "NodeSignature")
        .def(nb::init<...>())
        // ... methods
}
```

