# Shared Pointer Migration Plan: ApiPtr → std::shared_ptr

## Overview
This document outlines the plan to migrate from `ApiPtr` (with intrusive base) to `std::shared_ptr` with `enable_shared_from_this` for Graph and aliased shared_ptr strategy for all components.

## Goals
1. Remove `nb::intrusive_base` from Graph and all PyXXX wrapper components
2. Add `enable_shared_from_this` to Graph
3. Use `std::shared_ptr<Graph>` for Graph wrapper construction
4. Use aliased `std::shared_ptr` strategy for all other wrappers (Node, TimeSeries, etc.)
5. Support standalone output construction with `std::shared_ptr<TimeSeriesOutput>`

## Current Architecture

### Current State
- **Graph**: Inherits from `ComponentLifeCycle` → `nb::intrusive_base`
- **Node**: Inherits from `ComponentLifeCycle` → `nb::intrusive_base`
- **TimeSeriesType**: Inherits from `nb::intrusive_base`
- **All PyXXX wrappers**: Use `ApiPtr<T>` which stores raw pointer + `control_block_ptr`
- **Wrapper caching**: Uses `intrusive_base::self_py()` for caching
- **Graph lifetime**: Managed via `ApiControlBlock` (atomic flag)

### Key Files
- `cpp/include/hgraph/api/python/api_ptr.h` - ApiPtr definition
- `cpp/include/hgraph/types/graph.h` - Graph class
- `cpp/include/hgraph/types/node.h` - Node class
- `cpp/include/hgraph/types/time_series_type.h` - TimeSeriesType base
- `cpp/src/cpp/api/python/wrapper_factory.cpp` - Wrapper creation logic
- `cpp/src/cpp/api/python/py_graph.h/cpp` - PyGraph wrapper
- `cpp/src/cpp/api/python/py_node.h/cpp` - PyNode wrapper
- `cpp/src/cpp/api/python/py_time_series.h/cpp` - PyTimeSeries wrappers

## Implementation Plan

### Phase 1: Graph Changes

#### 1.1 Modify Graph Class
**File**: `cpp/include/hgraph/types/graph.h`

**Changes**:
- Remove inheritance from `ComponentLifeCycle` (or make ComponentLifeCycle not inherit from intrusive_base)
- Add `std::enable_shared_from_this<Graph>`
- Change `using ptr = nanobind::ref<Graph>` to `using ptr = std::shared_ptr<Graph>`
- Remove `control_block_ptr` member (no longer needed)
- Remove `control_block()` method
- Update constructor to not create `ApiControlBlock`

**Dependencies**:
- `ComponentLifeCycle` must not inherit from `nb::intrusive_base` (or Graph must not inherit from ComponentLifeCycle)
- All places that call `graph->control_block()` need updating

#### 1.2 Update Graph Implementation
**File**: `cpp/src/cpp/types/graph.cpp`

**Changes**:
- Remove `_control_block` initialization in constructor
- Remove `_control_block->mark_dead()` in destructor
- Update `control_block()` method (remove or return shared_ptr to self)
- Update all methods that use `control_block()`

### Phase 2: Remove Intrusive Base from Components

#### 2.1 ComponentLifeCycle
**File**: `cpp/include/hgraph/util/lifecycle.h`

**Changes**:
- Remove `: nb::intrusive_base` inheritance
- Keep all lifecycle methods unchanged
- ComponentLifeCycle becomes a simple base class with just two bool members (`_started`, `_transitioning`)
- No other changes needed - it's already lightweight

**File**: `cpp/src/cpp/util/lifecycle.cpp`
- Update `register_with_nanobind` to not inherit from `intrusive_base`:
  ```cpp
  nb::class_<ComponentLifeCycle>(m, "ComponentLifeCycle")  // Remove nb::intrusive_base
  ```

**Impact Analysis**:
- ComponentLifeCycle is only used as a base class (never as a pointer type)
- No `nb::ref<ComponentLifeCycle>` usage found
- State is just two bools - very lightweight
- Graph/Node can use multiple inheritance: `struct Graph : std::enable_shared_from_this<Graph>, ComponentLifeCycle`
- No composition or template changes needed - just remove intrusive_base inheritance

#### 2.2 TimeSeriesType
**File**: `cpp/include/hgraph/types/time_series_type.h`

**Changes**:
- Remove `: nb::intrusive_base` inheritance
- Update `using ptr = nb::ref<TimeSeriesType>` to `using ptr = std::shared_ptr<TimeSeriesType>`
- Update all derived classes (TimeSeriesOutput, TimeSeriesInput)

**Note**: TimeSeriesType is abstract, so we need to update all concrete implementations:
- `BaseTimeSeriesOutput`
- `BaseTimeSeriesInput`
- All specialized types (TS, TSD, TSS, TSB, TSL, TSW, etc.)

#### 2.3 Node
**File**: `cpp/include/hgraph/types/node.h`

**Changes**:
- Remove `: ComponentLifeCycle` inheritance (or ComponentLifeCycle already fixed)
- Change `using ptr = nanobind::ref<Node>` to `using ptr = std::shared_ptr<Node>`
- Update all node types

#### 2.4 Other Intrusive Base Classes
**Files to check**:
- `cpp/include/hgraph/types/traits.h` - `Traits : nb::intrusive_base`
- `cpp/include/hgraph/types/node.h` - `NodeSignature : nb::intrusive_base`
- `cpp/include/hgraph/types/node.h` - `NodeScheduler : nb::intrusive_base`
- `cpp/include/hgraph/runtime/evaluation_engine.h` - `EvaluationClock : nb::intrusive_base`
- `cpp/include/hgraph/runtime/graph_executor.h` - `GraphExecutor : nb::intrusive_base`
- `cpp/include/hgraph/runtime/graph_executor.h` - `EvaluationLifeCycleObserver : nb::intrusive_base`
- `cpp/include/hgraph/builders/builder.h` - `Builder : nb::intrusive_base`
- `cpp/include/hgraph/types/schema_type.h` - `AbstractSchema : nb::intrusive_base`

**Decision needed**: Which of these should remain intrusive_base vs. shared_ptr?
- **Recommendation**: Keep intrusive_base for lightweight objects (Traits, NodeSignature, NodeScheduler, EvaluationClock, Builder, AbstractSchema)
- Use shared_ptr only for Graph, Node, TimeSeriesType (objects that need standalone lifetime)

### Phase 3: Update ApiPtr to SharedPtr

#### 3.1 Remove ApiPtr
**File**: `cpp/include/hgraph/api/python/api_ptr.h`

**Action**: Delete or deprecate (keep temporarily for migration)

#### 3.2 Update PyGraph
**File**: `cpp/include/hgraph/api/python/py_graph.h`

**Changes**:
- Change `using api_ptr = ApiPtr<Graph>` to `using impl_ptr = std::shared_ptr<Graph>`
- Update constructor: `explicit PyGraph(std::shared_ptr<Graph> graph)`
- Remove `control_block()` method

**File**: `cpp/src/cpp/api/python/py_graph.cpp`

**Changes**:
- Update constructor implementation
- Update `copy_with()` to use `shared_from_this()` or create new shared_ptr
- Update all methods that access `_impl`
- Update `register_with_nanobind` if needed

#### 3.3 Update PyNode
**File**: `cpp/include/hgraph/api/python/py_node.h`

**Changes**:
- Change `using api_ptr = ApiPtr<Node>` to `using impl_ptr = std::shared_ptr<Node>`
- Update constructor: `explicit PyNode(std::shared_ptr<Node> node)`
- Remove `control_block()` method

**File**: `cpp/src/cpp/api/python/py_node.cpp`

**Changes**:
- Update constructor to use aliased shared_ptr:
  ```cpp
  PyNode::PyNode(std::shared_ptr<Node> node) 
      : _impl(node, node->graph())  // Aliased: shared_ptr to Graph, pointer to Node
  ```
- Update all methods

#### 3.4 Update PyTimeSeriesType
**File**: `cpp/include/hgraph/api/python/py_time_series.h`

**Changes**:
- Change `using api_ptr = ApiPtr<TimeSeriesType>` to `using impl_ptr = std::shared_ptr<TimeSeriesType>`
- Update constructors:
  ```cpp
  explicit PyTimeSeriesType(TimeSeriesType *ts, std::shared_ptr<Graph> graph);
  explicit PyTimeSeriesType(std::shared_ptr<TimeSeriesType> ts);  // For standalone
  ```
- Remove `control_block()` method

**File**: `cpp/src/cpp/api/python/py_time_series.cpp`

**Changes**:
- Update constructors to use aliased shared_ptr:
  ```cpp
  PyTimeSeriesType::PyTimeSeriesType(TimeSeriesType *ts, std::shared_ptr<Graph> graph)
      : _impl(graph, ts)  // Aliased: shared_ptr to Graph, pointer to TimeSeriesType
  ```
- For standalone outputs:
  ```cpp
  PyTimeSeriesType::PyTimeSeriesType(std::shared_ptr<TimeSeriesType> ts)
      : _impl(ts)  // Direct shared_ptr
  ```

### Phase 4: Update Wrapper Factory

#### 4.1 Wrapper Creation (No Caching)
**File**: `cpp/src/cpp/api/python/wrapper_factory.cpp`

**Current**: Uses `intrusive_base::self_py()` for caching

**New Strategy**: Remove caching, always create new wrappers

**Changes**:
- Remove `get_or_create_wrapper` function entirely
- Simplify wrapper creation to direct instantiation:
  ```cpp
  nb::object wrap_node(const Node *impl, std::shared_ptr<Graph> graph) {
      if (!impl) return nb::none();
      auto node_ptr = std::shared_ptr<Node>(graph, impl);  // Aliased
      return nb::cast(PyNode(node_ptr));
  }
  ```
- Wrappers are lightweight value types, Python will manage their lifetime
- No need for caching - duplicates are acceptable

#### 4.2 Update Wrap Functions
**File**: `cpp/src/cpp/api/python/wrapper_factory.cpp`

**Changes**:
- `wrap_graph()`: Takes `std::shared_ptr<Graph>`
- `wrap_node()`: Takes `Node*` and `std::shared_ptr<Graph>` (aliased)
- `wrap_output()`: Takes `TimeSeriesOutput*` and `std::shared_ptr<Graph>` (aliased)
- `wrap_input()`: Takes `TimeSeriesInput*` and `std::shared_ptr<Graph>` (aliased)
- Add overload for standalone outputs: `wrap_output(std::shared_ptr<TimeSeriesOutput>)`

### Phase 5: Update All Usages

#### 5.1 Graph Construction
**Files**: All places that create Graph instances

**Changes**:
- Use `std::make_shared<Graph>(...)` instead of `new Graph(...)`
- Update `GraphBuilder::make_instance()` to return `std::shared_ptr<Graph>`
- Update all `graph_ptr` type aliases

#### 5.2 Node Construction
**Files**: All places that create Node instances

**Changes**:
- Nodes are typically created within Graph, so use aliased shared_ptr
- When creating PyNode wrapper, use: `std::shared_ptr<Node>(graph_ptr, node_ptr)`

#### 5.3 TimeSeries Construction
**Files**: All places that create TimeSeries instances

**Changes**:
- For graph-owned: Use aliased shared_ptr
- For standalone: Use `std::make_shared<TimeSeriesOutput>(...)`

#### 5.4 Output Builder
**File**: `cpp/include/hgraph/builders/output_builder.h`

**Changes**:
- Add overload: `virtual time_series_output_ptr make_instance(std::shared_ptr<TimeSeriesOutput> standalone_output) const = 0;`
- Update all implementations

**File**: `cpp/src/cpp/builders/output_builder.cpp`

**Changes**:
- Update Python bindings to support standalone output creation

### Phase 6: Update Type Aliases

#### 6.1 Graph Types
**Files**: All header files with `graph_ptr` or `Graph::ptr`

**Changes**:
- `using graph_ptr = std::shared_ptr<Graph>`
- Update all usages

#### 6.2 Node Types
**Files**: All header files with `node_ptr` or `Node::ptr`

**Changes**:
- `using node_ptr = std::shared_ptr<Node>`
- Update all usages

#### 6.3 TimeSeries Types
**Files**: All header files with time series ptr types

**Changes**:
- `using time_series_output_ptr = std::shared_ptr<TimeSeriesOutput>`
- `using time_series_input_ptr = std::shared_ptr<TimeSeriesInput>`
- Update all usages

### Phase 7: Nanobind Registration Updates

#### 7.1 Graph Registration
**File**: `cpp/src/cpp/types/graph.cpp`

**Changes**:
- Register `Graph` with `nb::class_<Graph, std::shared_ptr<Graph>>`
- Use `nb::init<std::shared_ptr<Graph>>()` if needed

#### 7.2 Node Registration
**File**: `cpp/src/cpp/types/node.cpp`

**Changes**:
- Register `Node` with `nb::class_<Node, std::shared_ptr<Node>>`
- Handle aliased shared_ptr in Python bindings

#### 7.3 TimeSeries Registration
**Files**: All time series registration files

**Changes**:
- Register with `nb::class_<TimeSeriesOutput, std::shared_ptr<TimeSeriesOutput>>`
- Support both aliased and direct shared_ptr

### Phase 8: Testing & Validation

#### 8.1 Baseline Test Status
**Before migration**: See `CURRENT_TEST_FAILURES.md` for baseline
- **Total Tests**: 1336
- **Currently Passing**: 1306
- **Currently Failing**: 10
  - 2 component tests (record_replay, record_recovery)
  - 6 mesh tests (all mesh functionality)
  - 1 inspector test
  - 1 example test

**Goal**: After migration, we should have:
- Same or fewer failures (ideally same 10, or fewer if migration fixes issues)
- No new failures introduced

#### 8.2 Compilation
- Ensure all files compile
- Fix any template instantiation issues
- Resolve circular dependency issues

#### 8.3 Runtime Tests
- Test graph creation and destruction
- Test node/edge creation
- Test time series operations
- Test standalone output creation
- Test memory leaks (valgrind/ASAN)

#### 8.4 Python Integration
- Test Python bindings work correctly
- Test object lifetime management
- Test standalone output usage

#### 8.5 Regression Testing
- Run full test suite: `HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests`
- Compare results to baseline in `CURRENT_TEST_FAILURES.md`
- Document any new failures
- Verify existing failures remain the same (or are fixed)

## Critical Considerations

### 1. Aliased Shared Pointer Pattern
The aliased shared_ptr pattern is key:
```cpp
std::shared_ptr<Graph> graph = ...;
Node* node = graph->nodes()[0].get();
std::shared_ptr<Node> node_ptr(graph, node);  // Aliased: shares ownership with graph
```

**Benefits**:
- All nodes/edges share ownership with graph
- Graph lifetime extends to all components
- No need for separate reference counting

**Caveats**:
- Must ensure `node` pointer remains valid (it's owned by graph)
- Cannot use aliased shared_ptr after graph is destroyed

### 2. Wrapper Caching
Without `intrusive_base::self_py()`, we need alternative caching:

**Option A: Weak Pointer Registry**
```cpp
static std::unordered_map<
    const void*, 
    std::weak_ptr<nb::object>
> wrapper_cache;
```

**Option B: Store in Graph**
```cpp
struct Graph {
    mutable std::unordered_map<const void*, std::weak_ptr<nb::object>> _wrapper_cache;
};
```

**Option C: No Caching**
- Let Python manage object lifetime
- May create duplicate wrappers (acceptable?)

**Recommendation**: Option B (store in Graph) - cleanest, automatic cleanup

### 3. Standalone Output Support
For standalone outputs, we need:
```cpp
std::shared_ptr<TimeSeriesOutput> output = std::make_shared<TimeSeriesValueOutput<int>>(nullptr);
PyTimeSeriesOutput wrapper(output);  // Direct shared_ptr, not aliased
```

**Key Points**:
- Standalone outputs don't have a graph
- Need to handle `owning_graph()` returning null/empty
- May need special handling in wrapper methods

### 4. ComponentLifeCycle Inheritance
**Decision**: Should Graph/Node still inherit from ComponentLifeCycle?

**Analysis**: ComponentLifeCycle is just a mixin providing lifecycle state (two bools) and virtual methods. It's never used as a pointer type, only as a base class.

**Solution**: Use multiple inheritance - remove `: nb::intrusive_base` from ComponentLifeCycle, keep it as a simple base class:
```cpp
struct ComponentLifeCycle {  // No inheritance - just a simple base class
    bool _started{false};
    bool _transitioning{false};
    // ... virtual methods ...
};

struct Graph : std::enable_shared_from_this<Graph>, ComponentLifeCycle {
    // ...
};

struct Node : ComponentLifeCycle, Notifiable {
    // ...
};
```

**Benefits**:
- Very lightweight (just two bools)
- No composition overhead
- No template complexity
- Clean multiple inheritance pattern
- Minimal changes required

### 5. EvaluationClock and Other Runtime Objects
**Decision**: Should these remain intrusive_base or become shared_ptr?

**Recommendation**: Keep as intrusive_base - they're lightweight, don't need standalone lifetime

## Migration Order

1. **Phase 1**: Graph changes (enable_shared_from_this)
2. **Phase 2**: Remove intrusive_base from Graph/Node/TimeSeriesType
3. **Phase 3**: Update PyGraph wrapper
4. **Phase 4**: Update wrapper factory and caching
5. **Phase 5**: Update PyNode wrapper
6. **Phase 6**: Update PyTimeSeries wrappers
7. **Phase 7**: Update all usages and type aliases
8. **Phase 8**: Update nanobind registrations
9. **Phase 9**: Testing and validation

## Risk Assessment

### High Risk
- Wrapper caching mechanism change
- Aliased shared_ptr correctness
- Graph lifetime management
- Python object lifetime

### Medium Risk
- Type alias updates (many files)
- Nanobind registration changes
- Output builder changes

### Low Risk
- Constructor updates
- Method signature changes

## Rollback Plan
- Keep `ApiPtr` code commented out
- Use feature flag to switch between ApiPtr and shared_ptr
- Maintain both implementations during migration

## Success Criteria
1. **Test Status**: Same or fewer test failures than baseline (currently 10 failures)
   - Baseline documented in `CURRENT_TEST_FAILURES.md`
   - No new failures introduced
   - Existing failures should remain the same (or be fixed if migration resolves them)
2. No memory leaks (valgrind/ASAN)
3. Standalone output creation works
4. Graph lifetime properly managed
5. Python bindings work correctly
6. All code compiles without errors

## Additional Considerations

### 1. Aliased Shared Pointer Implementation
The aliased shared_ptr constructor syntax:
```cpp
std::shared_ptr<Graph> graph = ...;
Node* node = ...;  // Raw pointer owned by graph
std::shared_ptr<Node> node_ptr(graph, node);  // Aliased constructor
```

**Important**: The aliased constructor `shared_ptr<T>(shared_ptr<U> const& r, T* ptr)` creates a shared_ptr that:
- Shares ownership with `r` (the graph)
- Points to `ptr` (the node)
- When all shared_ptrs are destroyed, the graph is destroyed (not the node directly)

### 2. Wrapper Caching Implementation Details
**Decision**: Skip wrapper caching initially

**Rationale**: 
- Python wrappers are now lightweight value-type objects
- The only cost of creating/deleting is inc/dec of graph shared_ptr
- This is comparable to inc/dec of intrusive_base pointers/python ptrs
- Can live with duplicates at wrapper level
- Can add caching later if needed

**Implementation**: 
- Remove `get_or_create_wrapper` caching logic
- Always create new wrapper instances
- Wrappers will be garbage collected by Python when no longer referenced

### 3. Standalone Output Handling
For standalone outputs (not owned by a graph):

```cpp
// In PyTimeSeriesType constructor
PyTimeSeriesType::PyTimeSeriesType(std::shared_ptr<TimeSeriesType> ts) {
    if (ts->owning_graph()) {
        // Graph-owned: use aliased shared_ptr
        auto graph = ts->owning_graph();  // Returns shared_ptr<Graph>
        _impl = std::shared_ptr<TimeSeriesType>(graph, ts.get());
    } else {
        // Standalone: use direct shared_ptr
        _impl = ts;
    }
}
```

**Note**: `owning_graph()` needs to return `std::shared_ptr<Graph>` instead of `graph_ptr` (nb::ref)

### 4. Graph Construction Pattern
All Graph construction must use `std::make_shared`:

```cpp
// In GraphBuilder::make_instance
graph_ptr GraphBuilder::make_instance(...) const {
    auto graph = std::make_shared<Graph>(graph_id, nodes, parent_node, label, traits);
    // ... setup ...
    return graph;
}
```

### 5. Node Storage in Graph
Graph stores nodes as `std::vector<node_ptr>` where `node_ptr = std::shared_ptr<Node>`.

**Important**: When creating nodes, they should be created with aliased shared_ptr:
```cpp
// In Graph constructor or node creation
for (auto& node : nodes) {
    // node is already a shared_ptr<Node>, but we need to ensure it's aliased to this graph
    // Actually, nodes should be created with aliased shared_ptr from the start
}
```

**Better approach**: Store raw pointers in Graph, create aliased shared_ptr on demand:
```cpp
struct Graph {
    std::vector<Node*> _nodes;  // Raw pointers, owned by graph
    
    node_ptr get_node(size_t index) {
        return std::shared_ptr<Node>(shared_from_this(), _nodes[index]);
    }
};
```

**OR**: Store shared_ptr directly (nodes own themselves, graph holds reference):
```cpp
struct Graph {
    std::vector<std::shared_ptr<Node>> _nodes;  // Direct shared_ptr
    // Nodes must be created with make_shared, graph holds reference
};
```

**Recommendation**: Store as `std::vector<std::shared_ptr<Node>>` - simpler, nodes can outlive graph if needed

### 6. TimeSeriesType Storage
TimeSeriesType objects are typically stored as members of Node or other objects.

**Pattern**: Store as raw pointers or unique_ptr, create aliased shared_ptr when wrapping:
```cpp
struct Node {
    std::unique_ptr<TimeSeriesOutput> _output;  // Owned by node
    
    time_series_output_ptr output() {
        auto graph = this->graph();  // Returns shared_ptr<Graph>
        return std::shared_ptr<TimeSeriesOutput>(graph, _output.get());
    }
};
```

### 7. Breaking Changes
This migration will break:
- Python code that relies on `nb::ref` types (will need to use `std::shared_ptr`)
- Any code that directly accesses `control_block()`
- Wrapper caching behavior (may create new wrappers instead of reusing)

### 8. Testing Strategy
1. **Unit tests**: Test shared_ptr lifetime management
2. **Integration tests**: Test graph/node/edge creation and destruction
3. **Python tests**: Test Python bindings and object lifetime
4. **Memory tests**: Use valgrind/ASAN to check for leaks
5. **Performance tests**: Ensure no significant performance regression

### 9. Migration Checklist
- [ ] Remove `nb::intrusive_base` from Graph
- [ ] Add `std::enable_shared_from_this<Graph>` to Graph
- [ ] Update Graph to use `std::shared_ptr<Graph>`
- [ ] Remove `ApiPtr` usage from PyGraph
- [ ] Update wrapper factory caching mechanism
- [ ] Update PyNode to use aliased shared_ptr
- [ ] Update PyTimeSeries to use aliased shared_ptr
- [ ] Add standalone output support
- [ ] Update all type aliases
- [ ] Update all Graph/Node/TimeSeries construction
- [ ] Update nanobind registrations
- [ ] Update all `control_block()` usages
- [ ] Remove `ApiPtr` class
- [ ] Update tests
- [ ] Performance validation
- [ ] Memory leak validation

