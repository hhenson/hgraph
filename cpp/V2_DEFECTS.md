# V2 Time-Series API - Defect List

This document lists the known defects, missing implementations, and TODOs in the V2 time-series API.
The V1 implementation has been archived to `cpp/v1_archive/` for reference.

## Critical Missing Functionality

### 1. Edge Wiring (graph_builder.cpp)
**Location:** `cpp/src/cpp/builders/graph_builder.cpp:55-63`
**Issue:** Edges between nodes are NOT wired in V2. The code has a TODO loop that does nothing.
**Impact:** Nodes cannot communicate through time-series bindings.
**Reference:** V1 implementation used `_extract_input/output` helpers and `input->bind_output(output)`.

### 2. Node Implementations (Nested Nodes)
The following node types have V2 stubs that throw runtime errors:

#### SwitchNode (switch_node.cpp)
- `do_start()` - throws "not yet implemented"
- `eval()` - throws "not yet implemented"
- `wire_graph()` - throws "not yet implemented"
- `unwire_graph()` - throws "not yet implemented"
**Reference:** V1 uses `TimeSeriesValueInput<K>` for key input and dynamic graph switching.

#### TsdMapNode (tsd_map_node.cpp)
- `do_start()` - throws "not yet implemented"
- `eval()` - throws "not yet implemented"
- `tsd_output()` - throws "not yet implemented"
- `create_new_graph()` - throws "not yet implemented"
- `evaluate_graph()` - throws "not yet implemented"
- `un_wire_graph()` - throws "not yet implemented"
- `wire_graph()` - throws "not yet implemented"
**Reference:** V1 uses `TimeSeriesSetInput_T<K>` for key tracking and dynamic nested graphs.

#### ReduceNode (reduce_node.cpp)
- `ts()` - throws "not yet implemented"
- `zero()` - throws "not yet implemented"
- `initialise()` - throws "not yet implemented"
- `do_start()` - throws "not yet implemented"
- `eval()` - throws "not yet implemented"
- `last_output()` - throws "not yet implemented"
- `add_nodes()` - throws "not yet implemented"
- `remove_nodes()` - throws "not yet implemented"
- `swap_node()` - throws "not yet implemented"
- `grow_tree()` - throws "not yet implemented"
- `bind_key_to_node()` - throws "not yet implemented"
- `zero_node()` - throws "not yet implemented"
- `get_node()` - throws "not yet implemented"
**Reference:** V1 implements a tree-based reduction structure with `TimeSeriesDictInput_T<K>`.

#### TsdNonAssociativeReduceNode (non_associative_reduce_node.cpp)
- `initialise()` - throws "not yet implemented"
- `do_start()` - throws "not yet implemented"
- `eval()` - throws "not yet implemented"
- `update_changes()` - throws "not yet implemented"
- `extend_nodes_to()` - throws "not yet implemented"
- `bind_output()` - throws "not yet implemented"
- `last_output_value()` - throws "not yet implemented"
- `get_node()` - throws "not yet implemented"
**Reference:** V1 uses chain-based linear reduction with `TimeSeriesReferenceInput`.

#### ComponentNode (component_node.cpp)
- `recordable_id()` - throws "not yet implemented"
- `wire_graph()` - throws "not yet implemented"
- `do_start()` - throws "not yet implemented"
- `do_eval()` - throws "not yet implemented"
**Reference:** V1 uses `GlobalState` for component tracking and format string parsing.

#### MeshNode (mesh_node.cpp)
- `do_start()` - throws "not yet implemented"
- `eval()` - throws "not yet implemented"
- `tsd_output()` - throws "not yet implemented"
- `create_new_graph()` - throws "not yet implemented"
- `schedule_graph()` - throws "not yet implemented"
- `add_graph_dependency()` - throws "not yet implemented"
- `remove_graph_dependency()` - throws "not yet implemented"
- `request_re_rank()` - throws "not yet implemented"
- `re_rank()` - throws "not yet implemented"
**Reference:** V1 has complex rank-based scheduling with dependency tracking.

## Python Wrapper Issues

### 3. delta_value Not Implemented (py_time_series.cpp)
**Location:** `cpp/src/cpp/api/python/py_time_series.cpp:68-72` (Output), `179-181` (Input)
**Issue:** `delta_value()` just returns `value()` for both input and output.
**Expected:** Should return the delta/change since last tick, not the full value.
**Reference:** Python implementation tracks delta separately for collections.

### 4. Parent Tracking Not Implemented (py_time_series.cpp)
**Location:** `cpp/src/cpp/api/python/py_time_series.cpp:94-102` (Output), `202-209` (Input)
**Issue:** `parent_output()` and `parent_input()` always return None.
**Expected:** V2 views should track parent relationships for nested structures.

### 5. Reference Type Support Incomplete (py_time_series.cpp)
**Location:** `cpp/src/cpp/api/python/py_time_series.cpp:227-244` (Input)
**Issue:** Several methods return stubs:
- `has_peer()` - always returns False
- `output()` - always returns None
- `bind_output()` - always returns False
**Expected:** REF type inputs should track bound output relationships.

## Builder/Factory Issues

### 6. OutputBuilder.make_instance Throws (output_builder.cpp)
**Location:** `cpp/src/cpp/builders/output_builder.cpp:28-33`
**Issue:** V2 doesn't use builders to create time-series instances; throws runtime error.
**Impact:** Python code calling `builder.make_instance()` will fail.
**Note:** Time-series are now value types owned by Node, created via `emplace_*` methods.

### 7. wrap_time_series(shared_ptr) Throws (wrapper_factory.cpp)
**Location:** `cpp/src/cpp/api/python/wrapper_factory.cpp:155-162`
**Issue:** V1 types using `shared_ptr<TimeSeriesOutput>` cannot be wrapped in V2.
**Impact:** V1 TSD output types will fail at runtime.

## Diagnostics/Debug Issues

### 8. BackTrace Capture Not Implemented (error_type.cpp)
**Location:** `cpp/src/cpp/types/error_type.cpp:187-190`
**Issue:** V2 back trace capture returns empty data, doesn't iterate inputs.
**Expected:** Should iterate TSInput fields using V2 APIs for error diagnostics.
**Reference:** V1 used `node->input()->items()` iteration and `py_value()` calls.

## API Issues

### 9. all_valid Incomplete (py_time_series.cpp)
**Location:** `cpp/src/cpp/api/python/py_time_series.cpp:88-92` (Output), `198-200` (Input)
**Issue:** `all_valid()` just returns `valid()` for simple values.
**Expected:** For collections, should check all elements are valid.

---

## Design Notes

### Output Binding for Nested Graphs

**Problem:** V1 used "output transplanting" where `node.output = self.output` replaced a stub node's
output with the parent node's output. This doesn't work in V2 because TSOutput is a value type owned
by the node, not a shared pointer.

**Solution: bind_parent_output mechanism**

Add a `bind_parent_output(ts::TSOutput* parent)` method to Node:

1. When `bind_parent_output(parent)` is called, store a pointer to the parent's output
2. In `do_eval()`, when writing the result:
   - If `_bound_parent_output` is set, write to parent's output
   - Otherwise, write to `self.output` as normal

**Flow for nested graph output:**

1. Inner graph produces data in some node's TSOutput
2. That data flows through edges until reaching the output stub node
3. Output stub's TSInput receives a REF pointing to the actual data
4. Output stub evaluates `_stub(ts)` which returns the REF
5. Result is written to parent's output (via bound_parent_output)

**Implementation locations:**
- `Node::bind_parent_output()` - new method in node.h
- `BasePythonNode::do_eval()` - check for bound output and write there
- `ComponentNode::wire_graph()` - call `bind_parent_output` on output stub node

---

## V1 Reference Locations (Archived)

The V1 implementation is preserved in `cpp/v1_archive/` for reference:

- **api/python/v1/** - Python wrapper implementations
- **nodes/v1/** - Node implementations with full V1 time-series API
- **types/v1/** - V1 time-series types (TimeSeriesValueInput/Output, etc.)

Key V1 patterns to port:
- `TimeSeriesValueInput<T>::value()` - Direct typed value access
- `TimeSeriesBundleInput::items()` - Named field iteration
- `TimeSeriesReferenceInput::clone_binding()` - Reference copying
- `TimeSeriesSetInput_T<K>::added()/removed()` - Delta tracking for sets
- `TimeSeriesDictInput_T<K>::get_or_create()` - Lazy per-key input creation

---

*Generated: 2025-12-18*
*Status: V2-only build (V1 archived)*
