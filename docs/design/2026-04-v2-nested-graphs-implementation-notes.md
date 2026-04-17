# v2 Nested Graphs: Implementation Notes And Review

Date: 2026-04-17  
Status: Working notes  
Scope: Summarize the intended `v2` nested-graph implementation shape, with `try_except` as the first bring-up target, and compare that shape to the current implementation attempt.

## 1. What The RFC Actually Implies

The nested-graphs RFC is not asking for "legacy nested graphs, but hidden under a `v2` wrapper".

The intended shape is:

1. one `v2` graph family for both top-level and child graphs
2. a shared nested substrate:
   - `ChildGraphTemplate`
   - `ChildGraphInstance`
   - `BoundaryBindingPlan`
   - boundary bind / unbind / rebind executor
   - delegated nested clock
3. operator-owned semantics above that substrate
4. no runtime dependence on stub nodes or name scanning
5. Python as the semantic reference

The important design boundary is:

1. generic substrate owns child graph lifecycle and boundary mechanics
2. operators own keyed state, branching, reduction structure, exception policy, and output semantics

That makes `try_except` the right first real operator after `nested_graph`, because it needs only:

1. one child template
2. one child instance
3. direct input binding
4. one extra operator semantic: route failure into `exception` and success into `out`

## 2. How I Would Approach The Implementation

### 2.1 Phase 1: Make `nested_graph` substrate real

Before worrying about `try_except`, I would make the non-exception nested substrate solid enough that `nested_graph` is structurally correct, not just test-green.

That means:

1. `ChildGraphTemplate` is compile-once immutable data.
2. `ChildGraphInstance` owns:
   - child `Graph`
   - nested clock state
   - boundary-runtime state if any stateful binding behavior is needed later
3. `BoundaryBindingPlan` is the only runtime boundary contract.
4. `BoundaryBindingRuntime` must support real path navigation and the first useful binding modes, not only the happy-path direct bind.
5. node lineage must be designed before keyed operators arrive.

Minimal "real enough" substrate for first operator bring-up:

1. `BIND_DIRECT`
2. `ALIAS_CHILD_OUTPUT`
3. explicit output routing through the plan
4. correct child graph ownership / lifetime
5. correct nested scheduling semantics

### 2.2 Phase 2: Implement `try_except` as a thin operator

I would not make `try_except` a special variant of the generic nested runtime.

I would make it:

1. the same nested child-graph substrate as `nested_graph`
2. plus operator-specific eval wrapper logic
3. plus operator-specific output publication rules

Concretely:

1. child eval runs inside `try/catch`
2. on success:
   - forward child output into the parent `out` field
   - do not tick `exception`
3. on failure:
   - stop or dispose the child according to the Python reference
   - publish `NodeError` into `exception`
   - do not tick `out`
4. later evals become no-ops until the operator is explicitly reactivated, matching Python semantics

The generic nested substrate should not know what `try_except` means.

### 2.3 Phase 3: Only then generalize for `component`, `switch`, `map_`, `reduce`

Once `nested_graph` and `try_except` are structurally sound, the later operators can extend the same substrate:

1. `component`: activation / uniqueness semantics
2. `switch`: branch selection and reset semantics
3. `map_`: keyed child instance map and detach semantics
4. `reduce`: operator-owned reduction structure

## 3. Important Design Rules For The First Bring-Up

These are the design rules I would hold hard even for the first operator:

### 3.1 No operator semantics inside the generic nested runtime

The nested substrate can expose helpers. It should not own policy such as:

1. "`try_except` means publish to bundle field `out`"
2. "`try_except` means catch any exception and stop child graph"

That belongs in the operator layer.

### 3.2 The boundary plan must be executable, not aspirational

If `BoundaryBindingPlan` declares modes, the runtime should either:

1. implement them, or
2. reject them explicitly

What it should not do is declare a broad contract and then silently no-op most cases.

### 3.3 Output routing should be explicit

For `try_except`, the publication target should be explicit in the operator implementation or in the output binding spec.

It should not be inferred from a boolean like `captures_exception` hidden in generic nested runtime data.

### 3.4 Graph lineage must not be postponed too long

The RFC explicitly calls out `node_id`, `owning_graph_id`, and keyed path encoding as part of the acceptance contract.

That probably does not block the first smoke tests, but it does affect API shape. If ignored too long, later operators will bake in the wrong assumptions.

### 3.5 Avoid Python bridge copies as the default nested output mechanism

Using Python conversion to move nested outputs is acceptable as a temporary expedient for parity bring-up, but it should be treated as tactical.

The intended design is erased `v2` runtime plumbing, not "convert to Python and back" as the standard data path.

## 4. Comparison With The Current Attempt

The current attempt has some good direction, but it is still more of a tactical bring-up than the RFC target architecture.

### 4.1 What is directionally right

1. `ChildGraphTemplate` exists as a compile-once artifact.
2. `ChildGraphInstance` exists as a runtime handle.
3. `BoundaryBindingPlan` exists as an explicit compile-time plan.
4. nested graphs are being compiled from wiring-layer stubs into a cleaned child graph plus a boundary plan in `hgraph/_use_cpp_runtime.py`.
5. `try_except` is treated as a first simple nested operator rather than jumping straight into `map_` or `reduce`.

Those are the right top-level pieces.

### 4.2 Where the current attempt diverges from the RFC

#### A. The boundary binder is still mostly aspirational

`BoundaryBindingRuntime` declares multiple modes, but only `BIND_DIRECT` is implemented, and even that implementation is bundle-only and direct-input-only.

See [boundary_binding.cpp](/Users/hhenson/CLionProjects/hgraph_2/cpp/src/cpp/types/v2/boundary_binding.cpp:19).

`bind_keyed()` and `rebind()` are empty, and the other declared input modes are silent no-ops.

That is acceptable for a narrow first smoke test, but it is not yet a real reusable substrate.

#### B. `try_except` semantics initially leaked into the generic nested runtime

The initial nested runtime stored `captures_exception` in generic nested runtime data and then changed generic output forwarding behavior based on that flag.

See [node_builder.cpp](/Users/hhenson/CLionProjects/hgraph_2/cpp/src/cpp/types/v2/node_builder.cpp:965) and [node_builder.cpp](/Users/hhenson/CLionProjects/hgraph_2/cpp/src/cpp/types/v2/node_builder.cpp:1063).

That part has since been corrected. `try_except` now has its own eval path and the generic child template no longer carries exception-capture policy.

The original problem was:

1. generic nested eval catches exceptions when `captures_exception` is set
2. generic output forwarding hardcodes routing to bundle field `"out"`

That is not how I would structure it. The generic nested runtime should not know about `try_except` bundle layout.

#### C. Output forwarding is still tactical, not substrate-quality

`forward_child_outputs()` currently:

1. walks output paths as though they are bundle-only
2. copies via Python conversion (`to_python` / `from_python`)

See [node_builder.cpp](/Users/hhenson/CLionProjects/hgraph_2/cpp/src/cpp/types/v2/node_builder.cpp:1000).

The `try_except`-specific routing is now outside the generic helper, which is the right direction. The remaining issue is that output forwarding still uses the Python bridge as a tactical copy path.

#### D. Child graph lineage was initially not aligned with the RFC

This was originally wired off `node_index()`, which was too weak for future keyed nesting.

That has since been corrected by adding parent-derived node lineage helpers and applying identity to child graphs during instance initialisation.

The larger keyed-path story for `map_` still remains, but the basic non-keyed lineage contract is now in better shape.

#### E. The nested engine comments initially overstated current behavior

`ChildGraphInstance::initialise()` comments say the nested engine delegates lifecycle notifications to the parent engine, but the current nested engine ops are mostly no-ops.

See [child_graph.cpp](/Users/hhenson/CLionProjects/hgraph_2/cpp/src/cpp/types/v2/child_graph.cpp:224).

That mismatch has been corrected in comments. The engine is still intentionally thin, but the documentation no longer overclaims.

#### F. The compile-away-stubs pass is intentionally narrow

The Python translation layer in [hgraph/_use_cpp_runtime.py](/Users/hhenson/CLionProjects/hgraph_2/hgraph/_use_cpp_runtime.py:632) strips stub nodes and builds a plan, which is directionally right.

But it currently only reconstructs:

1. direct input bindings from input stubs
2. direct output aliasing from the output stub

That is good enough for `nested_graph` / `try_except`, but not yet the broader boundary compilation pass described by the RFC.

## 5. How I Would Refactor The Current Attempt

If continuing from the current code, I would not throw it away. I would reshape it.

### 5.1 Keep

1. `ChildGraphTemplate`
2. `ChildGraphInstance`
3. stub-compilation in `_use_cpp_runtime.py`
4. the existence of `BoundaryBindingPlan`

### 5.2 Change next

1. keep the generic nested runtime and operator-specific runtime split in place
2. keep `try_except` exception capture and `.out` / `.exception` routing out of generic nested helpers
3. make output forwarding either:
   - a proper shared helper with explicit target semantics, or
   - operator-owned publication logic for now
4. make `BoundaryBindingRuntime` explicitly reject unimplemented modes instead of silently doing nothing
5. extend the new lineage hooks into keyed path encoding before `map_` is attempted

### 5.3 Suggested immediate shape for `try_except`

Short term:

1. keep one shared `ChildGraphInstance`
2. keep `BIND_DIRECT`
3. keep the cleaned child graph and boundary plan
4. move the `try/catch` policy into a dedicated `try_except` node family or helper
5. move `"out"` / `"exception"` publication into `try_except`-specific code

That would still be pragmatic, but cleaner and closer to the RFC.

## 6. Bottom Line

If I were starting from scratch, I would still choose the same top-level direction as the current attempt:

1. compile away runtime stubs
2. introduce child graph templates and instances
3. bring up `try_except` early

But I would be stricter about one architectural line:

1. generic nested substrate must stay generic
2. `try_except` policy must live in the operator layer

So my view is:

1. the current attempt chose the right broad direction
2. it is still too tactical in the generic runtime layer
3. it is good enough as a bring-up scaffold
4. it should be refactored before `map_`, `switch_`, or `reduce` build on top of the same shortcuts
