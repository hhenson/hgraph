# RFC: C++ Compute Node Authoring and Builder Exposure

Date: 2026-03-04  
Status: Draft for discussion  
Scope: Define how to implement C++ compute nodes and expose them to Python wiring for migration.

## 1. Context

This RFC complements `2026-03-cpp-compute-node-migration-rfc.md`.

That RFC defines:
1. Python-derived stable node ids.
2. Python-owned mapping from node id to C++ callable.
3. Runtime backend selection in Python wiring (`HGRAPH_USE_CPP`).

This RFC defines:
1. The C++ authoring model for new compute nodes.
2. How C++ node builders should be exposed to Python.
3. Practical options and trade-offs for performance and maintainability.

## 2. Goals

1. Provide a clear, repeatable C++ implementation pattern for migrated compute nodes.
2. Keep hot-path execution in C++ with no per-tick Python conversion/import overhead.
3. Keep Python API shape unchanged.
4. Keep routing ownership in Python wiring (no C++ registry).
5. Keep dual runtime operation (`HGRAPH_USE_CPP=0/1`) during migration.

## 3. Non-Goals

1. Changing operator overload semantics.
2. Replacing Python wiring resolution logic.
3. Introducing a C++-owned mapping/registry for operator ids.
4. Removing Python implementations during migration.

## 4. Current State Review

## 4.1 Existing primitives are solid

1. `Node` in `cpp/include/hgraph/types/node.h` provides lifecycle, active/valid gating, scheduling, and endpoint ownership.
2. `NodeBuilder` in `cpp/include/hgraph/builders/node_builder.h` precomputes endpoint schema metadata and builds nodes.
3. `TSView`/`TSInputView`/`TSOutputView` provide direct C++ access APIs, including collection iterables and delta access.

## 4.2 C++ runtime currently executes Python compute logic for standard compute nodes

1. `PythonWiringNodeClass` currently uses `_hgraph.PythonNodeBuilder` in C++ mode.
2. `PythonNodeBuilder` constructs `PythonNode`/`PushQueueNode`, which call Python callables during `do_eval`.
3. This is correct for parity, but it is not the final path for C++ compute kernels.

## 4.3 C++ structural nodes exist and show the current extension pattern

1. `TsdMapNode`, `ReduceNode`, `SwitchNode`, `MeshNode`, etc., each have dedicated C++ node and builder classes.
2. Builders are exposed via nanobind and selected from Python wiring (`_use_cpp_runtime.py`).
3. This demonstrates that Python can already instantiate C++ builders directly.

## 4.4 Gaps to address for compute-node migration

1. There is no dedicated C++ compute-node authoring contract yet.
2. `_hgraph_nodes.cpp` is currently empty while several node registration hooks exist elsewhere.
3. Builder exposure conventions are inconsistent (some are classes, future mapping prefers callable symbols).

## 5. Constraints

1. Mapping remains Python-owned.
2. Node id derivation remains Python-owned.
3. C++ receives "build this node" requests only through mapped callables.
4. No C++ lookup registry is introduced.
5. C++ path should avoid Python conversion in hot loops.

## 6. Declared C++ Authoring Contract

Declared implementation path:
1. Author nodes as lightweight `Spec` types.
2. Instantiate runtime nodes via generic `NodeImpl<Spec>`.
3. Instantiate builders via generic `CppNodeBuilder<Spec>`.
4. Do not add `should_eval` customization at this stage; use existing `Node::eval()` gating semantics.

## 6.1 Spec contract

Required:
1. `static void eval(Node& node, state_t& state)` or `static void eval(Node& node)` (state-free form).

Optional:
1. `using state = ...` (default: `std::monostate`).
2. `static state make_state(Node& node)` (default-initialized state when absent).
3. `static void init(Node& node, state& s)`.
4. `static void start(Node& node, state& s)`.
5. `static void stop(Node& node, state& s)`.
6. `static void dispose(Node& node, state& s)`.

Notes:
1. Hook detection is compile-time (`requires`/concept checks).
2. Only implemented hooks are called; absent hooks are zero-cost no-ops.
3. Parse/cache scalar configuration in `make_state` or `init`, not per tick.
4. Use `TSInputView`/`TSOutputView` and typed collection APIs in `eval`.
5. Prefer `DeltaView`/`delta_payload()` and avoid Python conversion on hot paths.

## 7. Exposure Contract to Python Wiring

Python wiring should call mapped C++ callables directly and receive a `NodeBuilder`.

Recommended callable shape:

```python
# Python side usage (mapping value)
builder = _hgraph.op_getattr_cs(
    signature=node_signature,
    scalars=scalars,
    resolved_wiring_signature=resolved_wiring_signature,  # optional
)
```

C++ side callable returns:

1. A concrete `NodeBuilder` instance (`nb::ref<NodeBuilder>` / bound subclass).
2. No lookup by id inside C++.
3. No mutable global registry.

## 7.1 Builder Exposure Reduction Strategy (No Macros by Default)

Declared approach:
1. Use one generic binder template (`bind_cpp_node_builder_factory<Spec>`).
2. Keep a central spec pack/list in `_hgraph_nodes.cpp`.
3. Register factories with a fold-expression over the list.

Rationale:
1. Avoid per-node binding boilerplate while preserving compile-time type safety.
2. Keep code easy to debug (templates/functions instead of macro expansion).
3. Centralize exposure in one place without introducing runtime registries.

Macro policy:
1. Do not use macros for first implementation phase.
2. Revisit X-macro/codegen only if one spec list must drive multiple generated artifacts.

## 8. Options

## Option A (Declared): Spec + Generic NodeImpl/Builder

Pattern:
1. Define `Spec` with required `eval` and optional hooks.
2. Alias `using MyBuilder = CppNodeBuilder<MySpec>`.
3. Expose mapped callable returning `MyBuilder`.

Pros:
1. Best runtime profile (compile-time dispatch, no virtual-on-virtual policy layer).
2. Very low per-node boilerplate.
3. Preserves centralized `Node::eval()` semantics and error model.
4. Keeps node authoring lightweight and explicit.

Cons:
1. Requires careful spec hook contract documentation.
2. Visitor/type-registration implications must be handled once for `NodeImpl<Spec>`.

## Option B: Concrete Node + Concrete Builder per operator

Pattern:
1. `MyNode : Node`.
2. `MyNodeBuilder : BaseNodeBuilder`.
3. One class pair per operator.

Pros:
1. Straightforward debugging/profiling.
2. No shared generic scaffolding risks.

Cons:
1. Highest boilerplate and maintenance burden.

## Option C: Runtime-erased kernel object (`Node(std::unique_ptr<Ops>)`)

Pattern:
1. `Node` stores runtime-erased policy object.
2. Eval/start/stop dispatched through policy object.

Pros:
1. Maximum runtime flexibility.

Cons:
1. Per-instance allocation/indirection unless optimized heavily.
2. More runtime complexity than currently needed.
3. Harder to keep zero-overhead behavior.

## 9. Recommendation

Recommend:
1. Adopt Option A as the declared implementation approach.
2. Keep Option B only for exceptional cases where a bespoke class is required.
3. Keep Option C out of scope for current migration phases.

Why:
1. Option A gives low boilerplate with near-zero runtime abstraction cost.
2. It preserves existing `Node` lifecycle/gating behavior while simplifying authoring.
3. It aligns with Python-owned mapping and fast incremental migration.

## 10. Proposed Authoring Skeleton (Declared Option A)

```cpp
// minimal spec: only eval required
struct GetAttrCsSpec {
    static void eval(Node& node) {
        auto in = node.input().as_bundle().field("ts");
        auto out = node.output();
        if (in && in.valid()) {
            // Placeholder for real C++ logic (avoid Python conversion on hot paths).
            out.from_python(in.to_python());
        }
    }
};

using GetAttrCsBuilder = CppNodeBuilder<GetAttrCsSpec>;
```

Python-exposed factory example:

```cpp
m.def("op_getattr_cs", [](node_signature_s_ptr signature, nb::dict scalars) {
    return nb::ref<NodeBuilder>(new GetAttrCsBuilder(std::move(signature), std::move(scalars)));
});
```

Optional-state spec example:

```cpp
struct MySpec {
    struct state { int cached_flag; };
    static state make_state(Node& node) { return {/* decode from node.scalars() */ 0}; }
    static void start(Node& node, state& s) { /* optional */ }
    static void eval(Node& node, state& s) { /* required */ }
    static void stop(Node& node, state& s) { /* optional */ }
};
```

## 11. Build and Registration Touchpoints (Option A)

For each migrated compute operator:

1. Add or extend spec declaration/implementation (no bespoke node/builder class required by default).
2. Ensure generic `NodeImpl<Spec>`/`CppNodeBuilder<Spec>` scaffolding is compiled once.
3. Expose callable symbol (`op_xxx`) from `_hgraph` that returns `CppNodeBuilder<Spec>`.
4. Add Python mapping entry in wiring-controlled mapping table.
5. Add visitor/type integration only where required by existing runtime inspection/binding paths.

## 12. Performance Guardrails

For C++ compute nodes:

1. No per-tick `nb::module_::import_`.
2. No per-tick `std::getenv` on hot paths.
3. No per-element Python conversion loops when C++ views/iterables are available.
4. Prefer direct keyed/indexed lookup APIs (`get`, `contains`, `at_key`, `at(index)`).
5. Cache scalar decoding in `make_state`/`init` once per node instance.
6. Keep fallback logic explicit and limited; no hidden semantic fallback branches.
7. Keep `should_eval` behavior centralized in `Node::eval()` for now.

## 13. Test Matrix

For each migrated node:

1. Targeted parity tests with `HGRAPH_USE_CPP=0`.
2. Targeted parity tests with `HGRAPH_USE_CPP=1`.
3. Full `hgraph_unit_tests` pass in both modes before merge.
4. Add focused microbench for node-level throughput/latency on representative input shape.

## 14. Open Decisions

1. Callable signature for mapped C++ factories:
   - minimal (`signature`, `scalars`) vs compatibility superset kwargs.
2. Exposure style:
   - factory-only (`op_xxx`) vs class + factory.
3. Whether to wire existing `register_*_node_with_nanobind` hooks via `_hgraph_nodes.cpp` or remove dead hooks.
4. When to introduce Option C generation layer.

## 15. Initial Implementation Plan

Phase 1:
1. Implement and land generic `NodeImpl<Spec>` + `CppNodeBuilder<Spec>` scaffolding.
2. Implement one pilot compute node spec end-to-end with mapping.
3. Capture parity/perf evidence.

Phase 2:
1. Migrate 3-5 high-value compute nodes using same pattern.
2. Consolidate repeated glue points.

Phase 3:
1. Decide on Option C generation based on repeated boilerplate and defect rate.
2. Keep Option A runtime model unchanged.
