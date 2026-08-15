---
name: hgraph-compute-sink-node
description: Implement or review C++ hgraph compute nodes, sink nodes, algorithms, and concrete operator-node specializations. Use when adding or changing a static node `eval`, lifecycle hooks, local or recordable state, REF usage, incremental or windowed algorithms, operator strategy selection or overload registration, type-erased node strategy, node naming, complexity documentation, or native/Python behavioral tests for a compute or sink node.
---

# HGraph Compute and Sink Nodes

Build C++-first nodes whose per-tick path is small, whose lifecycle and state
semantics are explicit, and whose names and tests fit the operator system.

## Start from the existing contract

1. Read the repository `AGENTS.md` and the relevant operator definition,
   implementation, registration, and tests before editing.
2. Read the applicable parts of
   `docs/source/user_guide/cpp/authoring_nodes.rst`. Use
   `tests/cpp/test_static_node.cpp` as the compact executable reference for
   lifecycle hooks, `State`, and `RecordableState`.
3. Classify the work before choosing a type:
   - Use a compute node for time-series input plus an output.
   - Use a sink node for time-series input and a side effect with no output.
   - Implement an operator specialization when the behavior belongs to an
     existing abstract operator or valid implementations may vary by type or
     algorithm.
   - Introduce a standalone node only when the behavior is not an operator and
     cannot be expressed clearly as a graph.

Keep static node implementation structs empty. Put instance data in the
appropriate selector or plan rather than in members.

Keep the node's per-tick implementation logic in `eval`. Extract a helper only
when it is a well-defined operation with its own clear contract or is genuinely
shared by multiple callers. Do not scatter one node's implementation across
single-use helper functions. Keep one-off lifecycle work in `start` and `stop`
as described below.

## Design the hot path first

Treat `eval` as the hot path. Leave only work that depends on the current tick.

- Prefer typed `In`, `Out`, `State`, and `RecordableState` access.
- Avoid schema discovery, registry lookup, overload selection, RTTI, string
  construction, parsing, policy selection, and representation selection in
  `eval`.
- Avoid avoidable allocation, container growth, reference counting, locking,
  and Python conversion in `eval`. Reuse pre-sized storage when the algorithm
  requires scratch space.
- Resolve wiring-time scalar policies to a concrete overload or plan. Do not
  branch on a policy string every tick.
- Read each input view only as often as needed. Use modified/delta access when
  full-value traversal is unnecessary.
- Emit no output when the contract calls for no tick; do not manufacture an
  unchanged value merely to simplify control flow.

If expensive work appears unavoidable, state why, inspect neighboring hot-path
code, and add focused performance evidence for a material regression risk.

## Use REF deliberately

Treat `REF` as a semantic indirection with significant memory overhead, not as
the default way to connect nodes. Input bindings to non-`REF` outputs are
lightweight C++ structures.

- Use a normal binding when the consumer only needs to read an output.
- Use `REF` when indirection prevents a value from being copied from an input
  to an output. Selection and routing operators such as `if_then` and `route`
  are the primary pattern: capture the selected source output and direct that
  source instead of copying its current value.
- Use `REF` when dynamic source identity is part of the operator contract.
- Before adding `REF`, state which copy or dynamic binding it avoids and verify
  that plain input binding cannot express the behavior.

Do not reject `REF` merely because it is expensive. Use it whenever the
indirection is required, and pay its overhead intentionally.

## Place one-off work in lifecycle hooks

Use `start` for work performed once per node lifetime, including:

- initializing `State`, or seeding `RecordableState` only when it is invalid;
  never overwrite recordable state restored for replay;
- initializing sequences, cursors, buffers, or cached plans associated with
  that state;
- acquiring run-scoped resources or establishing subscriptions;
- scheduling the initial evaluation when declarative `schedule_on_start` is
  insufficient.

Use `stop` to flush, finalize, unsubscribe, or release what `start` acquired.
Keep teardown safe for partial lifecycle progress and follow existing scope
guard patterns where rollback is required.

Request only the selectors each hook needs. When lifecycle-only arguments are
required, use the established explicit `signature_args` pattern rather than
polluting `eval`.

## Choose state by semantics

- Use no state for a pure transformation or side effect.
- Use `State<T>` for private, ephemeral implementation state that does not
  participate in record/replay.
- Use `RecordableState<TSchema>` when a tick updates state that affects later
  ticks: this is loopback or feedback state and should be observable and
  restorable by record/replay.
- Do not combine `State` and `RecordableState` in one static node.
- Keep recordable state structured and typed. Update only the fields modified
  by the current tick.

Do not replace semantic loopback state with an opaque cache merely because the
cache is easier to implement.

## Implement algorithms incrementally

Prefer an incremental algorithm that consumes the current tick or delta and
updates only the sufficient statistics needed for the result. Do not retain an
entire history or window in private state when an online formulation exists.
Minimal sufficient statistics are algorithmic state; they are preferable to
capturing the input history.

- Put incremental state that affects later ticks in `RecordableState`.
- When an exact result intrinsically requires the window contents, use `TSW`
  as the semantic window rather than copying the window into private state.
  Exact median is the standard example.
- Do not maintain a private duplicate of a `TSW` merely to simplify the
  implementation.
- Test an incremental implementation against a simple full-recomputation
  reference over multiple ticks, including removals and boundary conditions.

Document algorithmic cost beside the implementation and public operator:

- state the worst-case or amortized time cost per tick;
- state retained-memory cost;
- for `TSW`, express both costs in terms of window size `W` and document the
  supported window semantics.

## Exploit concrete C++ types

Use type erasure at boundaries that genuinely need to accept independently
realized types. Once wiring or plan construction selects a concrete strategy,
make the per-tick implementation typed.

- Template an implementation when its scalar or time-series types vary, and
  use those template parameters to remove runtime conversion and dispatch.
- Prefer a concrete typed overload over inspecting `TSTypeKind` or a schema in
  `eval`.
- Select erased representation operations once from immutable wiring or plan
  metadata, then dispatch through the installed ops table.
- Follow the repository passive ops-table plus explicit erased-ownership
  pattern for a reusable erased contract. Do not add a facade `std::variant`
  or scatter strategy branches through semantic node code.
- Preserve native C++ authoring as the primary path. Adapt Python values and
  callables to the same node rather than implementing separate Python runtime
  semantics.

## Name operator implementations consistently

Name a concrete operator node from the operator plus a concise specialization
suffix, using lower snake case.

- If the operator name ends in `_`, append the suffix directly:
  `add_` + `float` becomes `add_float`.
- Otherwise insert `_` as the separator:
  `debug_print` + `tsb` becomes `debug_print_tsb`.
- Describe the specialization by the distinguishing type, shape, policy, or
  direction. Avoid generic suffixes such as `impl` when a precise name exists.
- Apply the same rule to the implementation type and any explicit diagnostic
  `name` unless a nearby registration convention requires a different label.

Keep the abstract operator in its operator header, the concrete node under the
existing `impl` boundary, and register it through the neighboring operator
registration mechanism.

Use the operator as the public abstraction whenever implementation selection
may vary by type or algorithm. Keep concrete node choices behind overload
resolution or the operator's wiring contract.

When an operator offers meaningful algorithmic trade-offs, expose a strongly
typed enum as a wiring-time scalar policy:

- Give each enum value a name that describes the accepted property or risk.
- Document accuracy, overflow, numerical stability, time, and memory trade-offs
  that differ between values.
- Select the concrete overload or immutable plan before execution; do not
  switch on the enum in `eval`.
- Use a graph-level switch only when the policy is genuinely time-varying.

For example, average may offer sum divided by count, with overflow risk, and an
online recurrence based on the previous average, count, and next value, with
different floating-point accuracy behavior. Test every exposed strategy.

## Document standalone nodes

For a node that is not an operator, add documentation beside its public or
implementation declaration that states:

- why a primitive node is required instead of a graph or existing operator;
- its input activation and validity behavior;
- its output or side-effect contract;
- its lifecycle and state semantics;
- any intentional hot-path cost or external-resource behavior.

Add user-facing documentation when the standalone node is public. Do not add
domain-specific behavior to core merely because the algorithm is generic.

## Test through public wiring

1. Add native C++ behavior coverage using a concrete minimal graph and
   `eval_node`. Cover sink side effects without driving runtime internals
   directly.
2. Cover multiple ticks for stateful nodes, including initialization, update,
   no-tick behavior, and teardown where relevant.
3. Prove recordable loopback state through its public recordable-state behavior.
4. Add equivalent Python authoring/bridge coverage for every Python-visible
   behavior; keep the semantic implementation in C++.
5. Test invalid input, passive/active behavior, and lifecycle failure in
   proportion to the risk.
6. Run focused tests while iterating, then every acceptance gate required by
   the current repository `AGENTS.md`. Add an installed-SDK consumer check for
   public-header changes and cross-platform validation for large runtime or
   type-erasure changes.

Before handing off, review the final diff specifically for work that can move
out of `eval`, unnecessary `REF`, retained history that can become incremental,
state that should be recordable, undocumented per-tick or window cost, policy
branches that should select an overload, per-tick erased dispatch that can
become typed, and names that do not identify their operator specialization.
