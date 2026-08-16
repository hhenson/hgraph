---
name: hgraph-write-graphs
description: Implement or review composable C++ hgraph graphs and Python `@graph` functions. Use when adding or changing graph composition, wiring-time type or scalar decisions, graph overloads and polymorphism, `GlobalState` or `LOGGER` graph injection, conditional wiring, graph documentation, or native/Python graph behavior tests.
---

# Write HGraph Graphs

Build behavior by composing small typed nodes and graphs at wiring time. Prefer
graphs as the ordinary unit of reuse; reserve new nodes for genuine runtime
primitives or demonstrated performance needs.

## Start from the existing contract

1. Read the current repository's `AGENTS.md`, the relevant operator and graph
   code, and nearby tests before editing.
2. Establish whether the task targets hgraph core or a downstream extension:
   - In the hgraph core checkout, read the applicable parts of
     `docs/source/user_guide/cpp/authoring_graphs.rst`,
     `docs/source/user_guide/python/tutorial/graph.rst`, and
     `docs/source/developer_guide/graph_wiring.rst`.
   - In a downstream project, use that project's graph conventions and tests,
     plus the public hgraph documentation for its pinned hgraph version. Do
     not assume the hgraph core source tree is present.
3. Identify the existing nodes, operators and graphs that already provide the
   required primitives. Compose them before creating a new primitive.
4. Preserve equivalent first-class C++ wiring and Python authoring behavior
   for every public cross-language feature.

Apply all repository documentation and type-system rules. A graph is not a
shortcut around signature validation, generic resolution or operator dispatch.

## Prefer a graph to a node

Start new behavior as a graph. A graph combines small, purpose-specific,
efficient and well-tested nodes into more complicated behavior, reducing the
new runtime code and validation surface.

Create a node only when the behavior requires a new per-tick primitive, side
effect, lifecycle service, runtime state transition, or a measured performance
improvement that composition cannot provide. Document that reason. Apply
`$hgraph-compute-sink-node` when implementing or reviewing the node.

Keep the trade-off explicit:

- Prefer composition and reuse by default.
- Measure before replacing a clear graph with a fused node for performance.
- Keep a justified fused node narrow and expose it through the same operator or
  graph contract where practical.

## Keep graph work at wiring time

A C++ graph's `compose` method and a Python `@graph` function execute once while
the graph is wired. Graphs flatten into their nodes and do not exist as runtime
evaluation objects.

Keep the graph's wiring implementation in `compose` or the decorated graph
function. Extract a helper only when it is a well-defined operation with its
own clear contract or is genuinely shared by multiple callers. Do not scatter
one graph's composition across single-use helper functions.

- Treat ports as typed wiring handles, never as current values.
- Inspect scalars, resolved types and wiring metadata only to choose topology.
- Do not retain runtime state or expect the graph body to run on a tick.
- Do not perform runtime side effects in a graph body.
- Move lifecycle and tick-dependent behavior into nodes.

The topology, overloads, bindings and policies selected by the graph are fixed
when graph composition returns.

## Compose for reuse and polymorphism

Use graphs as the primary workers for reusable behavior:

- Factor repeated compositions into small graphs with precise typed
  signatures.
- Compose graphs from other graphs; do not duplicate their internal nodes.
- Use generic graph signatures for type-safe reuse across compatible schemas.
- Use operator overloads when callers need polymorphic selection by type,
  shape or wiring-time algorithm policy.
- Use higher-order graph parameters when the caller should supply behavior.
- Keep wiring-time policy selection out of every node's `eval` path.

Prefer an operator contract over exposing one concrete graph when multiple
valid graph or node implementations may exist.

## Limit graph injectables

Graphs support only `GlobalState` and `LOGGER` as injectables.

- In Python, declare either injectable with a `None` default and never supply
  it from a graph call.
- Use `GlobalState` for graph-scoped wiring configuration and values that seed
  the built graph. Runtime reads or writes still belong in nodes.
- Use `LOGGER` to report wiring choices, resolved types and selected policies.
  It is the logger selected for graph wiring, not a per-tick node view.
- In C++, access the equivalent services through `Wiring::global_state()` and
  `Wiring::logger()`.
- Do not inject `STATE`, `RECORDABLE_STATE`, `SCHEDULER`, `CLOCK`,
  `EvaluationEngineApi`, `Traits` or `NODE` into a graph. Put behavior that
  needs a runtime service in a node.

Use a node `LoggerView` or the logging operator for runtime values. A graph
logger cannot observe ticks because the graph body has already finished.

## Make wiring choices explicit

A graph may inspect resolved type information, scalar configuration and wiring
metadata; perform ordinary conditional logic; select overloads; and log why it
made a choice.

- Base Python type decisions on resolved annotations, `AUTO_RESOLVE` values and
  supported metadata APIs.
- Base C++ decisions on concrete template types and wiring-time scalar values.
- Log a material type, policy or implementation choice when it helps explain
  the built topology.
- Use runtime control-flow operators such as selection, switching, mapping and
  mesh when a time-series value must change behavior after wiring.

Never read a time-series value to choose topology. Never imply that a logged
wiring choice can change later in the run.

## Preserve documentation and type rules

- Give every graph a complete input and output signature.
- Keep generic variables linked consistently across inputs, outputs and scalar
  resolution parameters.
- Return the declared time-series shape and preserve the established `REF`,
  dereference and structural binding rules.
- Keep scalar policies at wiring time and use enums where several named
  trade-offs are supported.
- Document public behavior, wiring-time choices, supported types, error cases
  and any measured reason for choosing a node over a graph.
- Update authoritative operator and type documentation with the implementation.

## Test through public wiring

1. Test graph behavior through `eval_node` with concrete public signatures.
2. Add equivalent native C++ and Python coverage for Python-visible behavior.
3. Cover every conditional wiring branch, generic type specialization and
   exposed algorithm policy.
4. Assert invalid types and unsupported graph injectables fail during wiring.
5. Test wiring-time logging without confusing it with runtime log output.
6. Prefer behavior assertions over internal node-count or index assumptions,
   except when topology itself is the contract.
7. Run focused tests while iterating, then every acceptance gate required by
   the current repository `AGENTS.md`.

Before handing off, review the final diff for runtime work left in the graph
body, a new node that could be a composition, time-series-dependent wiring,
unsupported injectables, unresolved type variables, and wiring decisions that
are not documented or tested.
