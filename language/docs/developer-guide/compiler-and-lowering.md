# Compiler and C++ lowering

Status: target pipeline with provisional runtime-function semantics

All execution modes share one pipeline:

```text
source
  -> tokens and syntax AST
  -> names and canonical value types
  -> recursive temporal shape expansion
  -> function classification
  -> phase-checked typed HIR
  -> hgraph semantic IR
  -> C++ source + build manifest
  -> native compiler and linker
  -> hgraph runtime
```

The classification arrow is a semantic stage rather than a parser shortcut.
Its first proposed rule classifies ordinary bodies as composition and bodies
containing node-only declarations or blocks as runtime evaluation.

## Frontend components

The implementation should grow around tested responsibilities:

```text
src/
  syntax/       source manager, lexer, parser, AST
  semantics/    names, canonical types, temporal shapes, function classifier
  ir/           typed HIR and hgraph semantic IR
  codegen/cpp/  generated C++ and source maps
  driver/       check, emit-cpp, build, run
  repl/         session assembly over the driver
```

The source manager owns file identities, byte offsets, line/column lookup, and
snippets. Diagnostics refer to source identities rather than scattering raw
filesystem paths through the AST.

## Common function representation

Parsing produces `UnclassifiedFn` for both named and anonymous functions. Its
signature stores:

- temporal parameters with canonical source types;
- `const` parameters with canonical value types and defaults;
- optional temporal result type;
- concise expression or block body;
- captures for anonymous functions once resolved;
- source ranges for every component.

Temporal shape expansion annotates parameters and results with their hgraph
schemas but does not choose graph or node lowering.

## Function classification

The classifier consumes resolved syntax and assigns `CompositionFn` or
`RuntimeFn`:

- no runtime-only construct produces `CompositionFn`;
- the presence of `state`, `inject`, `start`, `when`, or `stop` produces
  `RuntimeFn` for the complete body;
- a runtime function with invalid declaration order, duplicate lifecycle
  blocks, unsupported capabilities, or mixed phases is rejected.

The classifier must:

- be deterministic from source and resolved types;
- run before phase/effect checking;
- reject constructs that do not belong to the selected kind;
- preserve one classification across check, REPL, run, and build;
- never infer kind from C++ compiler behavior or registry candidate order.

Imported functions are different: their module descriptors provide a contract,
and hgraph's resolver selects a registered implementation that may be a graph
or native node without exposing that choice at the call site.

## Canonical type lowering

The type-shape layer recursively maps source types:

```text
f64
  -> atomic TS<Float> leaf

map<str, f64>
  -> keyed hgraph structure with TS<Float> values

atomic<map<str, f64>>
  -> TS<canonical map<str, f64> value>

map<str, atomic<tuple<f64, f64>>>
  -> keyed structure whose value endpoint carries a complete tuple

const window: i64
  -> Scalar<"window", Int>
```

`atomic` is erased from the canonical payload after it establishes the endpoint
boundary. It remains in source-level type identity and diagnostics where the
distinction from a structural value matters.

## Composition lowering

The classifier identifies this ordinary expression body as wiring
composition:

```hgl
fn midpoint(bid: f64, ask: f64) -> f64 =>
    (bid + ask) / 2.0
```

This lowers conceptually to public hgraph graph authoring:

```cpp
struct midpoint
{
    static hgraph::Port<hgraph::TS<hgraph::Float>>
    compose(
        hgraph::Wiring &w,
        hgraph::NamedPort<"bid", hgraph::TS<hgraph::Float>> bid,
        hgraph::NamedPort<"ask", hgraph::TS<hgraph::Float>> ask)
    {
        auto sum = hgraph::wire<hgraph::stdlib::add_>(w, bid, ask);
        return hgraph::wire<hgraph::stdlib::div_>(
                   w, sum, hgraph::Float{2.0})
            .as<hgraph::TS<hgraph::Float>>();
    }
};
```

The exact formatting is not contractual. The semantic requirements are:

- source `f64` expands to typed temporal ports;
- operator expressions dispatch through public hgraph markers;
- compatible literals use hgraph's normal scalar lifting;
- the result schema is validated;
- composition flattens through ordinary hgraph wiring.

An atomic tuple parameter lowers to one atomic endpoint. Indexing it must wire
an imported or generated extraction operation rather than read a current tuple
during composition.

## Runtime lowering

Consider a stateful runtime function with ordered handlers and explicit output
access:

```hgl
fn combined_total(a: f64, b: f64) -> f64 {
    state total: f64 = 0.0
    inject out

    when modified(a) && valid(a) {
        total += a
        out = total
    }

    when modified(b) && valid(b) {
        total -= b
        out = total
    }
}
```

For all ordered `when` predicates, the runtime semantic pass derives:

1. the union of activation dependencies derived from `modified(...)` terms;
2. validity admission requirements common to every executable handler;
3. ordered residual predicates that remain in the per-evaluation body.

For this example both inputs are active, but neither is globally
required-valid: each handler can execute without the other input. Both inputs
therefore use unchecked validity. The state declarations synthesize one hidden
recordable state schema and replay-aware startup initialization. The body
lowers conceptually to:

```cpp
using combined_total_state = hgraph::TSB<
    "combined_total_state",
    hgraph::Field<"total", hgraph::TS<hgraph::Float>>>;

struct combined_total
{
    static void start(hgraph::RecordableState<combined_total_state> state)
    {
        auto total = state.field<"total">();
        if (!total.valid()) { total.set(hgraph::Float{0.0}); }
    }

    static void eval(
        hgraph::In<"a", hgraph::TS<hgraph::Float>,
                   hgraph::InputValidity::Unchecked> a,
        hgraph::In<"b", hgraph::TS<hgraph::Float>,
                   hgraph::InputValidity::Unchecked> b,
        hgraph::RecordableState<combined_total_state> state,
        hgraph::Out<hgraph::TS<hgraph::Float>> out)
    {
        auto total = state.field<"total">();

        if (a.modified() && a.valid())
        {
            total.set(total.value().checked_as<hgraph::Float>() + a.value());
            out.set(total.value().checked_as<hgraph::Float>());
        }

        if (b.modified() && b.valid())
        {
            total.set(total.value().checked_as<hgraph::Float>() - b.value());
            out.set(total.value().checked_as<hgraph::Float>());
        }
    }
};
```

The exact generated spelling is not contractual. In particular,
the union of `modified(a)` and `modified(b)` makes both inputs active without a
separate outer test. Handler-specific validity remains in each ordered `if`.
If both handlers write the atomic output, hgraph coalesces modification at the
evaluation time and downstream nodes observe the final write. An input omitted
from the activation union becomes passive if it is still read by the body.

If a predicate cannot be represented completely by hgraph node metadata, the
compiler emits the remaining Boolean test inside `eval`. It must still prove
that every payload read is dominated by a static or flow-sensitive validity
check.

State initializers become `start` work that only seeds invalid fields, so
record/replay restoration is preserved. Explicit source `start` and `stop`
blocks become the corresponding static hooks. All state variables share one
typed state schema. A future ephemeral-cache form must lower separately and
must not cause one node to mix incompatible state selectors.

An inject declaration maps each approved source capability to its public
hgraph selector. The canonical signature includes lifecycle-only selectors
when necessary, while each generated hook requests only the subset it uses.
`inject out` exposes the `Out` selector to evaluation expressions; its schema
is the function's resolved result schema.

In a runtime function, `return value` lowers to `out.set(value); return;`.
Direct whole-output assignment calls `out.set` and continues evaluation.
Collection projections lower to the typed output view's incremental mutation
operations, preserving one combined delta for writes to distinct children.
Reaching the end without a return or output mutation produces no output tick.

Runtime lowering must obey hgraph's native contracts:

- generated node implementations are empty static structs;
- per-tick work lives in typed hooks;
- scheduling and validity use input metadata, not body simulations;
- state affecting future ticks is recordable;
- wiring-time policy selects an overload or immutable plan before evaluation;
- hot paths avoid schema discovery, registry lookup, and arbitrary resources.

Arithmetic and comparisons in a runtime body operate on admitted scalar
payloads and must follow language-defined scalar semantics rather than inherit
accidental C++ overflow or conversion behavior. Calls in a runtime body must
eventually resolve to an admitted scalar kernel or another implementation that
can execute without wiring. The first slice may reject such calls until that
contract is designed.

Output access during lifecycle hooks, ephemeral caches, and generated sink
behavior remain future source-design work.

## Imported function bridge

The compiler resolves a source call by supplying hgraph with expanded temporal
schemas, `const` values, and ordered/named arguments. The shared bridge owns:

- argument normalization and defaults;
- `TypePattern` matching and output resolution;
- scalar lifting;
- candidate ranking and `requires` predicates;
- selected implementation identity and build dependencies;
- detailed rejection diagnostics.

The compiler may cache deterministic results but must not copy the matching
algorithm. Generated C++ dispatches through the public marker again so
descriptor or registry drift becomes an error.

## Module descriptors and build manifests

A descriptor exposes source-level function contracts and implementation
metadata, plus:

- canonical module and compatibility versions;
- canonical types and their hgraph schemas;
- required public headers and CMake packages;
- imported targets and registration entry points;
- descriptor fingerprints and documentation links.

Selected declarations produce a deterministic build manifest. Only used
modules contribute headers, packages, targets, and registration calls.

Scalar-dependent native candidate constraints need either a declarative form
interpreted by hgraph's shared resolver or an isolated resolver helper. The
compiler must not approximate them.

## Source mapping and generated artifacts

Every generated declaration and meaningful expression maps to its language
range through `#line` directives and/or a sidecar map. A native error in
generated implementation detail is a compiler defect and reports the retained
artifact path plus compiler, SDK, module, profile, and target versions.

Generated output and manifests must be deterministic. Absolute developer
paths, timestamps, random identifiers, and unordered iteration must not affect
them.

## Scripted, REPL, and AOT drivers

`hgl run`, `hgl repl`, and `hgl build` use the same type expansion, function
classifier, typed IR, and C++ backend.

The initial REPL may materialize a synthetic module and rebuild the full
session. A failed declaration must not replace the last valid session.

A future JIT must consume the same classified semantic IR and pass cross-mode
parity before it can replace compile-and-run.
