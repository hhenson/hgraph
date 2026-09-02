# Compiler and C++ lowering

Status: target pipeline with provisional generic, module, and runtime semantics

All execution modes share one pipeline:

```text
source
  -> tokens and syntax AST
  -> package target and locked module closure
  -> module descriptors and candidate universe
  -> modules, nominal names, and canonical value types
  -> generic binding and operator conformance
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

- whether a named exact function carries the `export` modifier;
- type and `const` generic parameters;
- temporal parameters with canonical source types;
- `const` parameters with canonical value types and defaults;
- optional temporal result type;
- concise expression or block body;
- captures for anonymous functions once resolved;
- source ranges for every component.

Temporal shape expansion annotates parameters and results with their hgraph
schemas but does not choose graph or node lowering.

A parsed `operator` is represented separately as a bodyless `OperatorContract`
with a canonical `(module, name)` identity, generic signature, public parameter
roles, defaults, result relationship, and source range. Every
`OperatorContract` is public. Name resolution marks a same-named
`UnclassifiedFn` as an implementation of that identity only when a unique local
or selectively imported operator binding exists. Such an implementation is a
provider candidate automatically and rejects an `export` modifier; only an
unbound exact function may be exported directly.

## Function classification

The classifier consumes resolved syntax and assigns `CompositionFn` or
`RuntimeFn`:

- no runtime-only construct produces `CompositionFn`;
- the presence of `state`, `inject`, `start`, `when`, `stop`, or a runtime
  collection iterator produces `RuntimeFn` for the complete body;
- a runtime function with invalid declaration order, duplicate lifecycle
  blocks, unsupported capabilities, or mixed phases is rejected.

The classifier must:

- be deterministic from source and resolved types;
- run before phase/effect checking;
- reject constructs that do not belong to the selected kind;
- preserve one classification across check, REPL, run, and build;
- never infer kind from C++ compiler behavior or registry candidate order.

Operator implementations are registered after classification. Hgraph's
resolver may select either a graph or native-node candidate without exposing
that choice at the call site.

## Canonical type lowering

The type-shape layer recursively maps source types:

```text
f64
  -> atomic TS<Float> leaf

datetime
  -> atomic TS<DateTime> leaf

set<str>
  -> TSS<Str>

rolling<f64, 20>
  -> TSW<Float, 20, 20>

rolling<f64, 20, 5>
  -> TSW<Float, 20, 5>

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

`rolling` is not erased. It establishes a TSW endpoint whose resolved maximum
and minimum tick counts participate in schema identity. The compiler
normalizes an omitted minimum to the maximum, validates both values before
lowering, and rejects `atomic<rolling<...>>` and `const` rolling values.

Plain generic parameters lower to hgraph type-pattern variables at operator
and candidate boundaries. `const` generics that shape a rolling type must bind
through the same hgraph resolution record rather than a compiler-only side
table. Repeated variables must unify, and the resolved candidate must contain
no unbound type or size required by its inputs or output.

The current public hgraph type pattern represents TSW sizes as either concrete
values or one wildcard over the complete window shape. It does not yet bind
named maximum and minimum size variables. Generic
`rolling<T, max_size, min_size>` lowering therefore requires a public TSW
size-pattern extension integrated with `ResolutionMap`; the compiler must not
approximate this with private matching logic.

`let` and `var` lower to scoped native locals. `let` is immutable in the typed
HIR. `var` admits assignment but does not allocate node state. In composition
code a local may hold a scalar or port handle; in runtime code it holds a
canonical scalar or borrowed view. Only `state` lowers through a recordable
state selector.

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

## Metadata and collection-view lowering

Runtime `last_modified(value)` lowers directly to the endpoint view's public
`last_modified_time()` operation and produces a canonical `datetime` scalar.
It does not wire hgraph's similarly named temporal operator from inside an
evaluation hook.

`key_set(tsd)` has two phase-specific lowerings behind one source contract. A
composition call dispatches the standard key projection with a TSS output
shape. A runtime call obtains the current `TSDDataView::key_set()` borrowed
view. Both paths use public hgraph APIs.

The typed HIR represents `keys`, `values`, and `items` as borrowed
`RuntimeIterator` values carrying:

- the source collection and concrete hgraph shape;
- yielded binding types and endpoint or membership-slot provenance;
- an optional built-in, named, or inline predicate;
- a lifetime fixed to the current evaluation.

The iterator type is compiler-internal. It is valid only as the source of a
`for` loop and has no scalar schema, time-series schema, state representation,
or callable ABI.

Recognized metadata predicates select the matching public native range
directly. This includes the delta predicates and other filters such as
`valid`. For example:

```text
items(tsd, modified)  -> tsd.modified_items()
keys(tsd, removed)    -> tsd.removed_keys()
values(tsd, added)    -> tsd.added_values()
values(tss, added)    -> tss.added_values()
items(tsl, modified)  -> tsl.modified_items()
values(tsd, valid)    -> tsd.valid_values()
```

A general predicate initially lowers to the unfiltered range plus a native
`if`. Predicate analysis may extract a leading built-in metadata condition and
select a narrower native range, leaving the residual expression in the loop.
This is the iterator counterpart of decomposing a `when` condition into native
admission metadata and residual evaluation logic. A known source predicate is
inlined or emitted as a direct call; the generated loop does not allocate a
callable object.

TSB traversal is statically expanded in schema order so each heterogeneous
child retains its concrete type. The predicate and body are instantiated and
checked for each field. TSL traversal uses ascending indices, converting the
native index to checked `i64`. TSD and TSS retain native view order.

The current public TSL input view exposes modified ranges but not the requested
added and removed ranges for an unbounded TSL. That language surface requires
a first-class public hgraph view API before code generation lands. The compiler
must not reach into private TSData operation tables or reconstruct structural
deltas independently.

## Nominal operator bridge

Source name resolution produces a canonical `OperatorId` before candidate
matching. A selective import can introduce one operator identity under an
unqualified short name; a module alias resolves a qualified call such as
`analytics::rolling_mean`. Distinct identities never contribute candidates to
one another even when their short names are equal.

The compiler resolves an operator call by supplying hgraph with that identity,
expanded temporal schemas, `const` values, generic bindings, and ordered/named
arguments. The shared bridge owns:

- argument normalization and defaults;
- `TypePattern` matching and output resolution;
- scalar lifting;
- candidate ranking and `requires` predicates;
- selected implementation identity and build dependencies;
- detailed rejection diagnostics.

The compiler may cache deterministic results but must not copy the matching
algorithm. Generated C++ dispatches through the public marker again so
descriptor or registry drift becomes an error.

A source-defined operator lowers to a deterministic generated C++ marker. Each
compatible same-named `fn` lowers to an explicitly registered graph or node
candidate according to its classified body. An ordinary `fn` without an
operator binding lowers as an exact callable and is not placed in a registry.
Only an ordinary `export fn` is emitted into the module's public exact-function
surface.

The generated marker or descriptor mapping must preserve the full nominal
identity rather than using an unqualified registry string that could collide
with another module.

## Module descriptors and build manifests

A descriptor separates its importable interface from its provider inventory.
The interface contains automatically public nominal operators and explicitly
exported exact functions. The provider inventory contains every
candidate-to-operator binding, including the provider module identity and
implementation metadata, plus:

- canonical module and compatibility versions;
- canonical types and their hgraph schemas;
- required public headers and CMake packages;
- imported targets and module lifecycle and registration entry points;
- descriptor fingerprints and documentation links.

The compiler constructs the candidate universe from every source module in the
application target and every module in its locked transitive package closure.
It does not infer provider participation from source imports and does not scan
installed packages. The deterministic build manifest links every participating
provider and directly references its initialization entry point, preventing
static-library dead stripping. A candidate-universe fingerprint must agree with
the registrations installed before graph wiring.

Scalar-dependent native candidate constraints need either a declarative form
interpreted by hgraph's shared resolver or an isolated resolver helper. The
compiler must not approximate them.

## Generated module lifecycle and ownership

Each compiled module exposes compiler-generated lifecycle functions through a
versioned public ABI. The exact spelling is provisional, but the semantic split
is required:

```cpp
ModuleHandle init_module(ModuleContext &context);
void deinit_module(ModuleContext &context, ModuleHandle handle);
```

`init_module` starts a registration transaction for the module's canonical
identity and descriptor fingerprint. It records one keyed installer containing
all type registrations, operator candidates, and native associations, then
commits an opaque `ModuleHandle`. A failed transaction rolls back without
leaving a partial candidate set. Repeating initialization for the same active
module instance is idempotent.

The application compiler emits one bootstrap that directly invokes every
module initializer in dependency order and then runs all installers before
wiring. Registry reset replays the installers, not the one-time initialization
hooks. The bootstrap retains each handle and deinitializes modules in reverse
dependency order.

`deinit_module` removes by provider handle rather than issuing candidate-level
erase calls. Removal first prevents new resolution against the provider, then
removes its currently installed candidates, exact-function metadata, type
associations, and installer intent. Removing installer intent is essential: a
later registry reset must not resurrect the provider.

Operator selection and exact-function lowering attach a provider lease to each
resulting graph, node plan, or cached callable that may reference module code or
metadata. Deinitialization must wait or report `module in use` while such leases
or dependent modules remain. Native-library unloading is a later, stricter step
allowed only when no installer callback, generated function, or type metadata
points into that image. Logical removal may retain the image for process
lifetime in the initial implementation.

The current public hgraph `OperatorRegistry` provides keyed installers and
reset replay but no provider-scoped removal or installer unregistration. Hgraph
must gain a first-class module registration transaction/handle, candidate
provenance, removal, and lease contract. Generated language code must not reach
into registry storage or attempt to coordinate several registries privately.

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

Replacing a REPL module stops and destroys graphs holding its leases, removes
the old module handle and installer intent, initializes the replacement, and
rebuilds the registry from the active module set. If removal cannot complete,
the old revision remains active and the replacement fails atomically. Retaining
old native images is acceptable; retaining their candidates is not.

A future JIT must consume the same classified semantic IR and pass cross-mode
parity before it can replace compile-and-run.
