# Compiler and C++ lowering

Status: target pipeline with agreed structured-value lowering and provisional
generic, module, and runtime semantics

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
  -> direct wiring (test, repl, run)  -> hgraph runtime, in process
  -> C++ source + build manifest -> native compiler -> hgraph runtime
```

The last two lines are the two backends over one semantic IR; the section
"Direct-wiring backend" below records the first, and the architecture record
("Two backends, one wiring") records which command uses which.

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
  wiring/       direct-wiring backend: IR walk over hgraph's erased dispatch,
                harness sequences, test runner
  codegen/cpp/  generated C++ and source maps
  driver/       check, test, emit-cpp, build, run
  repl/         session assembly over the driver
```

The source manager owns file identities, byte offsets, line/column lookup, and
snippets. Diagnostics refer to source identities rather than scattering raw
filesystem paths through the AST.

`src/syntax/` is implemented as follows. `source` holds a file's path, text,
and line table; every token and node carries a half-open byte range into
it. `diagnostic` collects `Category`-tagged diagnostics with optional notes
and renders them as `path:line:col: category: message` plus the source line
and a caret. `temporal` parses and validates the temporal literal spellings
of the syntax guide into a `TemporalValue` (kind plus microseconds, offset,
and zone) and prints the canonical spelling. `lexer` produces one token
vector per file, with comments as trivia and one `Newline` token per run of
terminators. `ast` is an index-based arena: nodes are `std::variant`
payloads addressed by `NodeId`, so the tree owns no pointers and a module is
one movable value. `parser` is a hand-written recursive-descent parser over
the token vector that applies the newline rules of the syntax guide and
recovers at the synchronization points listed there; `ast_printer` dumps
the tree one node per line for `hgl check --dump-ast` and the tests.

## Common function representation

Parsing produces `UnclassifiedFn` for both named and anonymous functions. Its
signature stores:

- whether a named exact function carries the `export` modifier;
- type and `const` generic parameters;
- temporal parameters with canonical source types;
- `const` parameters with canonical value types and defaults;
- optional temporal result type;
- an optional checked generic-constraint expression;
- concise expression or block body;
- captures for anonymous functions once resolved;
- source ranges for every component.

Temporal shape expansion annotates parameters and results with their hgraph
schemas but does not choose graph or node lowering.

A parsed `operator` is represented separately as a bodyless `OperatorContract`
with a canonical `(module, name)` identity, generic signature, public parameter
roles, defaults, result relationship, and source range. Every
`OperatorContract` is public. Name resolution binds an `UnclassifiedFn` that
carries the `impl` modifier to the unique local or selectively imported
operator of its name, reports an error when no such operator exists, and
reports a conflict for a plain `fn` whose name is an in-scope operator. A bound
implementation is a provider candidate and rejects an `export` modifier; only
an unbound exact function may be exported directly.

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

date
  -> atomic TS<Date> leaf       (CivilDate)

time
  -> atomic TS<Time> leaf       (CivilTime)

datetime
  -> atomic TS<DateTime> leaf   (Instant)

duration
  -> atomic TS<TimeDelta> leaf  (Duration)

civil_datetime
  -> atomic TS<CivilDateTime> leaf

timezone
  -> atomic TS<ZoneId> leaf

zoned_datetime
  -> atomic TS<ZonedDateTime> leaf

zoned_time
  -> atomic TS<ZonedTime> leaf  (hgraph-side addition)

tuple<f64, str>
  -> UnNamedTSB<Field<"_0", TS<Float>>, Field<"_1", TS<Str>>>

list<f64>
  -> TSL<Float, 0>          (0 is hgraph's dynamic-size sentinel)

list<f64, 3>
  -> TSL<Float, 3>

list<f64, n>              (const generic n)
  -> TSL<Float, SIZE<"n">>

set<str>
  -> TSS<Str>

rolling<f64, 20>
  -> TSW<Float, 20, 20>

rolling<f64, 20, 5>
  -> TSW<Float, 20, 5>

rolling<f64, 5m>
  -> registry tsw_duration(Float, 5m, 5m)

rolling<f64, 5m, 1m>
  -> registry tsw_duration(Float, 5m, 1m)

map<str, f64>
  -> keyed hgraph structure with TS<Float> values

atomic<map<str, f64>>
  -> TS<canonical map<str, f64> value>

map<str, atomic<tuple<f64, f64>>>
  -> keyed structure whose value endpoint carries a complete tuple

struct Quote {
  bid: f64
  ask: f64
}
  scalar   -> Bundle<"module::Quote",
                     Field<"bid", Float>, Field<"ask", Float>>
  temporal -> TSB<"module::Quote",
                   Field<"bid", TS<Float>>, Field<"ask", TS<Float>>>

atomic<Quote>
  -> TS<Bundle<"module::Quote", ...>>

const window: i64
  -> Scalar<"window", Int>
```

For nested HGL structs and containers, the compiler constructs each `TSB`
field schema recursively rather than using the native `TSBFromScalar`
convenience alias, whose field lift is intentionally shallow. The resulting
named TSB's value schema must be the same module-qualified named Bundle used by
scalar and atomic contexts.

`atomic` is erased from the canonical payload after it establishes the endpoint
boundary. It remains in source-level type identity and diagnostics where the
distinction from a structural value matters.

Struct symbols carry ordered field descriptors containing the source type,
required/default/optional construction metadata, and source range. That
metadata does not alter native Bundle type identity. Complete construction
validates the metadata and produces the Bundle value or field-wise wiring
shape selected by context. Scalar arguments in temporal construction are
lifted; construction expected as `atomic<S>` generates one aggregation node
that activates on any supplied temporal field and publishes only once all
required fields are valid.

A `delta<S>(...)` expression lowers to a distinct checked-HIR delta value for
the recursively expanded temporal `S`. Its field-presence bitmap means “this
field participates in this update” and is not the Bundle value-validity bitmap.
No defaults are evaluated. A structural child contains its own delta, while an
atomic child contains a complete canonical value. This HIR value is consumable
only by runtime output, test/replay input, or a temporary local and has no
ordinary source-schema descriptor.

Native TSB delta values already represent omitted fields, and generated code
must use their typed delta builders or per-field output mutations rather than
materialize unchanged values. Explicit `null` in an optional field is a clear
operation, not omission. The existing scalar-child typed-null delta denotes no
change, so clear lowering remains blocked until hgraph exposes a distinct
public mutation or canonical delta encoding that survives record/replay.

`rolling` is not erased. It establishes a TSW endpoint whose kind and
resolved maximum and minimum sizes participate in schema identity. The
compiler normalizes an omitted minimum to the maximum, checks that both sizes
are of one kind and in range before lowering, and rejects
`atomic<rolling<...>>` and `const` rolling values. A tick window lowers to
the static `TSW<T, max, min>` schema. A duration window has no static schema
today (`static_schema.h` records duration windows as registry-only and the
parity matrix records the missing compile-time marker), so generated code
interns it through `TypeRegistry::tsw_duration(value_type, time_range,
min_time_range)`, the schema Python's `TSW[T, timedelta]` produces, until
hgraph adds the marker.

Plain generic parameters bind HGL source-type descriptors. At operator and
candidate boundaries their temporal and constant occurrences lower to hgraph
type-pattern variables. The compiler intersects the domains imposed by every
occurrence; for example, a `const` occurrence excludes `atomic` and `rolling`.
`const` generics that shape a rolling type must bind through the same hgraph
resolution record rather than a compiler-only side table. Repeated variables
must unify, and the resolved candidate must contain no unbound type or size
required by its inputs or output.

The context-neutral source variable exposes a gap in the current native
resolution record. Hgraph stores time-series, scalar, and size variables in
separate namespaces, while one HGL `U` may occur in both a temporal position
and a `const` value position. The public bridge must preserve one canonical
source-type binding and derive each contextual representation from it. The
preferred core extension is a source-type binding kind integrated
with `ResolutionMap`; generating unrelated native variables and correlating
them in a compiler-private table is not an acceptable second resolution model.

The current public hgraph type pattern represents TSW sizes as either concrete
tick values (`TypePattern::tsw`, which matches no duration window) or one
wildcard over the complete window shape (`tsw_any`). It has no concrete
duration form and does not yet bind named maximum and minimum size variables
of either kind. Generic `rolling<T, max_size, min_size>` lowering, and exact
matching of a duration window at a candidate boundary, therefore require a
public TSW size-pattern extension integrated with `ResolutionMap`; the
compiler must not approximate this with private matching logic.

List sizes need no such extension. hgraph's `TSL` pattern already carries a
named `SIZE<"n">` variable that binds the argument's concrete size, a dynamic
list binds it to `0`, and a concrete `TSL<T, 0>` pattern matches every size.
The source sentinel `unbounded` lowers to `0`, and a `const` generic in a
list-size position lowers to the existing size variable.

## Generic constraint IR and lowering

The semantic pass normalizes every `requires` clause into a declarative
constraint IR. Its initial node kinds are:

```text
TypeEqual(lhs, rhs)
TypeSetMember(type, allowed-types)
TypeCategory(type, category)
TypeFunction(name, arguments)
ConstPredicate(expression)
OperatorRequirement(operator-id, inputs, optional-output)
And / Or / Not
```

`fields`, `has_fields`, and `field_type` are compiler-known type functions over
canonical structured types. They do not invoke user code. Operator requirements
carry the canonical nominal operator identity established during name
resolution, never only its short source spelling.

For an operator-bound function, conformance establishes a substitution from
the operator's generic parameters to the candidate signature. The compiler
applies that substitution to the operator constraint and conjoins it with the
candidate's own constraint. The combined expression is used both to check the
implementation body and to generate its dispatch metadata and hooks.

Candidate resolution proceeds in one deterministic sequence:

1. Match arguments, explicit generic arguments, and any expected output,
   recording the initial substitution.
2. Evaluate orientable equality constraints in positive conjunctive positions
   and type functions whose inputs are available, repeating to a fixed point.
3. Reject an unresolved dependency, inconsistent re-binding, or equality
   failure with a source constraint diagnostic.
4. Resolve every required input and output schema.
5. Evaluate the remaining Boolean and operator requirements as candidate
   admission predicates.
6. Let hgraph rank the admitted candidates and require one best match.

This source model maps to the existing native hooks rather than reproducing
them:

| Constraint form | Native representation |
| --- | --- |
| repeated `U` | one shared `ResolutionMap` binding |
| `U in {f64, i64}` | constrained `TypePattern` variable |
| `V == field_type(U, name)` | generated `resolve_default_types` logic |
| `U is struct` and `has_fields(...)` | structural pattern where representable, otherwise generated `requires_` |
| `op(U, U) -> U` | viability query through the selected nominal hgraph operator |
| residual `const` predicate | context-aware generated `requires_` |

Constraints represented in `TypePattern` participate in hgraph specificity
ranking. A residual `requires_` predicate only rejects a candidate; it does not
make that candidate more specific. Consequently two same-ranked candidates
whose predicates both accept remain ambiguous. The compiler must not use
source order, import order, registration order, or an attempted general proof
of predicate implication as a tie-break.

The current TSB pattern is closed: its field names and count must exactly match
the concrete schema. `has_fields(U, {"a", "b"})` can initially lower to
`requires_`, but that cannot bind the field types or rank the candidate above
an unconstrained fallback. First-class structural generic overloads therefore
need a public open-struct pattern that records required fields, binds their
types, accepts additional fields, and contributes deterministic specificity.
That extension belongs in hgraph's shared `TypePattern` machinery, not in an
HGL-only matcher.

### Erased and specialized implementations

Generic substitution is complete at wiring time even when the generated
runtime implementation is type-erased. The checked HIR records which operations
the constraints make available to the body. Code generation may then choose:

- one erased implementation using public hgraph value, delta, and structural
  views when every body operation is valid for the complete admitted domain;
- specialization for a finite domain or a representation-specific operation;
- ordinary graph composition whose nested operator calls resolve at wiring
  time.

This choice must not introduce per-tick overload resolution or dynamic typing.
An actual existential or boxed `any` type, if ever required, is a separate
language feature from `<U>`.

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

A temporal literal is normalized in the AST and lowers to the matching hgraph
scalar constant: a `date` to `Date`, a `time` to `Time` microseconds since
midnight, a `datetime` to `DateTime` microseconds since the epoch after UTC
normalization, a `duration` to `TimeDelta` microseconds, a `civil_datetime`
to `CivilDateTime` local microseconds, a `timezone` to a `ZoneId` interned
from the validated name, a `zoned_datetime` to `ZonedDateTime` through
`from_resolved(instant, zone, offset)` so that the run's provider applies the
strict offset check, and a `zoned_time` to `ZonedTime` once hgraph adds it.
In a
temporal position it lifts like any other scalar literal. Temporal arithmetic
and comparison lower to the standard `add_`, `sub_`, `mul_`, `div_`, `neg_`,
and comparison operators using exactly the overloads hgraph registers, so the
language's operation table is hgraph's; in a runtime body the generated code
calls the same checked helpers on canonical values, and zoned arithmetic
reaches the provider through `GlobalState` exactly as the standard operators
do. The hgraph-side asks recorded in the roadmap are the ordering overloads
for `Time` and `CivilDateTime`, which `register_ordered_same_scalar_comparisons`
does not cover today, and the `ZonedTime` scalar with its `date + zoned_time`
and policy-taking `resolve` operators.

An atomic tuple parameter lowers to one atomic endpoint. Indexing it must wire
an imported or generated extraction operation rather than read a current tuple
during composition. A structural tuple parameter lowers to an un-named bundle;
indexing it with a literal is projection of field `_<index>` (a wired
projection in composition, a field view at runtime), and a non-literal index is
a type diagnostic.

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
`impl fn` lowers to an explicitly registered graph or node candidate according
to its classified body. An ordinary `fn` lowers as an exact callable and is not
placed in a registry.
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

## Direct-wiring backend

Status: proposed (2026-09-03) with the test harness and run model in
[Syntax and semantics](syntax-and-semantics.md#tests-and-the-evaluation-harness).

The direct-wiring backend executes a composition-only program without
generating C++. It is not an interpreter of hgraph behaviour: it walks the
semantic IR of a composition function and asks hgraph to wire each operation
through the erased entry that the Python bridge already uses,

```cpp
OperatorWireResult wire_operator(
    Wiring &w, std::string_view name, std::span<const WiringArg> args,
    std::optional<bool> output_required,
    const TSValueTypeMetaData *expected_output);
```

so the registry lookup, resolver, and graph construction are hgraph's own
and identical to the generated `wire<Operator>(...)` call for the same source.
The backend owns exactly these steps:

- constant folding of `const` expressions and literals into `Value`s with
  their interned `ValueTypeMetaData`, which become `WiringArg::Kind::Scalar`
  arguments;
- the mapping from a resolved nominal operator identity to the registry name
  hgraph knows it by (the same mapping the C++ backend bakes into a marker);
- the argument order and names hgraph's resolver expects, including the
  `expected_output` schema when the language has already fixed the result
  type (a return annotation, a `replay` for a typed parameter);
- calling an exact `fn` by walking its body with the caller's ports bound to
  its parameters, so exact functions inline at wiring time exactly as the
  generated C++ would;
- the harness: an `eval` argument becomes a `replay` operator wired at the
  parameter's expanded schema, the callee result becomes a `record`
  operator, and the sequences seed and read the in-memory buffers through
  the public `hgraph/lib/testing/record_replay.h` helpers.

The harness uses the `"testing"` record/replay backend for dense sequences
(`dense_record`; index i is evaluation cycle `MIN_ST + i*MIN_TD`, which is
the alignment `eval` promises) and the sparse absolute-time entries of the
`"memory"` backend for timed sequences. A test run is one
`GraphExecutorBuilder` over the wired graph, evaluated in process; the
observed sequence is read back with `get_recorded_deltas` or
`get_recorded_sparse`, padded by the rule in the specification, and compared
with `Value::equals` element by element.

`hgl run` under this backend wires the entry function with its `[run.params]`
constants as scalar arguments, applies the mode, start, and end to the
executor builder, and prints each tick of the result port through a `record`
sink read after the run (simulation) or a streaming sink (real time).

The backend rejects, with a `backend` diagnostic that names the function, any
evaluated closure that contains a runtime function, a source-defined operator
whose selected candidate is a runtime function, or a `use` of a module whose
descriptor has no loaded native image. Those programs need the C++ backend.
It never emulates a node body.

The backend depends only on public hgraph headers: `operator_dispatch.h`,
`graph_wiring.h`, `executor.h`, `lib/testing/record_replay.h`, and the
`stdlib` in-memory record/replay implementations for backend selection.
Anything it needs beyond those is an hgraph-side ask recorded in the roadmap,
not a private include.

## Source mapping and generated artifacts

Every generated declaration and meaningful expression maps to its language
range through `#line` directives and/or a sidecar map. A native error in
generated implementation detail is a compiler defect and reports the retained
artifact path plus compiler, SDK, module, profile, and target versions.

Generated output and manifests must be deterministic. Absolute developer
paths, timestamps, random identifiers, and unordered iteration must not affect
them.

## Scripted, REPL, and AOT drivers

`hgl test`, `hgl run`, `hgl repl`, and `hgl build` use the same type
expansion, function classifier, and typed IR. `hgl build` always uses the
C++ backend; the other three use the direct-wiring backend when the evaluated
closure is composition-only and the C++ backend otherwise. The parity suite
runs every test the direct-wiring backend accepts through both backends and
compares the recorded ticks.

The initial REPL may materialize a synthetic module and rebuild the full
session. A failed declaration must not replace the last valid session.

Replacing a REPL module stops and destroys graphs holding its leases, removes
the old module handle and installer intent, initializes the replacement, and
rebuilds the registry from the active module set. If removal cannot complete,
the old revision remains active and the replacement fails atomically. Retaining
old native images is acceptable; retaining their candidates is not.

A future JIT must consume the same classified semantic IR and pass backend
parity before it can replace either backend.
