# Compiler and C++ lowering

Status: target pipeline with agreed structured-value lowering and provisional
generic, module, and runtime semantics

The normative pass boundaries, dependency rules, and migration away from the
resolved syntax tree are recorded in
[Compiler architecture](../design/compiler-architecture.md). This section
describes the current prototype and its lowering details.

All execution modes share one target pipeline:

```text
source
  -> tokens and syntax AST
  -> package target and locked module closure
  -> module descriptors and candidate universe
  -> modules, nominal names, and canonical value types
  -> generic struct specialization, hierarchy, and effective fields
  -> generic call binding and operator conformance
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

`src/syntax/` is currently implemented as follows. `source` holds a file's
path, text, and line table; every token and node carries a half-open byte range
into it. `diagnostic` collects `Category`-tagged diagnostics with optional notes
and renders them as `path:line:col: category: message` plus the source line
and a caret. `temporal` parses and validates the temporal literal spellings
of the syntax guide into a `TemporalValue` (kind plus microseconds, offset,
and zone) and prints the canonical spelling. `lexer` produces one token
vector per file, with comments as trivia and one `Newline` token per run of
terminators. It also records non-overlapping source fragments for every token,
whitespace run, physical line break, and line comment; those fragments exactly
reconstruct the input even where several line breaks share one grammar token.
`syntax_tree` owns the parser-independent source arena. Its production nodes
and source tokens retain ranges, its lexical fragments retain all trivia, and
its issue nodes distinguish zero-width missing tokens from unexpected source
tokens. `ast` is an index-based semantic syntax arena: nodes are `std::variant`
payloads addressed by `NodeId`, so the tree owns no pointers and a module is
one movable value. `token_grammar` is the private lexy production grammar
selected by ADR 0001. It parses the lexer's token stream, materializes the
source arena, and discards all lexy storage before returning. Every clean
legacy-parser test and every checked-in HGL example must pass this declarative
grammar. Focused malformed cases prove local recovery after three independent
missing tokens and complete source retention after a fatal error. `parser`
still produces `ast::Module` directly during this intermediate slice; it is
the hand-written recursive-descent implementation that applies the newline
rules. `ast_printer` dumps that arena one node per line for
`hgl check --dump-ast` and the tests.

This dual-parser state is deliberately temporary. The next parser slice
projects the source arena into the existing `ast::Module` and makes that the
compiler path. Only after equivalence tests pass may the hand-written syntax
decisions be removed. Downstream compiler passes never receive lexy types.

The first pass of `src/semantics/` is `resolve`. It binds every value, type,
and constraint-name occurrence of one compilation unit by the lookup rules of
the syntax guide ("Scopes and name lookup"), checks `use` declarations against
the interim kernel table (below, "Interim kernel table"), resolves nominal
struct hierarchies and effective fields, validates construction and closed
generic-struct requirements, classifies every function by the rule of
"Function classification", and applies the phase rules of `test` bodies. Its
result, `ResolvedModule`, annotates the syntax tree with expression and type
bindings, constraint identities, struct metadata, and function kinds instead
of building the typed HIR. Hgraph's resolver still types every operator the
direct-wiring backend wires, so the HIR becomes necessary when callable
substitution or the C++ backend needs canonical types ahead of hgraph. Until
then `src/wiring/` walks the resolved syntax tree directly. This is explicitly
temporary: typed HIR followed by hgraph semantic IR will become the only input
to both backends.

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

## Interim kernel table

Status: first pass (2026-09-03), until module descriptors exist.

Kernel descriptors (below, "Module descriptors and build manifests") will
say which names a kernel module exports and the registry name behind each.
Until they exist the compiler carries one table:

| Module | Short name `x` resolves to | Checked |
| --- | --- | --- |
| `hgraph.std` | the registry operator `x`, else `x_` (`map` -> `map_`) | at `hgl check`, against the process registry |
| `hgraph.analytics` | the registry operator `hgraph.analytics.x` | when wired: the analytics image loads only in a tool that links it |

A `use` of any other module is a `module` diagnostic in the first pass:
programs are one compilation unit, and source modules beyond the unit
arrive with descriptors. A selective import whose name the kernel does not
export is `module: hgraph.std does not export 'x'`. The prelude intrinsics
of the syntax guide ("Scopes and name lookup", step 5) are not in the table:
the resolver binds them last, so any declaration may shadow them, and the
backend gives them their composition-phase meaning (`valid` and `modified`
wire the standard operators of the same name, folding several arguments with
`and_` and `or_`, and `all_valid` is `valid` folded with `and_`;
`last_modified` wires `last_modified_time`; `key_set` wires `keys_`; the
traversal intrinsics `keys`, `values`, `items`, `added`, `removed`, and
`delta` are runtime-only and a `backend` diagnostic in a composition body
of the first pass).

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

Struct symbols carry parent identities, abstract/final kind, ordered field
descriptors, inherited-default overrides, and source ranges. Each effective
field descriptor keeps its declaration origin, canonical source type,
invariant optionality, and effective construction default. Constructor
metadata does not alter native Bundle type identity. Complete construction
validates the effective metadata and produces the Bundle value or field-wise
wiring shape selected by context. Scalar arguments in temporal construction
are lifted; construction expected as `atomic<S>` generates one aggregation
node that activates on any supplied temporal field and publishes only once all
non-optional fields are valid.

Hierarchy resolution runs before temporal expansion and generic reflection. It
rejects cycles, concrete parents, abstract construction, inherited type or
optionality changes, a typed redeclaration of an inherited field, invalid
default overrides, and unresolved multiple-parent default conflicts. A
default override updates constructor metadata only. It may add or replace a
default but cannot remove one; `null` remains legal only for a field whose
introducing declaration made it optional. `fields`, `has_fields`, and
`field_type` operate on the validated effective field set.

Every concrete struct lowers as a final nominal Bundle containing its inherited
and locally declared fields, with abstract-parent relationships registered in
hgraph's type hierarchy. Abstract structs have no constructible value of their
own. A scalar or atomic abstract position lowers through hgraph's closed
polymorphic value plan and retains the concrete leaf discriminator. Its leaf
set comes from the complete linked module closure and is captured in the graph
type-realization snapshot. A temporal abstract position lowers only the fixed
field-wise base TSB. The compiler must not insert an implicit derived-to-base
temporal projection; the source language will expose that graph operation
explicitly once its spelling is defined.

The compiler must assign one deterministic effective field order before schema
interning, descriptor fingerprinting, and code generation. The exact
multiple-parent linearization remains an open language rule, so hierarchy
lowering must not ship until that rule is fixed and tested.

### Generic struct specialization

A generic struct symbol owns an ordered list of type and `const` parameters,
its normalized constraint IR, parent type expressions, fields, and default
overrides. The generic origin has nominal identity but no constructible Bundle
schema. A fully resolved application creates a specialization key containing
the origin plus every canonical type or typed constant argument. The key is
invariant and is interned before hierarchy resolution and temporal expansion.

Struct type parameters initially have the canonical-value domain. The checked
IR still stores their HGL source-type descriptors rather than erasing them to
native value metadata; semantic checking rejects `atomic` and `rolling`
arguments for this language edition. Constant parameters store their declared
value type and normalized value. A constant participates in identity even when
it does not affect a field schema.

An explicitly applied type or constructor must provide the complete ordered
argument list. Otherwise constructor inference:

1. matches an expected result type against the generic origin;
2. matches supplied named arguments against the unsubstituted field types;
3. repeatedly unifies type and constant bindings from every occurrence;
4. evaluates orientable equality constraints to a fixed point;
5. rejects unresolved or conflicting parameters; and
6. evaluates the remaining struct requirements before interning the
   specialization.

Omitted fields and their defaults do not provide inference evidence. In
particular, `Maybe()` cannot invent the argument for `Maybe<T>` merely because
its only field defaults to `null`. Generic parameter defaults and partial
applications do not enter this algorithm.

After substitution, ordinary hierarchy rules apply. A generic child may map
its parameters into a fully applied abstract parent or fix the parent
arguments. Each concrete application is final, inherited fields are checked
after substitution, and a closed abstract family contains only descendants of
that exact parent specialization. `Event<Quote>` and `Event<Trade>` never
share alternatives merely because they have the same generic origin.

The existing hgraph Bundle registry already supports invariant type-only
specializations through generic-argument metadata, and the scalar pattern
matcher can bind arguments for a named generic Bundle origin. HGL should use
that public path for `Box<T>` and derive temporal matching from the matching
Bundle value schema rather than build a second TSB-specific generic system.

The native metadata currently records only type arguments. A source type such
as `Vector<T, const size: i64>` requires the public nominal Bundle metadata,
manifest fingerprint, and generic Bundle pattern to carry typed constant
arguments as well. Encoding a constant only into a generated local name or
recovering it from the field layout is not sufficient: two specializations
must remain distinct even when the parameter is unused by their fields. The
extension must bind constants through hgraph's shared `ResolutionMap` and
participate in equality, hashing, diagnostics, and descriptor round trips.

Module descriptors publish the generic origin, parameter kinds, constraints,
and source-level field and parent expressions. The application compiler
collects the finite set of fully applied specializations used by the target and
registers those schemas before graph wiring. An invalid or unresolved
application never creates a registry entry. Generic erasure or per-type C++
specialization remains a backend choice for functions consuming the type; it
does not introduce an open `Box<any>` runtime value.

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

For a generic struct, the same IR validates the family declaration and each
complete application. It admits or rejects a nominal specialization and does
not participate in overload ranking. Operator requirements are viability
queries against the target's selected nominal operator universe; the compiler
does not cache a specialization that fails one.

Candidate resolution proceeds in one deterministic sequence:

1. Match call arguments and any expected output, recording the initial
   substitution. Explicit generic arguments are handled only by the separate
   struct-application algorithm in the initial design.
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
record/replay restoration is preserved. They may read scalar `const`
parameters, which are included in the generated lifecycle signature. Explicit
source `start` and `stop` blocks become the corresponding static hooks and may
likewise read state and `const` parameters, but not temporal inputs or output.
All state variables share one typed state schema. A future ephemeral-cache form
must lower separately and must not cause one node to mix incompatible state
selectors.

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
algorithm. Generated C++ dispatches through the public contract alias again so
descriptor or registry drift becomes an error.

A source-defined operator lowers to a deterministic alias of the corresponding
`hgraph::Operator` contract. Each `impl fn` lowers to an explicitly registered
graph or node candidate according to its classified body. An ordinary `fn`
lowers as an exact callable and is not placed in a registry.
Only an ordinary `export fn` is emitted into the module's public exact-function
surface.

The generated contract alias or descriptor mapping must preserve the full nominal
identity rather than using an unqualified registry string that could collide
with another module.

## Module descriptors and build manifests

A descriptor separates its importable interface from its provider inventory.
The interface contains automatically public nominal operators, explicitly
exported exact functions, and exported struct declarations and hierarchy
relationships, including generic origins and their normalized constraints. The
provider inventory contains every
candidate-to-operator binding, including the provider module identity and
implementation metadata, plus:

- canonical module and compatibility versions;
- canonical types, effective constructor metadata, abstract/concrete hierarchy
  contributions, generic specialization keys, and their hgraph schemas;
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

The public hgraph `OperatorRegistry` now returns an opaque provider handle from
keyed installer registration, records candidate provenance while the installer
runs, removes that provider's candidates and installer intent, rolls back a
throwing installer's candidates, and carries provider leases through wired graph
plans and runtime graphs. This is the operator-registry foundation, not yet the
complete HGL module ABI: hgraph still needs one transaction/handle coordinating
type associations, exact-function metadata, native resources, and operator
registration. Generated language code must not reach into registry storage or
coordinate those registries privately.

Indirect resolution follows the same rule as direct operator wiring. In
particular, selecting a lifted kernel for a reduce or map node passes the active
`Wiring` to the registry before the kernel pointer is stored in the node plan.
A schema-only resolution probe may omit the wiring only when no provider-owned
callback or metadata pointer escapes the probe.

## Direct-wiring backend

Status: executable prototype (2026-09-03) with the test harness and run model in
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

The backend never emulates a node body. A runtime function or source-defined
operator is wired by its module-qualified registry name, so the driver must
first load the generated native image which supplies that candidate. Calling
the backend API directly without such an image produces an operator-resolution
diagnostic. Imported modules whose descriptors have no loaded image remain an
error.

The backend depends only on public hgraph headers: `operator_dispatch.h`,
`graph_wiring.h`, `executor.h`, `lib/testing/record_replay.h`, and the
`stdlib` in-memory record/replay implementations for backend selection.
Anything it needs beyond those is an hgraph-side ask recorded in the roadmap,
not a private include.

### First pass

Implemented (2026-09-03) in `src/wiring/` as `backend`, over the resolved
syntax tree of `src/semantics/`. A `Session` bootstraps hgraph once per
process (`register_standard_types`, `register_standard_operators`, and the
backend's own installer, below). The walk assigns every expression one of the
following wiring-time values: a *constant* (`Value` plus its interned
`ValueTypeMetaData`), `null`, a sparse structured delta, a *port*
(`WiringPortRef`), a nominal struct, a *function* (a module `fn` or a registry
operator), or, inside a `test` body, a *harness sequence*. The rules of the
walk:

- literals are constants: `i64`, `f64`, `bool`, `str`, `datetime`,
  `duration`, `date`, and `time` map to hgraph's `Int`, `Float`, `Bool`,
  `Str`, `DateTime`, `TimeDelta`, `Date`, and `Time`; the zoned and civil
  literals are `backend` diagnostics until the zoned scalar wiring lands;
- a unary or binary operator over constants folds in the compiler
  (arithmetic on `i64` and `f64`, comparison on both plus `str`, `&&`,
  `||`, `!`), a tuple literal of constants is a constant tuple, and a
  sequence literal of constants outside a test is a constant list; any
  operand that is a port wires the corresponding `hgraph.std` operator
  (`add_`, `sub_`, `mul_`, `div_`, `mod_`, `eq_`, `ne_`, `lt_`, `le_`,
  `gt_`, `ge_`, `and_`, `or_`, `not_`, `neg_`) with the constant side as a
  scalar argument, which hgraph's resolver lifts;
- `a[i]` wires `getitem_` and `a.b` wires `getattr_` when `a` is a port,
  and `a[i]` folds when `a` is a constant tuple or list;
- a non-generic or explicitly type-applied struct constructor builds its
  module-qualified native Bundle value when every supplied field is scalar;
  defaults are evaluated only for a complete value, optional `null` fields
  remain unset, field access folds, and `delta<S>` instead builds a sparse
  Bundle without defaults. A constructor containing ports produces a public
  structural `WiringPortRef` at the recursively expanded named TSB schema,
  lifting scalar fields and filling absent optional fields with typed null
  sources. Type-only generic applications use hgraph's generic Bundle metadata;
  constructor inference and typed `const` generic arguments are rejected
  explicitly rather than encoded into an unstable name;
- calling a registry operator builds `WiringArg`s in the source order with
  the source names and calls `wire_operator` with no expected output;
  `OperatorResolutionError` is reported as an `operator` diagnostic carrying
  hgraph's message;
- calling a module `fn` binds the arguments to its parameters (positional
  then named, then defaults, evaluated in the callee's frame after its
  `const` parameters), wires `const` at the parameter's declared schema for
  a constant passed to a temporal parameter, requires a port to carry
  exactly that schema, and walks the body with those bindings; a constant
  passed to a `const` parameter converts to the declared value type (`i64`
  to `f64`, elementwise through tuples and lists; anything else is a `type`
  diagnostic), and a sequence literal passed to a `const list<T>` parameter
  inside a test is that list; a runtime function, an `impl fn`, or a
  generic function is a `backend` diagnostic naming it;
- the prelude intrinsics take the meaning of "Interim kernel table";
- `if` selects a branch when its condition is a constant `bool`; a port
  condition is a `backend` diagnostic until the guide decides whether it
  lowers to `if_then_else`; the branch value is the arm's tail expression;
- a block body runs its statements in order: `let` and `var` bind locals
  (a declared type converts a constant or checks a port's schema), `=` and
  the compound assignments rebind a `var`, `return` ends the activation,
  and the tail expression is the value; the runtime statement forms are a
  `backend` diagnostic;
- `eval` takes a module function (an operator must be wrapped in a `fn`,
  so the harness always has declared parameter types) whose temporal
  parameters lower to `ts` schemas: atomic tuples, atomic structs, scalars, and
  the temporal scalars run; structural tuples, structs, lists, sets, maps, and
  windows are a `backend` diagnostic until the harness drives those kinds. It runs
  dense sequences only (a keyed element is a `test` diagnostic): each
  temporal argument becomes a `replay` at the parameter's schema under the
  key `hgl::in::<name>`, an omitted parameter with a default replays its
  default once, the callee result a `record` under `hgl::out` (a constant
  result is wired as `const` first), both selected by the `"testing"`
  backend; one simulation executor runs from `MIN_ST`; the result is read
  back with `get_recorded_deltas` and padded with `_` to the longest
  input; `==` and `!=` between a harness result and a literal sequence
  give the literal the result's delta schema and compare as the syntax
  guide specifies, remembering the first differing cycle; two literal
  sequences cannot be compared (nothing fixes their type).

A failing `assert` in `hgl test` prints `assert failed: <source text>` and,
after a sequence comparison, one detail line: `cycle i: expected x,
observed y in [...]` or `expected n cycles, observed m: [...]`. Numbers,
tuples, and lists print in source spelling (an `f64` keeps its decimal
point), other values in hgraph's `Value::to_string` form, and every schema
in a diagnostic is hgraph's name (`float`, `Tuple[float,float]`,
`TS[float]`) until the language has its own type printer. Each test runs
in its own frame; a diagnostic inside a test fails that test (`diagnostics
reported`) and is rendered after the summary. The driver prints
`<name> ... ok|FAILED`, the message lines indented, and `<n> tests, <m>
failed`; the exit status is non-zero on any failure or diagnostic.

`hgl run` picks the entry (`--entry <name>`, else the one `export fn`
whose parameters are all `const`; none or several is a `backend`
diagnostic), binds every parameter from `--set name=<constant expression>`
(the text is parsed and folded as the body of a `fn` in a scratch unit
`module hgl.cli`, then converted to the parameter's value type; an unknown
name is a `name` diagnostic) or its default (a parameter with neither is a
`type` diagnostic), walks the body, sends the result port to the backend's
own sink operator `hgl.print_tick` (an `In<TsVar>` node that writes
`time value` per tick, the time in the canonical `datetime` spelling
without its `@`; the one node the language tool registers, through the
registry installer `hgl.wiring` so a registry rebuild keeps it), and runs
the executor in the selected mode: `--mode sim` (default) from `--start`
or `MIN_ST`, `--mode realtime` from `--start` or the wall clock, until
`--end` as a datetime, or as a duration after the start, or `MAX_ET`.
Registering that sink is not node emulation: it is the tool's output
device. The TOML run configuration is not in the first pass.

## C++ backend, first pass

Status: implemented for the composition and runtime forms exercised by every
checked-in example as of 2026-09-05. This includes nominal and generic structs,
generic operator implementations, fixed and duration windows, sparse struct
deltas, concise `map` functions, scalar and collection runtime inputs, borrowed
collection traversal, `out`, `logger`, state, and lifecycle hooks. File-based
`test` and `run` compile/load supported runtime modules on Unix; portable native
loading and the remaining language-depth items are still staged.

`hgl emit-cpp <file.hgl>` writes one header/source pair named after the
source — `prices.hgl` becomes `prices.h` and `prices.cpp` — beside the
source by default, into one directory with `--out-dir`, or split with
`--include-dir` and `--src-dir`; `--print` writes both to stdout for tooling
and tests. The namespace is the module name: `module examples.prices` emits
`namespace examples::prices`, and a C++ keyword or a name the generated code
reserves (`w`, `operators`, `compose`, ...) gets a trailing underscore. Before
the driver exposes the pair, it runs both files through `clang-format` with the
repository's fixed generated-code style. That policy is embedded in the
compiler, so invoking an installed `hgl` from a project with a different
`.clang-format` file does not change its output. Formatting failure is a
compilation diagnostic, not a best-effort warning; `HGL_CLANG_FORMAT`
overrides the executable selected when `hgl` was built.

What is emitted, in this order:

- **Operator contracts.** `namespace operators` holds one transparent alias to
  `hgraph::Operator<"module.name",
  In<...>..., Scalar<...>..., Out<...>>` per source `operator` and per
  `export fn`, so every public callable has a registry identity
  (`examples.prices.smooth`) and a typed contract
  (`examples::prices::operators::smooth`). Generated code never subclasses an
  operator merely to give the contract a C++ name.
- **Graph implementation structs.** Every composition function has a plain
  implementation struct with `static constexpr auto
  name` and a `compose(hgraph::Wiring &w, ...)`: `hgraph::Port<S>` for a
  temporal parameter, `hgraph::Scalar<"n", T>` for a `const` one, returning
  `hgraph::Port<S>` for the declared result. Exported functions are declared
  in the header and defined out of line in the source; module-internal
  functions and `impl fn` candidates are whole structs in an anonymous
  namespace of the source, in dependency order (a recursive helper is a
  diagnostic). `const` parameter defaults become `static auto defaults()`
  so the registry applies them when the function is called by name.
- **Structural types.** An exported source struct becomes a readable C++
  declaration with `value_type` and `time_series` aliases. `NominalBundle`
  preserves module-qualified identity, abstract parents, and concrete generic
  arguments; `NominalTSB` preserves the recursively temporalized fields.
  Constructors lower to `to_tsb`, an `atomic<S>` result aggregates that TSB
  through `combine_cs`, and a runtime `delta<S>` builds and applies only its
  supplied fields.
- **Generic and window types.** Source type parameters become hgraph
  `ScalarVar` patterns at operator boundaries and ordinary C++ template
  parameters for structural declarations. A generic rolling parameter becomes
  `TSWAny<T>` while a concrete tick or duration window becomes `TSW<T, N, M>`
  or `TSWDuration<T, period_us, minimum_us>`. The selected call's concrete
  window schema is retained when a graph implementation receives `TSWAny`.
- **Runtime-node structs.** A runtime function in the supported scalar subset
  is an empty static node struct in the generated header. Its `eval` signature
  carries typed `In`, `Scalar`, `RecordableState`, and `Out` selectors. The
  union of `modified(...)` parameters selects active inputs; other temporal
  inputs are passive. A function with `when` conservatively admits unchecked
  inputs and retains its complete ordered predicates in `eval`. A function
  without `when` uses ordinary active/valid input policy.
- **Bodies.** The same lowering the direct-wiring backend performs, printed:
  a constant expression folds into a C++ expression with the same rules
  (`/` on integers is a `Float` division, `Int` and `Float` mix to `Float`,
  strings concatenate, durations and datetimes add and subtract); known
  numeric values are retained far enough to reject zero divisors and invalid
  compile-time rolling sizes before C++ is written; a
  time-series expression is `hgraph::wire<marker>(w, args...)` — the
  standard operator for each infix form (`add_`, `lt_`, `and_`, ...),
  `getitem_` / `getattr_` for indexing and fields, `valid` / `modified` /
  `all_valid` / `last_modified_time` / `keys_` for the intrinsics, the
  kernel table's marker for an imported operator (`hgraph::stdlib::x`,
  `hgraph::analytics::x`) and `hgraph::arg<"n">(...)` for a named argument;
  a call to a module function is `hgraph::wire<name>(w, ...)` with defaults
  folded in; a constant at a temporal position is
  `hgraph::wire<hgraph::stdlib::const_, S>(w, value)`; a port whose schema
  the registry decides is narrowed with `.as<S>()` at a typed boundary, which
  the wiring checks. `let` / `var` are `const auto` / `auto` locals, but a
  `var` keeps the static type fixed by its annotation or initializer and every
  rebind is converted or checked at that boundary. `if` over a constant is a
  C++ `if`; `return` and the tail expression return the result port.
- **Runtime bodies.** Scalar payload expressions use the same checked type and
  widening rules, while `modified`, `valid`, and `all_valid` call selector
  metadata directly. `valid(a, b)` is an `&&` fold and `modified(a, b)` is
  an `||` fold. Ordered `when` blocks become independent `if` statements.
  `return value` sets the output and returns; assignment through `inject out`
  sets it and continues, so the final whole-output write wins. Scalar state
  fields form one named `TSB` behind `RecordableState`; `start` seeds only
  invalid fields before running an explicit state-and-configuration start
  block. `inject logger` lowers `logger.info(message)` to `LoggerView::log`.
  Runtime `map`, `set`, and `list` parameters retain their typed selectors;
  `keys`, `values`, and `items` become ordinary C++ range loops over current,
  `modified`, `added`, or `removed` views. A concise iterator predicate is
  inlined as a readable loop guard. Keyed `out[key] = value` uses the typed TSD
  output selector and accumulates child writes in the cycle's delta.
- **Registration.** `hgraph::OperatorProviderHandle register_operators()`
  registers each export and
  each `impl fn` with
  `hgraph::register_graph_overload<operators::x, x>()` for a composition or
  `hgraph::register_overload<operators::x, x>()` for a runtime node. Private
  runtime helpers get readable aliases in a translation-unit-local
  `operator_contracts` namespace under the same module-qualified identities,
  so direct wiring can compose them without exposing them in the module
  header. Registration creates a keyed provider
  installer named after the module, activates exactly that provider, and
  returns its opaque handle. Activation can nest under an aggregate library
  installer while preserving each provider's provenance. A scoped rollback
  removes a provider whose activation fails without masking the original
  exception; registry reset replays active installers without resurrecting a
  removed one.
- **Python.** With `--python <file> --python-native <module>` the wrapper
  module imports the native module (which registers) and binds each export
  to `hgraph.operator_function("module.name")`. Python keywords gain a
  trailing underscore on this surface without changing the registry name;
  mapping collisions and invalid native-module identifiers are diagnostics.

The header includes the standard operator umbrella, the analytics header
when the module imports from `hgraph.analytics`, and the wiring/dispatch
headers; the source includes the header plus the scope-guard utility used by
registration rollback. Every emitted function is preceded by a `// file:line`
comment; output is deterministic (basenames, no timestamps).

The first pass still fails closed, before writing either file, on: generated
runtime sources, runtime calls, non-scalar state, output kinds other than the
implemented scalar, nominal-struct, and map forms, injectables other than
`out` and `logger`, lifecycle access to temporal inputs or output, optional
field clearing in a sparse delta, generic constructor inference and typed
`const` generic struct metadata, tuple and list literals and other compound
constants, `if` or a block used as a value, zoned and civil temporal literals,
an `impl fn` of an imported operator, and a missing module declaration. Each is
a diagnostic naming the construct.

`hgl_add_module()` (`cmake/HglLanguage.cmake`, installed with `hgl`) runs
`emit-cpp` as an `add_custom_command` per `.hgl` source, compiles the pairs
with any `SOURCES` into one library that links `hgraph::core` and the
`LINK_LIBRARIES` the module needs (`hgraph::analytics` when it imports
from it), publishes the header directory, and with `PYTHON_MODULE`
configures `cmake/hgl_python_module.cpp.in` into a nanobind module that
calls every module's `register_operators()` on import and collects the
generated wrappers into a package directory. The native module output path is
expressed with a generator expression so multi-configuration builds do not add
a configuration child directory, and an installed compiler executable is a
file dependency of every generated output. The repository's codegen tests
build every file under `language/examples`, plus the parity and runtime
fixtures, through exactly this function under the repository warning policy.

## Source mapping and generated artifacts

Every generated declaration and meaningful expression maps to its language
range through `#line` directives and/or a sidecar map; the first pass writes
a `// file:line` comment before each function instead, which the native
compiler's diagnostics do not consume. A native error in generated
implementation detail is a compiler defect and reports the retained artifact
path plus compiler, SDK, module, profile, and target versions.

Generated output and manifests must be deterministic. Absolute developer
paths, timestamps, random identifiers, and unordered iteration must not affect
them.

## Scripted, REPL, and AOT drivers

`hgl test`, `hgl run`, `hgl repl`, and `hgl emit-cpp` use the same type
expansion, function classifier, and checked tree. `emit-cpp` always uses the
C++ backend. File-based `test` and `run` keep the direct-wiring path for a
composition-only unit; when a unit contains a runtime function or `impl fn`,
the driver emits the whole unit, invokes the configured C++ compiler, loads
the resulting image, invokes its fixed registration entry point, and then
wires tests or the entry through the same backend and hgraph registry. There
is no `hgl build`: a package builds emitted C++ with `hgl_add_module()`.

The scripted compiler configuration is generated from the `hgl` CMake target:
compiler, preprocessor definitions, and evaluated include paths. It does not
retain the build machine's compiler launcher. An installed executable also
resolves the SDK include directory relative to its configured
`CMAKE_INSTALL_BINDIR`/`CMAKE_INSTALL_INCLUDEDIR` layout rather than assuming
the default `bin` and `include` names.
`HGL_CXX` overrides the compiler for diagnostics/testing,
`HGL_CLANG_FORMAT` overrides the required generated-code formatter, and
`HGL_ARTIFACT_DIR` selects the transient and failed-build root. The executable
exports hgraph symbols and the generated image does not link a second static
hgraph, so both use one registry. This path is currently Unix-only.

The native cache is format-versioned under a platform per-user cache directory,
or `HGL_CACHE_DIR/v2` when overridden. Its SHA-256 key covers the emitted
header, source and registration bootstrap; the registration ABI; the resolved
compiler executable path and digest, reported version and target, and effective
arguments; CMake system, processor and configuration; hgraph version/commit;
relevant compiler search environment; and the hosting `hgl` executable path
and digest. The executable digests prevent an image built by a changed tool or
against one host executable's exported symbols from being reused even when its
path and reported version remain unchanged. External module descriptor
fingerprints must join the key when scripted imports of separately built
providers land.

A miss compiles in the ordinary artifact directory, copies the generated
sources, image and diagnostic manifest into a unique staging directory beside
the cache, writes a completion marker containing the image digest, then
atomically renames that complete directory to its digest. Concurrent publishers
may both compile but converge on the first complete entry; partial directories
are never visible at the final key. A reader verifies the image digest before
loading. A damaged final entry is renamed with an `.incomplete-` prefix before
replacement, preserving it for diagnosis. Failure to create or publish the
cache falls back to the transient image; `HGL_CACHE_TRACE=1` reports that path.
Caching is likewise skipped when either executable identity cannot be resolved
and hashed, or when no per-user cache location is available. There is no shared
temporary-directory cache fallback. `HGL_DISABLE_CACHE=1` requests the
transient path explicitly. Cache eviction is not automated in this prototype.

Loaded images remain resident for the process because registry callbacks may
still point into them. Logical provider removal controls visibility; physical
unloading is deliberately deferred. Successful transient build directories are
removed; a compile failure retains its complete directory and reports the path.

The REPL uses direct wiring for composition-only sessions and the same cached
native loader for a session containing runtime functions or implementations.
The parity suite runs every composition test accepted by both backends and
compares the recorded ticks; the runtime fixture executes both as an
ahead-of-time module and through the scripted loader.

The initial REPL may materialize a synthetic module and rebuild the full
session. A failed declaration must not replace the last valid session.

The first-pass `hgl repl` does exactly that. The session is `module repl`
plus the accepted declarations in order; every input is appended to a copy
of that text, parsed, resolved, and, when it is a declaration (its first
word is `fn`, `export`, `impl`, `operator`, `use`, or `test`), kept only
if the whole session still checks; an accepted `test` also runs at once.
Any other input is a statement or expression evaluated in a synthetic
`test __repl` body over the session, so `let`, `assert`, and `eval` work at
the prompt; a `let` or `var` that evaluates is retained and prefixed to
every later body, so bindings persist; the value of a final expression
prints as a constant, the schema name of a port, or a harness sequence.
Input continues onto following lines while brackets are open (the prompt
becomes `...> `); `:quit` leaves, `:list` shows the session and the
retained bindings, `:help` the commands. Diagnostics locate into the
session text as `<repl>:line:col`. A declaration whose complete session needs
the C++ backend is emitted and staged before it is accepted. Failure keeps both
the last valid session text and its active native provider.

Input comes through `driver/line_reader`: on a terminal, with the tool
built with `HGL_ENABLE_LINE_EDITING` (default on), it is an isocline line
editor — cursor movement, up/down history persisted in `$HGL_HISTORY` or
`~/.hgl_history`, and tab completion over the `:` commands, the declaration
keywords, the kernel module names, and every name the session has declared
or bound; the prompt carries its own marker and bracket continuation stays
the REPL's loop. Off a terminal (a pipe, a file), or with the option off or
`HGL_NO_LINE_EDITING` set, it is a plain `std::getline` with the prompt
printed, so `tests/repl/repl_smoke.cmake` and scripted use see the same
text they always did.

Replacement happens between evaluations, after temporary graphs and plans have
released their leases. The driver compiles and loads a candidate image first,
removes the old provider handle and installer intent, and activates the new
provider. If activation throws, it reactivates the old image and reports the
failure. Old native images remain mapped; their candidates do not remain
selectable or return after reset.

A future JIT must consume the same classified semantic IR and pass backend
parity before it can replace either backend.
