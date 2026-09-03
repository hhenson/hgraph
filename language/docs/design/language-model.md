# Language model

Status: agreed syntax and structured-value foundation; provisional generic,
module, and runtime semantics

The [User Guide](../user-guide/README.md) is authoritative for observable
source behavior. The
[Developer Guide](../developer-guide/syntax-and-semantics.md) records the
proposed grammar and compiler boundaries.

## Design principles

The language should feel familiar to Python, Rust, and Swift users without
copying any one language. Ordinary functions, calls, braces, expressions,
named arguments, and canonical collection types carry most of the syntax.

The bespoke behavior is semantic:

- ordinary parameters are temporal;
- `const` parameters are fixed wiring-time values;
- `let` and `var` distinguish immutable and mutable lexical bindings;
- canonical types recursively describe hgraph temporal structures;
- `atomic<T>` stops recursive temporalization;
- `rolling<T, max_size[, min_size]>` describes a typed rolling window;
- `list<T[, size]>` describes an unbounded or fixed-size temporal list, with
  `unbounded` as the size sentinel;
- nominal `struct` declarations define one canonical scalar value and one
  recursively temporalized bundle shape; abstract structs define data families
  and every concrete struct is implicitly final;
- `delta<S>(...)` constructs a sparse update without weakening complete-value
  field requirements;
- bodyless nominal `operator` declarations define generic callable contracts;
- `impl fn` declarations supply operator implementations explicitly;
- operators and their implementation candidates are public by definition,
  while an ordinary exact function requires `export fn` for public exposure;
- name resolution selects an operator before hgraph ranks its implementations;
- runtime collection traversal uses borrowed, non-escaping iterators;
- ordinary `fn` bodies compose wiring, while node-only declarations and blocks
  classify a body as runtime evaluation for a generated node.

## Illustrative program

```hgl
module examples.prices

use hgraph.analytics::{rolling_mean}

fn midpoint(
    tob: atomic<tuple<f64, f64>>
) -> f64 =>
    (tob[0] + tob[1]) / 2.0

export fn smooth(
    tob: atomic<tuple<f64, f64>>,
    const window: i64 = 20
) -> f64 {
    rolling_mean(midpoint(tob), window)
}
```

There are no `graph` or `node` declaration keywords, no temporal wrapper on
`f64`, and no punctuation separating temporal and wiring parameters.

## Declarations

The agreed declaration forms are:

- `module` for a compilation and import unit;
- `use` for selective imports and aliased module namespaces;
- `operator` for a bodyless nominal generic contract;
- `fn` for module-internal named functions;
- `impl fn` for an implementation of an operator in scope;
- `export fn` for a public ordinary exact function;
- `struct` for a module-internal nominal structured type;
- `abstract struct` for a module-internal abstract data family;
- `export struct` for a public nominal structured type;
- `export abstract struct` for a public abstract data family;
- anonymous `fn(...) => expression` values.

Within a function body, `let` introduces an immutable lexical binding and
`var` introduces a mutable lexical binding. Neither persists between runtime
evaluations. Persistent mutable data is declared with `state`; fixed
caller-supplied wiring policy is declared with a `const` parameter.

An `fn` may use a concise expression body or a brace-delimited block with a
tail expression. An outputless function omits its return arrow. An `operator`
ends with its signature, never has a body, and is automatically public.

## Parameters and results

An ordinary parameter is temporal:

```hgl
price: f64
positions: map<str, f64>
tob: atomic<tuple<f64, f64>>
```

A `const` parameter is a wiring-time value:

```hgl
const window: i64 = 20
const settings: map<str, str>
```

Function results are temporal unless the function is outputless. No syntax for
a returned wiring scalar has been agreed.

## Structured values and deltas

One struct declaration supplies the value schema and temporal shape that
Python currently expresses separately as `CompoundScalar` and
`TimeSeriesSchema`:

```hgl
export struct Quote {
    bid: f64
    ask: f64
    venue: str = null
}
```

`const Quote` is the scalar value, temporal `Quote` recursively expands into a
field-wise bundle, and `atomic<Quote>` is one endpoint carrying the complete
value. Atomic markers may occur at any nested boundary and are erased when
deriving the canonical scalar schema.

Complete construction is named and checks required/default/optional metadata.
A sparse delta has a separate contextual constructor:

```hgl
Quote(bid: 100.0, ask: 101.0)
delta<Quote>(bid: 100.5)
```

Omitted delta fields mean no change even when the corresponding complete-value
field is required. Defaults never run for a delta. `null` declares or clears an
optional field; it is not the representation of an omitted delta field.
`delta<S>` is intentionally not a normal parameter, result, state, or stored
field type because temporal schemas already define their own delta shapes.
Runtime code returns it or assigns it to injected `out`; there is no separate
output keyword.

The runtime already supports sparse TSB delta fields through Bundle validity.
Explicit optional-field clearing still needs a distinct public native
operation or delta encoding, because the existing unset delta field means no
change. The compiler must preserve that distinction through checked HIR and
reject the clear form until its native contract exists.

### Abstract structs and final values

Struct inheritance is restricted to abstract data families:

```hgl
export abstract struct Instrument {
    symbol: str
    venue: str = null
    currency: str = "USD"
}

export struct EuropeanInstrument: Instrument {
    venue = "XLON"
    currency = "GBP"
}
```

Only an `abstract struct` may appear as a parent. Abstract structs are not
constructible; they may inherit one or more abstract parents and provide
common stored fields. Concrete structs may inherit one or more abstract
parents, are always constructible final leaves, and may be empty. There is no
concrete-to-concrete inheritance, `final` modifier, behavior inheritance, or
method-resolution order.

Field type and optionality are invariants of the declaration that first
introduces the field. A descendant cannot redeclare a field, change its type,
or change whether it may be unset. It may introduce or replace the inherited
constructor default with an untyped `field = constant` member. Defaults may
not be removed in the initial design. Consequently a non-optional field may
gain a default without becoming optional, while an optional field remains
clearable even after a descendant gives it a non-null default. A `null`
override is valid only for a field whose original declaration is optional.

Multiple abstract parents may contribute the same field only when type and
optionality agree. Identical defaults merge; differing defaults, including a
default contributed by only one otherwise compatible parent, require an
explicit choice in the child. Type and optionality conflicts are errors. Field
order remains stable schema metadata; the exact deterministic multiple-parent
linearization is deferred until implementation.

Scalar and `atomic<Abstract>` positions are polymorphic closed families whose
values retain the identity of one final concrete leaf. Their alternatives are
the concrete descendants registered by the compiled target's complete module
closure and frozen by the graph's type-realization snapshot. A temporal
abstract position is instead the fixed field-wise base bundle. Derived
temporal bundles do not implicitly convert to it, because that would hide a
projection graph and its change-tracking cost; the explicit projection
spelling remains open.

## Generics and nominal operators

Generic parameters follow an `operator` or `fn` name. Plain parameters bind
source types. Parameters prefixed with `const` bind wiring-time values that may
shape a type:

```hgl
operator summarize<
    T,
    const max_size: i64,
    const min_size: i64
>(window: rolling<T, max_size, min_size>) -> T
```

A repeated generic name denotes one consistent binding. Every generic required
by a selected candidate must resolve from inputs, expected output, explicit
generic arguments, defaults, or a solvable equality requirement. Generic
matching must lower to hgraph `TypePattern` and `ResolutionMap` rather than a
language-local matcher.

Different generic names are independent, while repetition requires equality:

```hgl
fn independent<U, V>(a: U, b: V) -> U => a
fn same<U>(a: U, b: U) -> U => a
```

The plain variable ranges over any HGL source type admitted by all of its use
sites. A temporal position permits scalar, container, structured, atomic, and
rolling types; a `const` position restricts the same variable to canonical
value types. Parameter and result context determines its representation, so
`U` is not intrinsically a time-series variable or a scalar variable. It is
also not a runtime `any`: every accepted call has a concrete substitution
before its graph or node is built.

Declarations express restrictions and derived substitutions with a trailing
`requires` clause:

```hgl
fn add_numeric<U>(a: U, b: U) -> U
requires U in {f64, i64}
=> a + b

operator get_field<U, V>(value: U, const name: str) -> V
requires U is struct
      && name in fields(U)
      && V == field_type(U, name)
```

Constraint expressions support closed type sets, type categories, structural
reflection, type equality, Boolean composition, and nominal operator
requirements. A type equality is declarative: it resolves one still-unbound
side when the other can be evaluated and validates the relationship when both
sides are known. Residual Boolean constraints only admit or reject a complete
substitution.

`struct` is a nominal, module-qualified source declaration. In scalar context
it denotes a Bundle-like value; temporal context recursively temporalizes its
fields into a named TSB-like shape; `atomic<S>` stops that recursion and carries
the complete scalar value. Fields are immutable and publicly readable.
Complete construction uses named arguments, enforces required fields, applies
ordinary defaults, and permits fields declared with `= null` to remain unset.
The contextual `delta<S>(...)` form instead permits every field to be omitted,
applies no defaults, and distinguishes omission from explicitly clearing an
optional field with `null`. The generic reflection interface is `fields`,
`has_fields`, and `field_type`; structural constraints do not erase nominal
identity.

A requirement such as `add(U, U) -> U` means that the already name-resolved
nominal operator must have a viable candidate for that type relationship. It
does not introduce another overload namespace or allow same-named operators to
compete.

The language preserves this symbolic type information through checked HIR.
Code generation may use one erased implementation when the body uses only
operations valid for every admitted substitution, or specialize an
implementation when representation-specific code is required. This is an
implementation decision: source generics remain statically substituted in
both cases.

An operator is a nominal contract analogous to a Rust trait or Swift protocol,
while retaining a function-shaped source declaration. Its identity is the
defining module plus name. The contract owns the public parameter names,
temporal-versus-`const` roles, defaults, generic input/output relationships,
and public requirements. It contains no graph or runtime implementation.

An `impl fn` binds to the local operator of the same name or to exactly one
such operator brought into local scope by a selective import; the binding is
written, never inferred from a name coincidence. `impl fn` with no operator in
scope is an error, and a plain `fn` that shares a name with an in-scope
operator is a conflict rather than a candidate. The function signature must be
compatible specialization of the contract and may itself be generic. Its body
is checked with the operator requirements in scope and classified through the
ordinary composition-versus-runtime rules. Candidate-specific requirements may
further restrict an implementation; dispatch applies the conjunction of the
mapped contract and candidate requirements.

Two operator contracts with the same short name but different defining modules
are unrelated. A namespace import such as `use my.module as mm` permits an
explicit `mm::my_op(...)` call without introducing `my_op` as an unqualified
implementation binding. Selectively importing two different operator
definitions under one local short name is an import error.

This separates two decisions. Language name resolution first selects one
nominal operator. Hgraph overload resolution then ranks only the implementation
candidates registered for that operator. An equal-ranked overlap within one
operator remains an ambiguity error; namespace selection is not an overload
tie-break.

## Visibility and implementation participation

An ordinary exact `fn` is visible throughout its defining module. `export fn`
adds that exact declaration to the public module interface; it does not create
an overload set. Other modules may selectively import it or call it through a
module alias.

An operator contract is public by definition. Every `impl fn` bound to that
operator contributes a public implementation candidate, but the candidate is
not independently importable through its provider module. `export` on an
`impl fn` is therefore invalid rather than a second visibility axis.

There are no declaration re-exports in the initial design. An operator has one
defining module and one canonical import identity even when implementations
come from many other modules.

The active implementation set is determined by the resolved application target
rather than source imports. It contains the target's source modules and all
modules in its locked package dependency closure. The compiler enumerates their
descriptors, collects every candidate by canonical operator identity, and
reports overlapping best matches with provider provenance. Packages merely
installed in the environment do not participate.

## Compiled module lifetime

Every compiled HGL module exposes compiler-generated initialization and
deinitialization entry points. Initialization records a keyed installer for all
type and operator contributions; the installer can be replayed after an hgraph
registry reset without repeating one-time module initialization. The final
application explicitly initializes the complete target closure before wiring.

The module manager retains an opaque registration handle. Removing that handle
deactivates the provider for future resolution, removes its installer intent so
a reset cannot resurrect it, removes its active registrations, and releases
owned resources. Dependencies initialize first and deinitialize in reverse
order. Initialization and deinitialization are idempotent, and failed
initialization rolls back its pending contribution.

Selected implementations give wired graphs and cached plans a lease on their
provider. A module cannot complete deinitialization, and its native image cannot
be unloaded, while such a lease is live. Logical registration removal and
physical library unloading are therefore distinct; the first implementation
may keep removed native images resident for process lifetime.

These lifecycle entry points are compiler and native-module infrastructure.
HGL source does not acquire arbitrary module-level side effects through `init`
or `deinit` blocks.

## Canonical temporal types

Source types describe values and structures without spelling hgraph's `TS`,
`TSB`, `TSL`, `TSS`, `TSD`, or `TSW` wrappers.

Temporalization proceeds recursively:

- scalar leaves such as `f64` become atomic endpoints;
- the temporal scalars (`date`, `time`, `datetime`, `duration`,
  `civil_datetime`, `timezone`, `zoned_datetime`, `zoned_time`) become atomic
  endpoints carrying the corresponding hgraph RFC 0002 types;
- tuples become un-named structural bundles with positional temporal children;
- lists become structural time-series lists, unbounded unless sized;
- sets become set-valued time series;
- maps become keyed temporal maps;
- rolling windows become typed hgraph `TSW` endpoints;
- records become structural bundles;
- `atomic<T>` becomes one endpoint carrying the complete canonical `T` value.

This distinguishes:

```hgl
tuple<f64, f64>                   // independently temporal children
atomic<tuple<f64, f64>>           // one tuple-valued endpoint

list<f64>                         // unbounded temporal list
list<f64, 3>                      // exactly three temporal elements

map<str, f64>                     // keyed temporal map
atomic<map<str, f64>>             // stream of complete map snapshots
map<str, atomic<tuple<f64, f64>>> // keyed atomic tuple values

set<str>                          // set-valued time series
atomic<set<str>>                  // stream of complete set snapshots

rolling<f64, 20>                 // maximum and minimum size are both 20
rolling<f64, 20, 5>              // maximum 20, valid from 5 values
rolling<f64, 5m>                 // the last five minutes, valid once spanned
rolling<f64, 5m, 1m>             // the last five minutes, valid from a 1m span
```

`const` bypasses temporalization. `const value: atomic<T>` is invalid because
atomicity describes a runtime temporal boundary.

The temporal scalars are the RFC 0002 core types: `date` and `time` are civil
values, `datetime` is an instant on the UTC timeline (the engine clock type),
`duration` is elapsed microseconds, `civil_datetime` is an uninterpreted wall
clock reading, `timezone` is an interned TZDB name, `zoned_datetime` is an
instant with its zone and resolved offset, and `zoned_time` is a wall-clock
time in a named zone, the one type the language adds to hgraph (recorded as
an RFC ask). The calendar period and range types stay library scalars under
their registered hgraph names. Literals are single tokens, `@` plus an
RFC 3339 shape with an optional RFC 9557 zone annotation (`@2026-09-03`,
`@09:30`, `@2026-09-03T09:30Z`, `@2026-09-03T10:30+01[Europe/London]`,
`@09:30[America/New_York]`, `@[Europe/London]`) or unit-suffixed numbers
(`5m`, `1.5h`, `1h30m`), validated and normalized when lexed, with one
canonical printed spelling per value; shorthand is accepted wherever it stays
unambiguous, and a literal never chooses a fold or gap policy silently. The
arithmetic table is hgraph's own, so an expression means the same thing in a
composition body, a runtime body, and a constant expression.

`rolling<T, max_size, min_size>` is inherently temporal rather than a canonical
scalar container. Its sizes are wiring-time constants of one kind, `i64` tick
counts or `duration` spans, omission of `min_size` normalizes it to
`max_size`, and the kind and both resolved sizes form part of the type
identity. The semantics are hgraph's: a tick window keeps the newest
`max_size` values, a duration window keeps every value within `max_size` of
the evaluation time, and either is invalid until it holds `min_size` values
or spans `min_size`. Rolling-window iteration and a spelling that accepts
either kind remain open.

Every expanded type must map to an existing public hgraph schema. A structural
tuple maps to hgraph's un-named bundle with index-named fields (`_0`, `_1`,
...), which is why heterogeneous tuples need no new runtime shape. A list size
is part of the type identity; a `const` generic in a list-size position binds
the argument's actual size, including the `unbounded` sentinel, so an
implementation indifferent to fixedness declares one generic candidate rather
than two.

## Function abstraction

`fn` intentionally hides hgraph implementation category from ordinary source
syntax. Operator calls behave this way too: after language name resolution
selects a nominal contract, hgraph may select a graph implementation or native
node implementation belonging to that contract.

For user functions, the current proposed classification rule is syntactic. A
body without node-only constructs is a composition function. The presence of
any of these constructs makes the complete body a runtime function:

- a `state` declaration;
- an `inject` declaration;
- a `start`, `when`, or `stop` block.
- a runtime `keys`, `values`, or `items` iterator.

Mixing wiring-only and runtime-only constructs is an error. Classification is
based on the resolved source body, not on the implementation kind selected for
a called operator.

A runtime function may declare persistent state, approved injected
capabilities, lifecycle behavior, and ordered activation handlers:

```hgl
fn combined_total(a: f64, b: f64) -> f64 {
    state total: f64 = 0.0
    inject out, logger

    start {
        logger.info("starting")
    }

    when modified(a) && valid(a) {
        total += a
        out = total
    }

    when modified(b) && valid(b) {
        total -= b
        out = total
    }

    stop {
        logger.info("stopping")
    }
}
```

All state variables aggregate into one typed state value. State initializers
lower to replay-aware startup initialization and do not overwrite restored
state. `state` is by definition a time series and is always recordable: the
language sets as its default the practice hgraph's own library applies only to
loopback state. Bespoke non-temporal values, such as cached adaptor handles,
are not `state`; they belong to a separate global or module-level resource
concept whose syntax remains future work.

`inject` is a comma-separated function-level declaration of approved runtime
capabilities. It does not add caller-visible parameters. `out` is a special
injectable whose type comes from the function result and permits the runtime
body to inspect or incrementally update its output. Other injectables, such as
`logger`, `clock`, and `scheduler`, map to their hgraph selector contracts.

`start` and `stop` execute once at node startup and teardown. State storage and
injected capabilities are runtime-owned; `stop` expresses semantic
finalization, not ordinary value destruction. Arbitrary resources, callbacks,
threads, and transports remain native extension responsibilities.

Multiple `when` blocks are independent ordered conditions. The compiler uses
the union of their activation dependencies and the validity requirements common
to all handlers as the most permissive safe node-level policy. It lowers each
remaining predicate to a C++ `if` in source order. Later handlers observe state
and output changes made by earlier handlers.

`modified(a, b, ...)` is true when any listed input was modified, while
`valid(a, b, ...)` is true only when every listed input is valid. Both require
at least one argument. The compiler converts activation and validity predicates
into hgraph input metadata whenever they are statically representable. Only
residual conditions remain in the per-tick body.

In a runtime function, `return value` writes the complete output and terminates
the current evaluation. Reaching the end without a return or output mutation
produces no output tick. With `inject out`, whole-output writes are
last-write-wins; writes to distinct collection children accumulate into one
delta, and repeated writes to one child use the last value.

A runtime function without `when` uses hgraph's default input activation and
validity rules. Calls to scalar kernels, richer structural mutation, output
access during lifecycle hooks, and exact conditional-expression spelling
remain open.

The compiler must represent a function as unclassified until the explicit
source rule is applied. Scripted and AOT modes must classify identically.

## Calls and operators

Calls use ordinary positional and named arguments:

```hgl
rolling_mean(value, window)
rolling_mean(value, period: window)
```

Operator calls use hgraph's candidate registry, type patterns, defaults,
normalization, ranking, and rejection diagnostics. The implementation kind
selected by the registry is not visible in call syntax. The source name has
already resolved to one nominal operator identity before candidate matching.

Selective imports introduce unqualified operator names and determine which
contract an `impl fn` of that name binds. Module aliases create
qualified namespaces instead:

```hgl
use market.pricing::{value}
use risk.pricing as risk

value(trade)
risk::value(trade)
```

The implicit prelude maps arithmetic, comparison, equality, Boolean, indexing,
and other admitted expression syntax to standard operator contracts. Infix and
postfix syntax are not separate dispatch paths.

Wiring-time policy values are declared `const`. They select an overload or
immutable plan before evaluation and must not become a per-tick policy branch
unless the selected contract explicitly requires dynamic behavior.

## Temporal metadata

Metadata uses function syntax:

```hgl
modified(value)
valid(value)
modified(bid, ask)
valid(bid, ask)
all_valid(book)
last_modified(value)
delta(value)
```

The language does not expose `value.modified`, `value.valid`, or `value.value`.
Like `key_set`, the metadata calls follow the phase of their containing
function: in composition they wire hgraph's standard `valid`, `modified`, and
`last_modified_time` operators and yield time series; `modified` and `valid`
are evaluator-local metadata in runtime functions. The
compiler may consume them as activation and admission policy rather than
materializing Boolean time series. `valid(value)` tests top-level endpoint
validity; recursive child validity is a distinct operation named
`all_valid(value)`. In runtime evaluation, `last_modified(value)` returns the
endpoint's native `last_modified_time` as `datetime`. The `delta` result shape
remains open.

## Collection traversal

The collection surface separates a materialized temporal view from borrowed
runtime iteration. `key_set(tsd)` is available in both phases: composition
produces the live TSS key projection, while runtime evaluation exposes the
current borrowed set view. The calls `keys(value)`, `values(value)`, and
`items(value)` produce evaluation-local iterators and therefore make their
containing function a runtime function.

`values` is the common value-only spelling for TSB, TSD, TSL, and TSS; there is
no `elements` alias. `items` yields `(field, value)` for TSB, `(key, value)` for
TSD, and `(i64, value)` for TSL. `keys` applies only to TSB and TSD.

Every traversal accepts an optional predicate:

```hgl
items(book, modified)
values(symbols, added)
keys(book, removed)
items(book, fn(key, value) => last_modified(value) > some_time)
```

The built-in `added`, `modified`, and `removed` predicate names select native
delta ranges. TSD and unbounded TSL support all three; TSB and fixed TSL support
`modified`; TSS supports `added` and `removed`. A compatible named function or
concise inline `fn` describes an arbitrary predicate over the yielded bindings.
It is phase-checked into the loop rather than becoming a stored runtime
callable.

Iterator bindings retain the native child or membership-slot provenance needed
by `valid`, `modified`, `added`, `removed`, and `last_modified`, while ordinary
expressions see their scalar payload. Iterators cannot escape an evaluation or
be represented as state, output, or a public function result.

Inline iterator predicates are pure. They may capture readable surrounding
bindings but cannot mutate `var`, `state`, or `out`, or invoke runtime effects.

## Tests and running

Status: proposed (2026-09-03); the record is
[Syntax and semantics](../developer-guide/syntax-and-semantics.md#tests-and-the-evaluation-harness).

A module carries its own tests. A `test` declaration is a named block in
module scope that sees every declaration of the module, exported or not; its
body is composition-phase code plus `assert`. The `eval` form drives a
function through hgraph's replay and record harness:

```hgl
test midpoint_waits_for_both_sides {
    assert eval(midpoint, tob: [(1.0, _), (_, 3.0), (2.0, _)])
        == [_, 2.0, 2.5]
}
```

A dense sequence is one element per engine cycle, `_` meaning "no tick" on
the input side and "did not tick" on the output side; a timed sequence keys
each element by an offset or absolute time. Both map exactly onto hgraph's
own `eval_node` alignment and its sparse absolute-time recording, so a test
in the language and a test of the same operator in C++ or Python observe the
same ticks. Tests never lower into a build artifact; `hgl test` runs them.

There is no `main` and no in-language run call. A module describes graphs;
running binds an exported function without temporal parameters to a mode, a
clock, and constant parameters from outside the module, through the `hgl
run` command line or a TOML file. The same module therefore runs unchanged
as a backtest and as a live process.

## Native boundary

Language source cannot:

- include native headers or name arbitrary C++ symbols;
- open files, sockets, or processes directly;
- create threads, callbacks, mutexes, or push-source senders;
- register native scalar types or services;
- bypass hgraph wiring, scheduling, state, lifecycle, or overload contracts.

Those capabilities live in C++ packages and are surfaced through reviewed
module descriptors.

## Open semantic questions

Later decisions must define:

- `i64` overflow, conversion, and division behavior;
- NaN comparison;
- generic struct declarations, self-recursive fields, destructuring, and
  copy-with-update syntax;
- runtime type tests, concrete downcasts, exhaustive abstract-family matching,
  the temporal base-projection spelling, and multiple-parent field ordering;
- explicit generic arguments, generic defaults, and any specialization
  relationship beyond the defined pattern ranking and ambiguity rule;
- general anonymous capture beyond inline runtime collection predicates;
- rolling-window iteration and a parameter spelling that accepts either
  window kind;
- an explicit end bound and approximate comparison for `eval`, delta
  spellings for set, map, and list harness elements, and tuple construction
  from temporal values;
- collection delta literals and the native encoding for explicit optional-field
  clearing;
- runtime scalar kernels, ephemeral caches, lifecycle output access, and sinks.

Diagnostics should identify the source concept and expanded hgraph shape while
preserving candidate rejection reasons from hgraph.
