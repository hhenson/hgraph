# Language model

Status: agreed syntax foundation; provisional generic, module, and runtime semantics

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
generic arguments, or defaults. Constraint syntax and complete inference rules
remain open, but generic matching must lower to hgraph `TypePattern` and
`ResolutionMap` rather than a language-local matcher.

An operator is a nominal contract analogous to a Rust trait or Swift protocol,
while retaining a function-shaped source declaration. Its identity is the
defining module plus name. The contract owns the public parameter names,
temporal-versus-`const` roles, defaults, and generic input/output relationships.
It contains no graph or runtime implementation.

An `impl fn` binds to the local operator of the same name or to exactly one
such operator brought into local scope by a selective import; the binding is
written, never inferred from a name coincidence. `impl fn` with no operator in
scope is an error, and a plain `fn` that shares a name with an in-scope
operator is a conflict rather than a candidate. The function signature must be
a compatible specialization of the contract and may itself be generic. Its body
is classified through the ordinary composition-versus-runtime rules.

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
- `datetime` becomes an atomic hgraph engine timestamp;
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
```

`const` bypasses temporalization. `const value: atomic<T>` is invalid because
atomicity describes a runtime temporal boundary.

`rolling<T, max_size, min_size>` is inherently temporal rather than a canonical
scalar container. Its sizes are positive wiring-time `i64` values, omission of
`min_size` currently normalizes it to `max_size`, and both resolved sizes form
part of the type identity. Duration-window syntax and rolling-window iteration
remain open.

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
- record declarations;
- generic constraints, explicit generic arguments, output-directed inference,
  and overlapping-implementation coherence;
- general anonymous capture beyond inline runtime collection predicates;
- duration rolling-window syntax and rolling-window iteration;
- structural temporal metadata and delta result shapes;
- runtime scalar kernels, ephemeral caches, lifecycle output access, and sinks.

Diagnostics should identify the source concept and expanded hgraph shape while
preserving candidate rejection reasons from hgraph.
