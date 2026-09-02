# Language model

Status: agreed syntax foundation; provisional runtime-function semantics

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
- function calls resolve through hgraph contracts and overloads;
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

fn smooth(
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
- `use` for explicit typed imports;
- `fn` for named functions;
- anonymous `fn(...) => expression` values.

Within a function body, `let` introduces an immutable lexical binding and
`var` introduces a mutable lexical binding. Neither persists between runtime
evaluations. Persistent mutable data is declared with `state`; fixed
caller-supplied wiring policy is declared with a `const` parameter.

An `fn` may use a concise expression body or a brace-delimited block with a
tail expression. An outputless function omits its return arrow.

Whether the language later admits user-defined overload families remains open.
Imported names already represent implementation-neutral function contracts.

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

## Canonical temporal types

Source types describe values and structures without spelling hgraph's `TS`,
`TSB`, `TSL`, `TSS`, or `TSD` wrappers.

Temporalization proceeds recursively:

- scalar leaves such as `f64` become atomic endpoints;
- `datetime` becomes an atomic hgraph engine timestamp;
- tuples become structural tuples of temporal children;
- lists become structural time-series lists;
- sets become set-valued time series;
- maps become keyed temporal maps;
- records become structural bundles;
- `atomic<T>` becomes one endpoint carrying the complete canonical `T` value.

This distinguishes:

```hgl
tuple<f64, f64>                   // independently temporal children
atomic<tuple<f64, f64>>           // one tuple-valued endpoint

map<str, f64>                     // keyed temporal map
atomic<map<str, f64>>             // stream of complete map snapshots
map<str, atomic<tuple<f64, f64>>> // keyed atomic tuple values

set<str>                          // set-valued time series
atomic<set<str>>                  // stream of complete set snapshots
```

`const` bypasses temporalization. `const value: atomic<T>` is invalid because
atomicity describes a runtime temporal boundary.

Every expanded type must map to an existing public hgraph schema. Heterogeneous
tuple mapping remains an open detail.

## Function abstraction

`fn` intentionally hides hgraph implementation category from ordinary source
syntax. Imported calls already behave this way: a registry may select a graph
overload or native node overload under the same function contract.

For user functions, the current proposed classification rule is syntactic. A
body without node-only constructs is a composition function. The presence of
any of these constructs makes the complete body a runtime function:

- a `state` declaration;
- an `inject` declaration;
- a `start`, `when`, or `stop` block.
- a runtime `keys`, `values`, or `items` iterator.

Mixing wiring-only and runtime-only constructs is an error. Classification is
based on the resolved source body, not on the implementation kind selected for
an imported call.

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
state. State affecting later evaluations uses recordable state by default;
ephemeral cache syntax remains separate future work.

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

Imported calls use hgraph's candidate registry, type patterns, defaults,
normalization, ranking, and rejection diagnostics. The implementation kind
selected by the registry is not visible in call syntax.

The implicit prelude maps arithmetic, comparison, equality, Boolean, indexing,
and other admitted expression syntax to standard function contracts. Infix and
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
`modified` and `valid` are evaluator-local metadata in runtime functions. The
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
- tuple-to-hgraph structural mapping;
- record declarations;
- general anonymous capture and generic type inference beyond inline runtime
  collection predicates;
- structural temporal metadata and delta result shapes;
- runtime scalar kernels, ephemeral caches, lifecycle output access, and sinks.

Diagnostics should identify the source concept and expanded hgraph shape while
preserving candidate rejection reasons from hgraph.
