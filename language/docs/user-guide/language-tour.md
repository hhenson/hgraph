# Language tour

This module defines a midpoint function and composes it with an imported
rolling operator:

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

There are no statement terminators or signature separators. Newlines separate
forms, braces delimit block bodies, and `//` starts a line comment.

## Reading the signature

`tob` is temporal because function parameters are temporal by default. Its
declared value type is a two-element tuple, and `atomic<...>` says the complete
tuple arrives and ticks as one endpoint.

`window` is marked `const`, so it is an ordinary `i64` available while hgraph
constructs the function. It cannot change from tick to tick.

The return type `f64` is temporal. Source type syntax describes the value and
structural shape; it does not require an outer `ts<...>` wrapper.

`midpoint` is an internal helper. `export fn smooth` makes the exact `smooth`
function part of this module's public interface without turning it into an
overload family.

## Reading the body

The concise `=> expression` form is useful for a single-expression function.
A block uses its final expression as its result. Explicit `return` may be used
for early exits once function control-flow semantics are settled.

`tob[0]` and `tob[1]` are source operations over a temporal value. They do not
read a tuple while hgraph is being constructed. Their selected implementations
must preserve the atomic tuple's tick behavior.

`midpoint(tob)` and `rolling_mean(...)` use ordinary function-call syntax.
The compiler resolves imported operator contracts through hgraph's overload
registry. The call site does not spell whether a selected implementation is a
native node or a graph composition. An imported exact `export fn` instead has
one target and bypasses overload ranking.

## Operator contracts and module namespaces

A bodyless `operator` defines a nominal generic contract. `impl fn`
declarations provide its implementations:

```hgl
operator combine<T>(lhs: T, rhs: T) -> T

impl fn combine(lhs: f64, rhs: f64) -> f64 =>
    lhs + rhs
```

The operator contract is public automatically. Its `impl fn combine`
declaration is published as an implementation candidate through that operator;
it is not an independently importable exact function and does not use
`export`. The `impl` modifier is mandatory, so a misspelt implementation name
is an error rather than an unrelated private function.

An implementation module selects an externally defined operator with a
selective import such as `use my.contracts::{combine}`. Exactly one operator
definition may occupy that unqualified binding. A module alias instead keeps
the namespace explicit and does not bind local implementations:

```hgl
use market.pricing as market
use risk.pricing as risk

market::value(trade)
risk::value(trade)
```

Name resolution chooses the nominal operator before hgraph ranks its
implementations, so equal short names in different modules do not compete.

Generic `const` parameters can shape rolling-window types:

```hgl
operator preserve_window<
    T,
    const max_size: i64,
    const min_size: i64
>(window: rolling<T, max_size, min_size>)
    -> rolling<T, max_size, min_size>

impl fn preserve_window<
    T,
    const max_size: i64,
    const min_size: i64
>(window: rolling<T, max_size, min_size>)
    -> rolling<T, max_size, min_size> =>
    window
```

`rolling<f64, 20>` omits the optional minimum size and currently means the
same maximum and minimum size. `rolling<f64, 20, 5>` becomes valid from five
values while retaining at most twenty.

Generic declarations may carry compile-time requirements:

```hgl
operator choose_number<U>(lhs: U, rhs: U) -> U
requires U in {f64, i64}

impl fn choose_number<U>(lhs: U, rhs: U) -> U => lhs

operator double<U>(value: U) -> U
requires add(U, U) -> U

impl fn double<U>(value: U) -> U => value + value
```

Repeating `U` requires the arguments and result to share one canonical source
type. The first contract restricts that type to `f64` or `i64`. The second
requires the nominal `add` operator to accept two `U` values and produce `U`;
its implementation may rely on that guarantee without repeating it. `hgl
check` evaluates closed requirements during typed-HIR completion and asks the
hgraph operator registry to decide native operator viability. Cases needing
native nominal-struct metadata, arbitrary constant predicates, or source
candidate ranking remain explicitly deferred or fail closed as described in
the roadmap. Requirements are never tested per tick.

## Anonymous functions

The same `fn` keyword introduces a concise anonymous function:

```hgl
fn notionals(
    prices: map<str, f64>,
    quantities: map<str, i64>
) -> map<str, f64> =>
    map(
        prices,
        quantities,
        fn(price, quantity) => price * quantity
    )
```

Context may infer anonymous parameter and result types. The exact inference
rules are still provisional, but anonymous and named functions share one
callable model.

## Wiring composition and runtime evaluation

`const` values may select fixed topology:

```hgl
fn maybe_smooth(
    price: f64,
    const enabled: bool = true,
    const window: i64 = 20
) -> f64 {
    if enabled {
        return rolling_mean(price, window)
    }
    price
}
```

Temporal metadata uses function syntax rather than endpoint properties:

```hgl
modified(price)
valid(price)
modified(bid, ask)
valid(bid, ask)
all_valid(book)
last_modified(price)
delta(positions)
```

`modified(a, b, ...)` is true when any argument changed. `valid(a, b, ...)` is
true only when every argument is valid. `valid(value)` tests the endpoint
itself; use `all_valid(value)` when every child of a structural or collection
endpoint must also be valid.

Runtime collection traversal uses `keys`, `values`, and `items`. An optional
built-in, named, or inline predicate filters the traversal without changing
those base names:

```hgl
for key, value in items(book, modified) {
    consume(key, value)
}

for key, value in items(
    book,
    fn(key, value) =>
        valid(value) && last_modified(value) > some_time
) {
    consume(key, value)
}

for symbol in values(symbols, added) {
    subscribe(symbol)
}
```

`key_set(book)` is different from `keys(book)`: `key_set` is available in both
composition and runtime functions and produces the set-shaped key view, while
`keys`, `values`, and `items` are evaluation-local iterators. Temporal lists use
`values` for value-only traversal and `items` for `(i64, value)` traversal;
there is no separate `elements` spelling.

An ordinary body such as `maybe_smooth` runs at wiring time and composes
operators. Runtime-only declarations and blocks instead implement one generated
node:

```hgl
fn running_total(value: f64) -> f64 {
    state total: f64 = 0.0
    inject out, logger

    start {
        logger.info("starting")
    }

    when modified(value) && valid(value) {
        total += value
        out = total
    }

    stop {
        logger.info("stopping")
    }
}
```

`state` persists between evaluations and initializes during startup. `inject`
requests approved runtime facilities without changing the call signature.
`when` declares activation and validity, while writing `out` produces an output
tick. Ordinary `return value` is the concise alternative when previous or
incremental output access is not required.

The exact syntax is still a design preview, but the intended distinction is
important: composition wires operations, while node-only constructs declare
runtime behavior and permit the compiler to fuse the body into a node.
