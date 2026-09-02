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

fn smooth(
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

## Reading the body

The concise `=> expression` form is useful for a single-expression function.
A block uses its final expression as its result. Explicit `return` may be used
for early exits once function control-flow semantics are settled.

`tob[0]` and `tob[1]` are source operations over a temporal value. They do not
read a tuple while hgraph is being constructed. Their selected implementations
must preserve the atomic tuple's tick behavior.

`midpoint(tob)` and `rolling_mean(...)` use ordinary function-call syntax.
The compiler resolves imported contracts through hgraph's overload registry.
The call site does not spell whether a selected implementation is a native
node or a graph composition.

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
delta(positions)
```

`modified(a, b, ...)` is true when any argument changed. `valid(a, b, ...)` is
true only when every argument is valid. `valid(value)` tests the endpoint
itself; use `all_valid(value)` when every child of a structural or collection
endpoint must also be valid.

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
