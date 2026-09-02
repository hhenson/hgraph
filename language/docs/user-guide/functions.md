# Functions

`fn` is the only user-facing function declaration. Source does not label a
definition as a graph or node.

## Named functions

A named function has typed parameters, an optional output type that is omitted
for outputless functions, and either a block or concise expression body:

```hgl
fn midpoint(bid: f64, ask: f64) -> f64 {
    (bid + ask) / 2.0
}

fn double(value: f64) -> f64 =>
    value * 2.0
```

Parameters and `let` bindings are immutable. `let` introduces an inferred
local:

```hgl
fn smooth(
    tob: atomic<tuple<f64, f64>>,
    const window: i64
) -> f64 {
    let mid = midpoint(tob)
    rolling_mean(mid, window)
}
```

Calls accept positional arguments followed by named arguments:

```hgl
smooth(tob, window: 50)
```

## Temporal and constant parameters

An unmodified parameter is temporal:

```hgl
price: f64
positions: map<str, f64>
tob: atomic<tuple<f64, f64>>
```

A `const` parameter is a wiring-time value:

```hgl
const window: i64 = 20
const enabled: bool
const settings: map<str, f64>
```

`const` and `atomic` are orthogonal:

- `const` changes **when** a parameter exists: once while the function is
  constructed rather than over runtime ticks.
- `atomic<T>` changes **how** a temporal value is represented: `T` ticks as one
  endpoint rather than being recursively temporalized.

Because a `const` parameter is not temporal, `const value: atomic<T>` is
invalid.

## Anonymous functions

An anonymous function uses the same keyword:

```hgl
map(values, fn(value) => value * 2.0)
```

Types may be explicit when context cannot determine them:

```hgl
reduce(
    values,
    fn(lhs: f64, rhs: f64) -> f64 => lhs + rhs
)
```

An anonymous function closes over immutable locals and `const` parameters.
Capture of temporal values and the resulting hgraph binding rules remain to be
specified with higher-order function semantics.

## Outputless functions

An outputless function omits the return arrow:

```hgl
fn observe(price: f64, const label: str = "price") {
    debug_print(price, label)
}
```

A call used as a statement must resolve to an outputless contract. Silently
discarding a temporal result is an error.

## Composition and runtime functions

The current design uses body constructs rather than a declaration keyword to
classify an `fn`.

An ordinary expression body describes composition:

```hgl
fn midpoint(bid: f64, ask: f64) -> f64 {
    (bid + ask) / 2.0
}
```

The body runs while hgraph is wired. Its operators and calls compose existing
contracts. The function itself flattens into the resulting primitive nodes and
does not remain as a runtime evaluation object.

A function containing `state`, `inject`, `start`, `when`, or `stop` describes
runtime evaluation and is compiled as one node:

```hgl
fn add_when_ready(a: f64, b: f64) -> f64 {
    when modified(a, b) && valid(a) {
        if valid(b) {
            return a + b
        }
        return 0.0
    }
}
```

Here either input can schedule evaluation, `a` must be valid, and the guarded
read permits `b` to be invalid. In a runtime function, `return value` writes
one output tick and terminates the current evaluation. Reaching the end without
writing or returning produces no output tick.

This can deliberately fuse work that would otherwise become several
primitive nodes and intermediate endpoints. Graph composition still flattens;
the possible saving comes from eliminating intermediate nodes, bindings,
scheduling, and change tracking rather than from removing a graph wrapper.

## State

`state` declares mutable data that persists across evaluations:

```hgl
fn accumulator(a: f64, b: f64) -> f64 {
    state total: f64 = 0.0

    when modified(a) && valid(a) {
        total += a
        return total
    }

    when modified(b) && valid(b) {
        total -= b
        return total
    }
}
```

State declarations are function-level declarations. All state variables in a
function are aggregated into one typed state value. Initializers run during
node startup and do not overwrite state restored for record/replay. State that
affects later ticks is recordable by default.

`let` remains an immutable lexical binding. A `let` inside `when` exists only
for that evaluation; it does not become persistent merely because it appears
in a runtime function.

## Injectables

`inject` requests approved hgraph runtime capabilities without adding them to
the callable signature:

```hgl
inject out, logger, clock, scheduler
```

The comma-separated form may span lines and may have a trailing comma:

```hgl
inject
    out,
    logger,
    clock,
    scheduler,
```

Capabilities are function-level declarations at the same level as `state`.
The compiler supplies each injectable only to lifecycle or evaluation hooks
that use it. Duplicate, unknown, and phase-incompatible injectables are errors.

## Lifecycle

Runtime functions may have one `start` block and one `stop` block:

```hgl
fn monitored_total(value: f64) -> f64 {
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

`start` runs once when the node starts, after replay-aware state initialization.
`stop` runs once during teardown and is used for semantic finalization such as
flushing or releasing an approved handle. Ordinary state storage and injected
capabilities are runtime-owned and require no explicit disposal. External
threads, callbacks, transports, and arbitrary native resources remain the
responsibility of C++ extensions.

## Ordered activation

Multiple `when` blocks are tested from top to bottom as independent conditions,
not as an implicit `else if` chain. Later blocks observe state and output
changes made by earlier blocks during the same evaluation:

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

The node uses the most permissive safe outer policy: the union of inputs that
can activate any block and only the validity requirements common to every
block. Handler-specific predicates remain ordered runtime checks.

If both inputs change, both blocks execute and the second whole-output write
wins. A `return` in an earlier block terminates evaluation and prevents later
blocks from running.

## Output access

Most runtime functions only need `return`. Direct output access is opt-in:

```hgl
fn running_total(value: f64) -> f64 {
    inject out

    when modified(value) && valid(value) {
        if valid(out) {
            out += value
        } else {
            out = value
        }
    }
}
```

`out` gets its type from the function result. Reading it observes the current
output value, including writes performed earlier in the same evaluation.
Assigning it produces an output tick. `return value` is equivalent to assigning
the complete output and then terminating evaluation.

Use `out` as the previous state when the last public output is exactly the
information required by the next evaluation. Use `state` when the function
needs private information, when that information may change without producing
an output tick, or when it differs from the output shape.

Collection output supports incremental mutation:

```hgl
fn latest_by_key(key: str, value: f64) -> map<str, f64> {
    inject out

    when modified(value) && valid(key, value) {
        out[key] = value
    }
}
```

Whole-output assignments are last-write-wins. Writes to different collection
children accumulate into one output delta; repeated writes to the same child
use the last value. `inject out` is invalid on an outputless function and `out`
is initially restricted to evaluation code rather than `start` or `stop`.

A runtime function without `when` uses hgraph's ordinary policy: any temporal
input can activate it and all ordinary inputs must be valid. `when` is needed
only to customize activation, validity, or ordered conditional handling.

The exact conditional-expression spelling, structural metadata aggregation,
ephemeral cache syntax, and calls between runtime functions remain provisional.

Imported functions are already implementation-neutral: hgraph's overload
registry may select a graph or native node implementation without changing the
call site.
