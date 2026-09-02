# Types and expressions

Source types describe canonical values and structures. Temporal context is
supplied by the function signature, so authors do not write `ts`, `tsb`,
`tsl`, `tss`, or `tsd` wrappers.

## Scalar value types

The initial scalar vocabulary is:

| Type | Meaning | Literal |
| --- | --- | --- |
| `bool` | Boolean | `true` |
| `i64` | Signed 64-bit integer | `20` |
| `f64` | 64-bit floating-point value | `2.0` |
| `str` | UTF-8 string | `"bid"` |
| `datetime` | hgraph engine timestamp | literal syntax remains open |

In an ordinary parameter or result position, a scalar type is an atomic
time-series leaf. In a `const` parameter position, it is a wiring-time scalar.

```hgl
fn scale(value: f64, const factor: f64) -> f64 =>
    value * factor
```

## Recursive temporalization

Canonical containers and records become structural time-series shapes
recursively:

| Source type | Temporal interpretation |
| --- | --- |
| `f64` | Atomic endpoint carrying `f64` |
| `tuple<f64, f64>` | Structural tuple with temporal children |
| `list<f64>` | Structural list of temporal `f64` values |
| `set<str>` | Set-valued time series of `str` members |
| `map<str, f64>` | Keyed temporal map from `str` to temporal `f64` |
| a record type | Structural bundle whose fields are temporal |

The exact hgraph schema chosen for heterogeneous tuples is still open, but it
must be an existing public hgraph shape rather than a parallel runtime type.

## Atomic boundaries

`atomic<T>` stops recursive temporalization at that point:

| Source type | Temporal interpretation |
| --- | --- |
| `atomic<tuple<f64, f64>>` | One endpoint carrying a complete tuple |
| `atomic<set<str>>` | One endpoint carrying a complete set snapshot |
| `atomic<map<str, f64>>` | One endpoint carrying a complete map snapshot |
| `map<str, atomic<tuple<f64, f64>>>` | Keyed temporal map whose values are atomic tuples |

For example:

```hgl
fn midpoint(
    tob: atomic<tuple<f64, f64>>
) -> f64 =>
    (tob[0] + tob[1]) / 2.0
```

Both extracted components observe the atomic tuple's tick. Without `atomic`,
the tuple's children may tick independently.

Wrapping a scalar leaf, such as `atomic<f64>`, is redundant. The frontend may
accept it for symmetry and normalize it to `f64`; that choice is not yet fixed.

## Constant values

A `const` parameter is never temporal, so its canonical type is used directly:

```hgl
const window: i64
const labels: map<str, str>
```

Defaults are constant expressions and cannot depend on a temporal parameter:

```hgl
fn moving_average(
    value: f64,
    const window: i64 = 20
) -> f64 =>
    rolling_mean(value, window)
```

## Local bindings

`let` introduces an immutable lexical binding. `var` introduces a mutable
lexical binding:

```hgl
let scale = 2.0
var total = 0.0
total += value * scale
```

Both are local to the block in which they are declared. In a composition
function they hold wiring-time values or port handles; reassigning a `var`
changes the local handle, not a time-series value. In a runtime function they
hold evaluation-local scalar values and are recreated whenever the containing
block executes. Use `state`, not `var`, for a value that must survive into a
later evaluation.

An initializer is required in the first language slice. `let` cannot be
assigned again. A `var` may use ordinary and compound assignment. Function
parameters and `for` bindings remain immutable.

## Calls and operators

Calls use ordinary positional and named arguments:

```hgl
rolling_mean(value, window)
rolling_mean(value, period: window)
```

Calls to imported contracts use hgraph's candidate matching, defaults, scalar
lifting, ranking, and output resolution. Whether the selected implementation
is a graph or node is invisible at the call site.

The prelude binds familiar tokens to standard operator contracts:

| Precedence, high to low | Tokens |
| --- | --- |
| Unary | `-x`, `!x` |
| Multiplicative | `*`, `/`, `%` |
| Additive | `+`, `-` |
| Comparison | `<`, `<=`, `>`, `>=` |
| Equality | `==`, `!=` |
| Boolean AND | `&&` |
| Boolean OR | `\|\|` |

Expression syntax is not a second operator implementation path.

## Temporal metadata

Temporal metadata uses functions rather than endpoint members:

```hgl
modified(value)
valid(value)
modified(bid, ask)
valid(bid, ask)
all_valid(book)
last_modified(value)
delta(value)
```

Source does not expose `value.modified`, `value.valid`, or `value.value`.
In a runtime `when` predicate, `modified(value)` and `valid(value)` inspect the
input endpoint while ordinary expressions read its current payload. Both
predicates accept one or more arguments: `modified(a, b, c)` is true when any
argument was modified, while `valid(a, b, c)` is true only when every argument
is valid. Calls without arguments are invalid.

For a structural or collection input, `valid(value)` tests the validity of the
endpoint itself rather than recursively requiring every child to be valid.
Recursive child validity uses the distinct `all_valid(value)` predicate. The
compiler uses these metadata predicates to derive node activation and validity
policies where possible. The shape of `delta(value)` remains open.

In a runtime function, `last_modified(value)` returns the hgraph engine time at
which the endpoint last changed. It is the source spelling of the endpoint's
native `last_modified_time` metadata and has type `datetime`.

The same metadata functions apply to explicitly injected output:

```hgl
inject out

if valid(out) {
    out += value
}
```

In a runtime expression, reading `out` observes its current value. Assigning
the complete output or one of its collection children produces the matching
tick or delta.

## Collection views and iteration

`key_set(value)` exposes the keys of a temporal map as `set<K>`. It works in
both function phases: a composition function receives the live set-valued
time-series projection, while a runtime function receives the current borrowed
key-set view.

Runtime functions traverse structural collections with three regular
operations:

| Operation | Supported structures | Yielded bindings |
| --- | --- | --- |
| `keys(value)` | bundle, temporal map | field name or map key |
| `values(value)` | bundle, temporal map, temporal list, temporal set | child value, or a set member |
| `items(value)` | bundle, temporal map, temporal list | `(key, value)`, `(field, value)`, or `(index, value)` |

A temporal-list index yielded by `items` is an `i64`. There is no separate
`elements` operation; `values` is the common value-only spelling for temporal
bundles, maps, lists, and sets.

Each traversal accepts an optional predicate. The built-in `modified`, `added`,
and `removed` predicates select the corresponding hgraph delta range:

```hgl
for key, value in items(book, modified) {
    consume(key, value)
}

for symbol in values(symbols, added) {
    subscribe(symbol)
}

for symbol in values(symbols, removed) {
    unsubscribe(symbol)
}
```

The available built-in delta predicates follow the underlying structure:

| Structure | Full traversal | Built-in delta predicates |
| --- | --- | --- |
| Bundle (TSB) | `keys`, `values`, `items` | `modified` on values and items |
| Temporal map (TSD) | `keys`, `values`, `items` | `added`, `modified`, `removed` |
| Fixed temporal list (TSL) | `values`, `items` | `modified` |
| Unbounded temporal list (TSL) | `values`, `items` | `added`, `modified`, `removed` |
| Temporal set (TSS) | `values` | `added`, `removed` |

A compatible named function or inline concise function provides a general
predicate. Its parameters match the traversal result: one parameter for `keys`
or `values`, and two for `items`.

```hgl
for key, value in items(
    book,
    fn(key, value) =>
        valid(value) && last_modified(value) > some_time
) {
    consume(key, value)
}
```

Child bindings retain endpoint metadata, so ordinary expressions read their
current payload while `valid`, `modified`, and `last_modified` inspect the
child endpoint. Set members and collection keys are scalar bindings that also
retain the membership provenance needed by `added` and `removed`.

Iterator predicates are pure filters. They may read admitted values and
capture surrounding bindings, but they cannot mutate a `var`, `state`, or
`out`, and cannot invoke an effect such as logging.

These iterators are evaluation-local borrowed views. They may be consumed by a
`for` loop but cannot be returned, stored in state, assigned to output, or kept
for a later evaluation. Collection traversal is not available during graph
composition; use graph operations such as `key_set` and `map` there instead.

## Open scalar edge cases

Before executable code generation, the language must define `i64` overflow,
division by zero, NaN comparison, Unicode normalization, and runtime scalar
error behavior independently of C++ debug or release settings.
