# Types and expressions

Source types describe canonical values and structures. Temporal context is
supplied by the function signature, so authors do not write `ts`, `tsb`,
`tsl`, or `tsd` wrappers.

## Scalar value types

The initial scalar vocabulary is:

| Type | Meaning | Literal |
| --- | --- | --- |
| `bool` | Boolean | `true` |
| `i64` | Signed 64-bit integer | `20` |
| `f64` | 64-bit floating-point value | `2.0` |
| `str` | UTF-8 string | `"bid"` |

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
| `map<str, f64>` | Keyed temporal map from `str` to temporal `f64` |
| a record type | Structural bundle whose fields are temporal |

The exact hgraph schema chosen for heterogeneous tuples is still open, but it
must be an existing public hgraph shape rather than a parallel runtime type.

## Atomic boundaries

`atomic<T>` stops recursive temporalization at that point:

| Source type | Temporal interpretation |
| --- | --- |
| `atomic<tuple<f64, f64>>` | One endpoint carrying a complete tuple |
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
| Boolean OR | `||` |

Expression syntax is not a second operator implementation path.

## Temporal metadata

Temporal metadata uses functions rather than endpoint members:

```hgl
modified(value)
valid(value)
delta(value)
```

Source does not expose `value.modified`, `value.valid`, or `value.value`.
In a runtime `when` predicate, `modified(value)` and `valid(value)` inspect the
input endpoint while ordinary expressions read its current payload. The
compiler uses metadata predicates to derive node activation and validity
policies where possible. Exact aggregation rules for structural inputs and the
shape of `delta(value)` remain open.

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

## Open scalar edge cases

Before executable code generation, the language must define `i64` overflow,
division by zero, NaN comparison, Unicode normalization, and runtime scalar
error behavior independently of C++ debug or release settings.
