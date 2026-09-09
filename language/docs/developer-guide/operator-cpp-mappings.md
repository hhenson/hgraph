# Operator source and C++ behavior

These paired examples describe the public mapping. Emitted code adds registration,
source locations, and wiring type checks. The executable fixtures are
[system-operators.hgl](../../tests/codegen/system-operators.hgl),
[operator-properties.hgl](../../examples/operator-properties.hgl), and their
[compiled tests](../../tests/codegen/generated_operator_tests.cpp).

## Graph true division

```hgl
export fn ratio(lhs: i64, rhs: i64) -> f64 => lhs / rhs
```

Corresponding C++ composition:

```cpp
struct ratio {
    static hgraph::Port<hgraph::TS<hgraph::Float>> compose(
        hgraph::Wiring &w,
        hgraph::Port<hgraph::TS<hgraph::Int>> lhs,
        hgraph::Port<hgraph::TS<hgraph::Int>> rhs) {
        return hgraph::wire<hgraph::stdlib::div_>(w, lhs, rhs)
            .as<hgraph::TS<hgraph::Float>>();
    }
};
```

Inputs `7, 2` produce `3.5`, not `3`. The symbol selects `div_`; its candidate
signature determines the output. `use hgraph.std::{div_}` followed by an explicit
`div_(lhs, rhs)` call selects the same native operator.

## Node evaluation and negative modulo

```hgl
export fn remainder(lhs: i64, rhs: i64) -> i64 {
    when modified(lhs) || modified(rhs) { return lhs % rhs }
}
```

Corresponding C++ evaluation:

```cpp
struct remainder {
    static void eval(hgraph::In<"lhs", hgraph::TS<hgraph::Int>> lhs,
                     hgraph::In<"rhs", hgraph::TS<hgraph::Int>> rhs,
                     hgraph::Out<hgraph::TS<hgraph::Int>> out) {
        if (lhs.modified() || rhs.modified()) {
            out.set(hgraph::stdlib::scalar_mod<hgraph::Int>::apply(
                lhs.value(), rhs.value()));
        }
    }
};
```

`-7 % 3` produces `2`, including in constant expressions and graph compositions.
Raw C++ `%` would produce `-1`. The shared scalar kernel checks a zero divisor
and avoids overflowing an intermediate quotient-times-divisor at integer
boundaries. `i64_min % -1` is `0`; floor division of that pair overflows.
Node true division similarly uses `scalar_div<Int>::apply`, yielding `Float`
and checking zero rather than relying on unchecked C++ `/`.

## A named floor-division call

```hgl
use hgraph.std::{floordiv_}
export fn quotient(lhs: i64, rhs: i64) -> i64 => floordiv_(lhs, rhs)
```

The corresponding expression in `compose` is:

```cpp
return hgraph::wire<hgraph::stdlib::floordiv_>(w, lhs, rhs)
    .as<hgraph::TS<hgraph::Int>>();
```

`-7, 3` produces `-3`. The intended `//` token cannot be enabled until the
line-comment conflict is resolved; the named call is executable today.

## A declared law is not a generated proof

```hgl
operator concatenate<T>(lhs: T, rhs: T) -> T
properties<str> { associative, identity = "" }
impl fn concatenate(lhs: str, rhs: str) -> str => lhs + rhs
```

The generated operator contract has one generic input/output binding. Its
`str` implementation wires `hgraph::stdlib::add_`. The descriptor retains the
concrete `str` domain, associativity claim, and typed empty-string identity.
No `commutative` flag is inferred, and the generated candidate does not acquire
trusted lifted-kernel metadata merely because the source declared a law.

In native authoring the relevant scalar function is
`hgraph::stdlib::scalar_add<hgraph::Str>`; `hgraph::lift<...>()` produces its
time-series candidate. Its verified metadata is associative, non-commutative,
with an empty-string identity. Neither `scalar_add<Float>` nor
`scalar_mul<Float>` advertises associativity.
