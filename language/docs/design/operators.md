# System operators and domain properties

Status: agreed design; executable declarations, descriptor metadata, and the
current symbol set are implemented. Floor-division token syntax remains blocked
by the existing `//` line-comment spelling (see below).

## Decisions from the discussion

- Use fixed system operator names, like a language protocol. Do not add a
  user-definable `symbol = "..."` clause or introduce Python dunder names.
- Reuse hgraph's names (`mul_`, not `mult_`) and native candidate resolution.
  Symbol syntax selects an operator identity; ordinary overload resolution
  selects its implementation and result type.
- Attach algebraic declarations to an operator **and a concrete type domain**.
  The initial vocabulary is `associative`, `commutative`, and `identity`.
- Result types and lifting are signature information, not algebraic properties.
  Division need not be closed over its input type.
- No inverse declarations, group/field hierarchy, automatic reduction detection,
  or loop-to-map/reduce conversion in this iteration.
- Missing metadata means **no guarantee**. A declaration is not a proof of a
  law for every implementation or every numerical policy.

## Fixed symbol-to-name mapping

| HGL expression | System operator | Notes |
| --- | --- | --- |
| `a + b` | `add_` | Includes string concatenation where a candidate exists. |
| `a - b` | `sub_` | Binary subtraction. |
| `a * b` | `mul_` | Multiplication. |
| `a / b` | `div_` | Numeric true division; `i64 / i64` produces `f64`. |
| `a % b` | `mod_` | Floor-based modulo, not C++ truncating remainder. |
| `-a` | `neg_` | Unary negation is a separate operator. |
| `!a` | `not_` | Boolean negation. |
| `a == b` | `eq_` | Equality. |
| `a != b` | `ne_` | Inequality. |
| `a < b` | `lt_` | Ordering requires an applicable candidate. |
| `a <= b` | `le_` | Ordering. |
| `a > b` | `gt_` | Ordering. |
| `a >= b` | `ge_` | Ordering. |
| `a && b` | `and_` | Graph composition wires both operands. |
| `a \|\| b` | `or_` | Graph composition wires both operands. |
| `a[index]` | `getitem_` | Existing collection projection rules also apply. |
| `a.field` | `getattr_` | Existing structural field projection rules also apply. |

`+=`, `-=`, `*=`, and `/=` combine assignment with the corresponding binary
operation; they do not introduce four more operator identities. Assignment and
comparison are distinct. Node Boolean expressions retain C++ short-circuit
evaluation; graph Boolean expressions do not conditionally wire their RHS.

The intended floor-division mapping is `a // b` to `floordiv_`, including
`i64 // i64 -> i64`. HGL currently lexes `//` as a line comment, including after
an expression. Changing that rule requires a decision about comment syntax;
whitespace or expression-context heuristics cannot reliably preserve existing
comments. Until resolved, use the ordinary `floordiv_(a, b)` call. This change
does not silently reinterpret existing comments. Power, bitwise, shifts, unary
plus, and user-defined tokens have no new HGL syntax in this iteration; their
native named operators remain separate library inventory work.

A local function named `add_` does not change `a + b`. Explicit named calls
still follow normal local/import lookup. System symbols resolve the native
identity, not the nearest function with a matching short name.

## Domain-bound declarations

```hgl
operator mul_<T>(lhs: T, rhs: T) -> T
properties<i64> { commutative, identity = 1 }

operator add_<T>(lhs: T, rhs: T) -> T
properties<str> { associative, identity = "" }
```

`properties<...>` follows the signature and optional `requires` clause. Entries
are comma-separated; a trailing comma and multiline layout are allowed.
Multiple clauses specialize the **same nominal operator**, not unrelated
operators that happen to have its short name. Properties apply to conforming
candidates in that domain; a consumer must still identify the actual selected
candidate and its numerical policy before using a property as a rewrite rule.

The selector binds declaration type parameters **in declaration order**:

```hgl
operator mul_<L, R, O>(lhs: L, rhs: R) -> O
properties<i64, i64, i64> { commutative, identity = 1 }
```

This selects `(i64, i64) -> i64`, not every candidate containing an `i64`.
It says nothing about `(i64, f64) -> f64`. The domain must satisfy the operator's
`requires` clause. Unknown properties, repeated properties, repeated domains,
wrong selector arity, non-concrete types, and incorrectly typed identities are
errors. The first implementation admits concrete type selectors and scalar
constant identities; const-generic selectors, partial domains, and value-range
or numerical-policy predicates are deferred. `ref` and `signal` are not value
domains for these algebraic declarations.

Both laws describe binary operators with the same two input types.
Associativity and identity additionally require closure, `(T, T) -> T`.
Commutativity may describe `(T, T) -> bool`, for example equality. An identity
is two-sided and must be a compile-time value of the result type. An identity
alone proves neither associativity nor commutativity.

## Numerical exceptions and reduction

Algebraic laws describe observable behavior, including failure and special
values, not just a mathematical analogy:

- String concatenation is associative but not commutative; `""` is its identity.
- Signed integer addition/multiplication cannot advertise unconditional
  associativity without an overflow policy. Regrouping can change which
  intermediate operation overflows.
- Floating-point addition/multiplication are not exactly associative.
  NaNs, signed zero, infinities, rounding, and exception policy also matter to
  identities and reordering. There is no unconditional floating-point
  associativity declaration or implicit “fast math” permission.
- Floating `min`/`max` cannot inherit total-order laws when NaNs and signed-zero
  selection are observable. Custom native/Python scalar operators inherit no
  laws merely because C++ supplies an overloaded operator.

The native lifted kernels now publish conservative, specialization-specific
metadata. Unknown guarantees are false in that API. Known string concatenation,
Boolean logic, integral bitwise operations, and supported total-order extrema
retain their applicable guarantees. Closed unsigned arithmetic can use modular
laws; that does not change HGL `i64` into an unsigned or wrapping type.

The compiler checks and preserves HGL declarations in HIR, graph IR and JSON
module descriptors. It does **not** prove arbitrary implementations, copy those
claims into trusted native kernel flags, or enable new optimizations from them.
Descriptor loading validates metadata shape and checks identity literals against
the result type after substituting the declared domain (including the normal
`i64` to `f64` widening). It does not verify mathematical truth. The
existing descriptor import catalog remains a native-function boundary; general
operator-contract imports and optimizer proof transport are not implemented.

An explicitly requested `reduce` retains its own contract. Removing an unsafe
native law prevents the lifted reduction fast path; it does not silently change
the caller's chosen tree reduction into an ordered fold. Reduction `zero`
remains distinct from a kernel identity: no implicit substitution, especially
for empty or singleton inputs. Unordered maps need an appropriate reduction;
order-sensitive lists may need linear reduction. Automatic dynamic-loop
accumulation remains deferred as agreed in [Iteration](iteration.md).

## Signatures, lifting, and implementation

```hgl
operator div_<L, R, O>(lhs: L, rhs: R) -> O
```

This contract admits a candidate `(i64, i64) -> f64`. A floor-division candidate
can instead be `(i64, i64) -> i64`. Selecting an output type is ordinary
candidate resolution, not an `inverse`, `associative`, or “loss” annotation.
Floor division rounds down (`-7` divided by `3` yields `-3`), and modulo has the
corresponding sign (`-7 % 3 == 2`, `7 % -3 == -2`). Division by zero is an error
for these default operations. Named native calls may expose explicit policies.
Floating-point modulo forms a remainder directly, then adjusts to the divisor's
sign (including signed zero). It does not form `lhs / rhs`: an overflowing or
underflowing quotient must not corrupt the remainder. For example,
`1.0 % (1e308 * 2.0)` is `1.0`, even though the divisor is positive infinity.
Constant folding, graph wiring, and node evaluation follow this same rule.

Native scalar lifting wraps a precise function signature as a time-series
candidate. A graph expression wires that candidate; node code evaluates its
scalar operation. See the [paired HGL/C++ examples](../developer-guide/operator-cpp-mappings.md).

The executable [operator module](../../stdlib/hgl/hgraph/operators.hgl) declares
the 16 arithmetic/comparison/Boolean hooks (including named floor division),
delegates implementations to production native candidates, and materializes
the supported primitive combinations. Its `hgraph.operators.*` identities are
parallel migration contracts, not replacements for the native system identities.
The existing `getitem_`/`getattr_` projections are not redeclared as scalar binary
arithmetic. Broader temporal, collection, and downstream scalar domains remain
in the native registry until their HGL materializations are covered.
