# Functions

Ordinary `fn` declares a temporal implementation. Source does not label
it as a graph or node. A bodyless `operator` declaration names
a generic callable contract whose implementations are supplied by `impl fn`
definitions. The agreed value-level extension is `const fn`, described below
with its implementation boundary.

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

`rolling_mean` in the next example is imported with
`use hgraph.analytics::{rolling_mean}`.

Parameters are immutable. `let` introduces an immutable local and `var`
introduces a mutable local; either may use an inferred type:

```hgl
fn smooth(
    bid: f64,
    ask: f64,
    const window: i64
) -> f64 {
    let mid = midpoint(bid, ask)
    rolling_mean(mid, window)
}
```

Calls accept positional arguments followed by named arguments:

```hgl
smooth(bid, ask, window: 50)
```

## Value-level functions

Local, non-generic value functions and their default temporal lifting are
implemented. See [Value functions and lifting](value-functions.md) for selection,
`const(function)`, testing, and the remaining implementation boundaries.

```hgl
const fn scale(value: f64, factor: f64) -> f64 =>
    value * factor
```

A value-level function executes on values or explicitly admitted views and
returns a value, not a temporal connection. It does not wire topology or
independently tick. A runtime node may call it on current input values; graph
construction may call it on available scalar values when its phase/effect
contract permits. There is no implicit reading of a temporal port's payload
at wiring time.

Function-level `const` does not mean compile-time-only, pure, or immutable.
Effects, mutation, ownership, and allowed lifecycle phases need separate
contracts. A value function cannot contain node declarations or handlers, or
call temporal implementations. Its calls must remain value-level.

Ordinary `fn` still composes a graph or wires a runtime node. A signature with
only `const` parameters does not imply `const fn`: fixed configuration can
describe a source that ticks. Parameter-level `const` continues to mean
wiring-time configuration, not a value argument supplied on each call.

The [design decision](../design/decisions/0008-temporal-contracts-and-target-mappings.md)
includes an HGL caller and expected C++ lowering. Combinations with `impl`,
`native`, and `export`, and migration of existing `native fn` helpers, remain
open syntax/compatibility work. There is no new accepted spelling for them in
this section.

## Parameter packs

> **Implementation status:** Pack signatures, calls, composition and runtime
> traversal are implemented, including cardinality suffixes. `len`, `keys`,
> `types`, `type_at`, and the `each` conjunction are implemented in `requires`,
> as are borrowed runtime schema views for native inspection.

HGL supports three variadic call shapes:

```hgl
fn homogeneous<T>(values: ...T)             # one repeated type, positional only
fn positional<...Ts>(values: ...Ts)         # heterogeneous positional tuple
fn keyword<...Fields>(values: ...{Fields})  # heterogeneous named bundle
```

`homogeneous(1, 2, 3)` binds one `T`; arguments with different source types do
not match. `positional(price, symbol, enabled)` preserves all three distinct
types. `keyword(bid: bid, ask: ask)` additionally preserves the names `bid`
and `ask`.

Positional packs use tuple iteration. The index returned by `items` is a
zero-based `i64`:

```hgl
for value in elements(values) {
    observe(value)
}

for index, value in items(values) {
    observe_at(index, value)
}
```

Named packs use bundle iteration:

```hgl
for name in keys(values) { ... }
for value in elements(values) { ... }
for name, value in items(values) { ... }
```

A pack uses the same spelling and traversal operations in composition and
runtime functions. A composition function may combine positional and named
packs. A runtime function currently accepts only one pack parameter.

Packs accept zero or more arguments unless a cardinality suffix is present:

```hgl
operator merge<T>(values: ...T{1:*}) -> T
operator pairwise<...Ts>(values: ...Ts{2}) -> i64
operator fields<...Fields>(values: ...{Fields}{1:8}) -> i64
```

`{n}` requires exactly `n` arguments, `{n:*}` means at least `n`, and `{n:m}`
is an inclusive range.

Pack types can be inspected in `requires`:

```hgl
requires "price" in keys(Fields)
      && type_at(Fields, "price") in {i64, f64}

requires each T in types(Ts) {
    format_value(T) -> str
}
```

`len`, `keys`, `types`, and `type_at` are compile-time pack reflection. Runtime
code continues to use `elements`/`items` for positional values and
`keys`/`values`/`items` for named values. The `each` form is a compile-time
conjunction: `T` is local to its block, the body must hold for every member,
and an empty pack satisfies it. A generic caller may forward the same premise
using any local binding name; binding names do not affect constraint identity.

`schemas(values)` lets runtime code pass information about pack member types
to native helpers that accept `schema`. Use `elements` or `items` for a
positional pack; `items` yields zero-based indexes. For a named pack, use
`keys`, `values`, or `items`; the keys are the argument names.

A `schema` can only be passed to an approved native helper during that
evaluation. It cannot be stored, returned, captured, compared, or used in
arithmetic. See [native helper authoring](../developer-guide/compiler-and-lowering.md#runtime-pack-schema-views)
for extension integration.

## Public functions

An ordinary named function is visible throughout its module but is not exposed
to other modules unless it is declared with `export`:

```hgl
fn validate_price(value: f64) -> bool =>
    value > 0.0

export fn normalize_price(value: f64) -> f64 =>
    if value > 0.0 { value } else { 0.0 }
```

Other modules may import or qualify `normalize_price`; they cannot name
`validate_price`. Exporting an ordinary function does not create an overload
family. Two exported ordinary functions therefore cannot share a name merely
because their signatures differ.

Operators use a different rule. Declaring an `operator` makes its contract
public automatically. A concrete compatible `impl fn` participates directly;
a generic `impl fn` contributes only the candidates requested by an
`instantiate` declaration, with `_` retaining selected resolver slots.
Candidates are reached through the operator and are
not independently exported exact functions. Consequently `export` is invalid
on an `impl fn`: the binding already supplies its public meaning.

## Temporal and constant parameters

In an ordinary `fn`, an unmodified parameter is temporal:

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

## Generic functions and operators

Generic parameters follow a function or operator name. A plain generic
parameter binds any HGL source type admitted by every place it is used; a
`const` generic parameter binds a wiring-time value that may participate in a
type:

```hgl
operator summarize<
    T,
    const max_size: i64,
    const min_size: i64
>(window: rolling<T, max_size, min_size>) -> T
```

Different generic names bind independently:

```hgl
fn choose_first<U, V>(a: U, b: V) -> U => a
```

Repeating a generic name requires one consistent source-type binding:

```hgl
fn choose_left<U>(a: U, b: U) -> U => a
```

Here `a` and `b` must have the same source type. The parameter context makes
both values temporal; `U` itself is not shorthand for a particular hgraph
time-series wrapper. An unbounded `U` can represent a scalar, container,
structured, atomic, or rolling source type. Using the same `U` in a `const`
position narrows that call to types permitted for constant values, excluding
`atomic` and `rolling`. The same binding rule applies recursively inside type
constructors. In the earlier window example, `T` is the rolling-window element
type and both size parameters are part of the concrete window type.

When a function parameter appears as an argument of a generic struct, that
occurrence narrows it to the struct's canonical-value domain. For example,
`fn unwrap<T>(box: Box<T>) -> T` accepts fully applied `Box` specializations
and binds `T` from the specialization metadata; it does not accept a runtime
`Box<any>`. Generic struct syntax, construction inference, and inheritance are
described in [Types and expressions](types-and-expressions.md#generic-structured-types).

### Requirements and type constraints

A trailing `requires` clause constrains an otherwise generic declaration. It
follows the signature and precedes the body:

> **Staging status:** `hgl check` evaluates closed type sets, `struct`
> admission, field reflection, positive-conjunction equality inference,
> Boolean composition, and nominal operator requirements during
> checking. Implementations are checked against their local or supported imported
> operator contract. Arbitrary residual constant predicates and unsupported
> imported type or constraint forms are rejected.

```hgl
fn add_numeric<U>(a: U, b: U) -> U
requires U in {f64, i64}
=> a + b
```

`in` expresses membership in a closed set of types. Type categories use `is`,
while structural requirements use compile-time reflection predicates:

```hgl
fn calculate<U>(value: U) -> f64
requires U is struct
      && has_fields(U, {"a", "b"})
      && field_type(U, "a") == f64
      && field_type(U, "b") == f64
{
    (value.a + value.b) / 2.0
}
```

`struct` is the canonical structured-value declaration described in
[Types and expressions](types-and-expressions.md#structured-values). The
constraint means that `U` is a nominal struct containing at least the named
fields. `fields(U)` exposes its field-name set when direct reflection is
useful. These are type-level operations, not the runtime `keys` operation used
to traverse collection values. Structural admission does not make two
unrelated nominal structs assignment-compatible.

A type equality can both infer and validate a substitution:

```hgl
operator get_field<U, V>(value: U, const name: str) -> V
requires U is struct
      && name in fields(U)
      && V == field_type(U, name)
```

If `U` and `name` are known, the last requirement resolves `V`. If an expected
output has already bound `V`, it checks that binding instead. A requirement
that cannot make progress because its inputs are unresolved or cyclic is a
compile-time error.

Requirements may also state that a nominal operator must be callable for the
substitution:

```hgl
use hgraph.std::{add_}

fn double<U>(value: U) -> U
requires add_(U, U) -> U
=> value + value
```

The body is valid only when the system `add_` contract has an implementation
for two `U` inputs producing `U`. A qualified operator such as
`math::add(U, U) -> U` names that exact nominal contract; a body relying on
that local contract calls `math::add(value, value)` explicitly. It does not
rebind the system `+` symbol.

Requirements are evaluated while the graph is wired. They never become
per-tick conditionals. Every generic needed by a selected implementation must
resolve from call arguments, the expected output, or a solvable equality
requirement. An unresolved or inconsistently rebound generic is a type error.

Explicit generic application is agreed for struct types and their constructors
(`Box<f64>` and `Box<f64>(...)`). Its spelling on ordinary function and
operator calls remains open; those calls continue to infer their generic
bindings in the initial design.

`U` is a type resolved while wiring, not a dynamically typed `any`.
The same binding must satisfy every occurrence of `U` in the signature.

An `operator` is a nominal contract, similar in role to a Rust trait or Swift
protocol. It declares a call shape and generic relationships but has no body:

```hgl
operator combine<T>(lhs: T, rhs: T) -> T
```

An operator may carry requirements when they are part of its public contract:

```hgl
operator choose_number<U>(lhs: U, rhs: U) -> U
requires U in {f64, i64}
```

Every implementation is checked with the operator requirements in scope and
may add stricter candidate requirements. At dispatch, the effective constraint
is the operator requirement combined with the candidate requirement. A
candidate does not need to repeat the public constraint merely to use its
guarantees in the body. Requirements introduced only because of an algorithm
belong to that implementation, not the operator. For example, an
addition-based implementation of `double` is:

```hgl
operator double<U>(value: U) -> U

impl fn double<U>(value: U) -> U
requires add_(U, U) -> U
=> value + value
```

The contract does not require addition. A different candidate can implement
the same operation using multiplication, for example
`impl fn double(value: f64) -> f64 => value * 2.0`. These are alternative
implementation strategies; their implementation-only constraints do not
restrict one another or become part of the public operator contract.

An operator is public by definition; there is no `export operator` form.

The contract owns its parameter names, temporal-versus-`const` roles, defaults,
and result relationship. An `impl fn` with the operator's name contributes a
compatible implementation candidate:

```hgl
impl fn combine(lhs: f64, rhs: f64) -> f64 =>
    lhs + rhs

impl fn combine(lhs: i64, rhs: i64) -> i64 =>
    lhs + rhs
```

The `impl` modifier is required. It makes the relationship visible at the
declaration, and it turns a misspelt implementation name into an error instead
of a quietly unrelated function: `impl fn combne` is rejected because no
operator `combne` is in scope. Conversely, a plain `fn combine` beside an
operator `combine` is a name conflict, not a silent candidate.

An implementation may be concrete or generic, but it must specialize the
operator contract rather than change its public argument roles. Its body is
classified normally: an ordinary body becomes graph composition, while
node-only constructs make that candidate a runtime implementation.

The contract is the minimum an implementation meets. An implementation
declares every contract parameter, in order, and may declare more after
them, with or without defaults. A call through the operator may pass those
extra arguments, by name or by position after the contract's parameters:

```hgl
operator pick<T>(value: T) -> T

impl fn pick(value: f64) -> f64 => value
impl fn pick(value: i64, const scale: i64) -> i64 => value * scale

fn tripled(value: i64) -> i64 => pick(value, scale: 3)
```

An implementation without a parameter for an extra argument does not match
the call. Nor does one whose extra parameter has no default and is not
supplied. `pick(value)` with an `i64` therefore reaches no implementation.
An extra argument that none of a module's own implementations accepts is
reported as an error naming the operator and the argument.

### Explicit implementation materialization

A generic `impl fn` is a source template, not an open-ended candidate placed in
the runtime registry. The module requests each implementation candidate it
needs with `instantiate`:

```hgl
operator absolute<T>(value: T) -> T

impl fn absolute<T>(value: T) -> T
requires T in {i64, f64}
=> if value < 0 { -value } else { value }

instantiate absolute<i64>, absolute<f64>
```

The arguments after `absolute` bind the generic parameters declared by the
`impl fn`, in order. They may include type arguments and wiring-time constant
arguments:

```hgl
operator summarize<T, const size: i64>(values: rolling<T, size>) -> T

impl fn summarize<T, const size: i64>(values: rolling<T, size>) -> T =>
    mean(values)

instantiate summarize<f64, 20>
```

Use `_` when a generic should remain selectable by overload resolution instead
of being fixed by this declaration. A reduction often needs a concrete element
type but does not care about the fixed size of its input list:

```hgl
operator sum_<T, const size: i64>(values: list<T, size>) -> T

impl fn sum_<T, const size: i64>(values: list<T, size>) -> T
requires T in {i64, f64}
{
    when {
        var total: T = 0
        for value in elements(values) {
            total += value
        }
        return total
    }
}

instantiate sum_<i64, _>, sum_<f64, _>
```

The two candidates have concrete accumulator types but still match any fixed
list size. The retained `size` participates in matching the input type; this form does
not make its numeric value available inside the function body.

A retained `_` argument can participate in matching, but the function body
cannot currently read its value. Bind a concrete value when the implementation
needs to use it in a calculation.

Each concrete argument binds the corresponding implementation generic in
declaration order; `_` retains it. Each request is checked against the
implementation signature, its `requires` clause, and the operator contract. A
request that matches no generic template, or repeats the same candidate
pattern, is a type error. Requirements over retained slots must currently be
decidable from the concrete bindings without constraining a retained slot.
When several
generic templates of the same operator accept the argument list, each matching
template is materialized; hgraph still applies its ordinary overload ranking
when the operator is called.

`instantiate` affects candidate generation, not source visibility. The generic
template and its requested candidates remain hidden behind the public
operator contract. A generic implementation with no materialization is valid
source but contributes no generated candidate.

> **Current compiler boundary:** explicit materialization is implemented for an
> operator declared locally or selectively imported from a module with a supported
> contract. See [separate implementations](modules-and-tools.md#compiling-a-separate-implementation).

Operator identity is nominal and includes its defining module. Two modules may
therefore declare unrelated operators with the same short name. Name and import
resolution first select one operator contract; only then does hgraph rank the
implementations belonging to that contract. Concrete or otherwise more
specific candidates win according to hgraph's resolver. Equal-ranked matching
candidates are an ambiguity error, never a declaration-order choice.

A `fn` without `impl` declares an ordinary function. Ordinary functions do
not form an overload set merely by repeating their name. See
[Modules and tools](modules-and-tools.md) for implementation discovery,
imports, and qualified operator calls.

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

An anonymous function closes over readable locals and `const` parameters.
Whether a general anonymous function may mutate a captured `var`, and the
binding rules for captured temporal values, remain to be specified with
higher-order function semantics. Runtime iterator predicates are the narrower
case already defined: they may capture a `var` for reading but are pure and
cannot mutate it.

## Outputless functions

An outputless function omits the return arrow:

```hgl
fn observe(price: f64, const label: str = "price") {
    debug_print(label, price)
}
```

A call used as a statement must resolve to an outputless contract. Silently
discarding a temporal result is an error.

## Composition and runtime functions

The current design uses body constructs to classify an ordinary temporal
`fn` as composition or a runtime node. The agreed `const fn` extension
separately identifies value-level functions; it does not replace this rule.

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

The activation and admission predicates have defaults. A missing `modified`
term means any temporal input may activate the handler; a missing `valid` term
means every temporal input must be valid. Empty calls make those sets explicit:
`modified()` is any input and `valid()` is all inputs. The compact form uses
both defaults:

```hgl
fn add(a: f64, b: f64) -> f64 {
    when {
        return a + b
    }
}
```

It is equivalent to `when modified() && valid() { ... }`. Explicit arguments
still narrow their respective predicate, so `when modified(a) && valid(a)`
does not require `b` and does not activate for changes to `b`.

| Handler | Activation | Validity admission |
| --- | --- | --- |
| `when { ... }` | Any temporal input | Every temporal input |
| `when modified() && valid() { ... }` | Any temporal input | Every temporal input |
| `when modified(a) { ... }` | `a` | Every temporal input |
| `when valid(a) { ... }` | Any temporal input | `a` |
| `when modified(a, b) && valid(a) { ... }` | `a` or `b` | `a` |

These defaults apply to temporal function parameters, not `const` parameters,
state, injectables, or `out`. `valid()` is not recursive for structural inputs;
use `all_valid(value)` when every child must be valid. Empty selector calls are
valid only in a function-level `when` predicate. `scheduled()` selects scheduler-driven activation without an input trigger.
There is no general spelling for “no validity requirement”; `valid()` selects
the complete temporal input list.

`when` is a function-level handler rather than a nested control-flow form.
Use `if valid(value) { ... }` inside a handler when only part of that handler
needs a value. Validity guards follow normal left-to-right short-circuit order,
so place `valid(value)` before reading `value` in the same `&&` condition.

Under the [iteration model](../design/iteration.md), `for` follows the
containing function's phase. During graph construction, fixed temporal lists
provide their child connections. Independent bodies over dynamic maps and
unbounded lists apply separately to each live key or index, and may capture
temporal inputs. During evaluation, traversal reads current children or scalar
elements. Loop-carried reductions, graph iterator predicates, `const` captures
in dynamic graph loops, escaping assignments, and loop returns are unsupported.
Scalar wiring-time iterables are not implemented yet.

## Conditional control flow

Status: partially implemented. Temporal conditions support branch values,
outputless branches, early returns, and assignments to variables declared
before the conditional. Several variables may be assigned together. A branch
may retain a variable's incoming binding; otherwise every path reaching a later
read must assign it. A value-producing temporal `if` without `else` produces
no ticks while its condition is false. Embedded expressions such as
`(if c { x } else { y }) + 1` are supported. Scalar branch captures and
temporal `else if` are unsupported.

`if` has three context-dependent meanings:

| Context | Behavior |
| --- | --- |
| Graph function, wiring-time Boolean condition | Wire the selected branch once. |
| Graph function, temporal Boolean condition | Run only the selected branch, changing the active branch when the condition changes. |
| Node evaluation, including `when` | Execute the selected branch using current readable values. |

For a temporal condition in a graph, only the selected child graph runs. A
change of condition stops the old branch and starts the new one. The surrounding graph still composes at wiring time.
Computations wired outside the branches retain their own lifetimes.

This differs from calling `if_then_else` on already-wired outputs, whose
upstream computations remain independently active. A value-producing temporal
`if` without `else` produces no ticks while false, and supplies no default
value. This works in scripted and compiled modes; see
[conditional-omitted-else.hgl](../../examples/conditional-omitted-else.hgl).
An escaping variable must be declared before the conditional. For example:

```hgl
fn use_conditional_result(condition: bool, x: i64, y: i64) -> i64 {
    var r: i64
    if condition {
        r = x + 1
    } else {
        r = y - 1
    }

    return r * 2
}
```

After the conditional, `r` follows the connection chosen by the active branch.
The multiplication continues to use that selected connection. This works for
one or several variables; see
[conditional-result.hgl](../../examples/conditional-result.hgl) and
[conditional-results.hgl](../../examples/conditional-results.hgl).
Branch-local declarations do not escape. A declaration without an initializer
supplies no default value or connection, so both branches above must assign `r`.

If `r` already has a binding before the conditional, a branch that leaves it
unchanged forwards that incoming binding, including the false branch when
`else` is omitted. It forwards the pre-conditional connection, not remembered
state from a previously selected branch. The variable retains its declared
type, including an explicit `ref<T>` type. See
[conditional-forwarding.hgl](../../examples/conditional-forwarding.hgl).

Every escaping variable must have a binding on every path reaching its use:
either a prior binding to forward or an assignment on that path. Otherwise
its use is a compile-time definite-assignment error. A type annotation alone
is insufficient, and an unassigned branch does not receive an automatic
never-ticking source. This checks whether a connection exists, not whether
its time series currently has a valid value.

An explicit `return` inside a temporal branch returns from the enclosing HGL
function. If one path returns early, the remaining function body becomes the
other path's continuation inside its switch branch. Nodes and sinks composed
there share that branch's lifetime; computations composed before the `if`
remain outside. This does not permanently stop graph execution: later
condition changes can still select either branch.

Capture analysis includes the continuation's dependencies. Definite assignment
checks only paths that reach a use; an earlier-returning path need not assign
a variable used solely in the continuation. See the
[early-return example](../design/control-flow.md#early-returns-and-continuations).

An outputless temporal conditional wires its sinks through the switch. In
`if enabled { debug_print("enabled", value) }`, the `"enabled"` sink is wired
through the selected branch when enabled. A subsequent
`debug_print("always", value)` outside that conditional is always wired into
the enclosing graph. The graph body describes the wiring; the sink nodes
perform the printing. With no `else`, the false path contributes no conditional
sink. The conditional has no output or escaping binding to remap.

This form is implemented in scripted and compiled modes. See the runnable
[conditional-sinks.hgl](../../examples/conditional-sinks.hgl) example.
The example also returns a value after a discarded sink conditional, showing
that the conditional does not inherit the enclosing function's result type.
Temporal `else if` is not supported yet;
use a block `else` in the current compiler.

Whether the switch needs an output depends on the conditional's results and
escaping variables, not on whether the enclosing function is outputless. See
[Outputless conditionals](../design/control-flow.md#outputless-conditionals).

A conditional can supply an expression result while also assigning escaping
variables. The final expression in each branch supplies the expression result;
assignments supply the escaping bindings. Count them together: zero results
need no output, one is returned directly, and multiple results use a generated
bundle. Its fields are remapped to the expression consumer and enclosing
variables. A binding initialized from the complete `if` expression is not
itself an escaping variable and needs no prior declaration. See the
[mixed-result design](../design/control-flow.md#expression-results-and-escaping-assignments).
This form runs in both scripted and compiled modes; see
[conditional-mixed-results.hgl](../../examples/conditional-mixed-results.hgl).

See
[Conditional control flow](../design/control-flow.md) for the agreed strategy,
single- and multiple-result examples, capture/signature derivation, Arrow
precedent, forwarding of existing bindings, definite-assignment and early-return
examples, outputless conditional wiring, and mixed expression/assignment
results.

## Explicit switch

Status: the following source form and node-style/graph-style semantics are
agreed design; compiler implementation remains separate work. See
[Explicit switch dispatch](../design/switch.md).

```hgl
fn select_result(mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    var r: i64
    switch mode {
        case 0:
            r = x + 1
        case 1:
            r = y - 1
        default:
            r = fallback * 3
    }

    return r * 2
}
```

Case values must be expressible as constants in source and compatible with
the selector's key type. Literals and named constants follow the existing
constant-value rules; time-series values and state reads cannot be case
labels. A case body continues until the next label or closing switch brace,
with no implicit fallthrough and no `break` needed. An explicit empty
`default:` is allowed. [Enum support](../design/type-extensions.md#enum-types)
uses the agreed declaration/member form, with case labels such as
`case Mode::first:`. Explicit/automatic numbering, member-name stringification,
and rejection of duplicate enum numbers are agreed. String conversion uses
`str(value)`; remaining enum type rules are separate design work.

In a node-style function, switch dispatch uses the current readable selector
value within that evaluation, preserving the function's state. In a graph
function, a wiring-time selector chooses one branch during construction; a
temporal selector changes which branch runs over time.

Graph branches reuse the `if`/`else` rules for captured inputs, reference
forwarding, escaping bindings, matching result types, definite assignment,
and early returns. The default branch participates in all of those checks.
One matching branch is selected, without implicit fallthrough between bodies.

`default: ...` handles values matching no explicit case. With no match and no
default, fail during wiring or evaluation as appropriate to the selector's
phase. Do not invent a never-ticking result or silently continue. Default is
not an exception handler and does not permit reading an invalid selector.

The [paired HGL/C++ scenarios](../developer-guide/control-flow-cpp-mappings.md)
show node-style `when` handlers, wiring-time selectors, temporal selectors,
multiple results, early returns, sinks, and state lifetime.

## State

`state` declares mutable data that persists across evaluations:

```hgl
fn accumulator(a: f64, b: f64) -> f64 {
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

Each handler writes `out` instead of returning. A `return` in the first
handler would end the evaluation before the second ran, so a tick on which `a`
and `b` both changed would lose the `b` update: `modified(b)` is false again
on the next tick. Use `return` in an ordered handler only when later handlers
are meant to be skipped.

State declarations are function-level declarations. Initializers run during node startup and do not overwrite state restored for
record/replay. Use `state` for history that must survive recovery.

`let` is an immutable lexical binding and `var` is a mutable lexical binding.
Either one declared inside `when` exists only for that evaluation; `var` does
not become persistent merely because it is mutable. Use `state` when later
computation depends on retained history.

### Reconstructible cache

`cache` declares node-local data that is **not** part of record/replay. It is
declared and used like `state`, but its initializer runs on every start,
restored or not:

```hgl
fn scale(value: f64, const factor: f64) -> f64 {
    cache multiplier: f64 = factor
    when {
        return value * multiplier
    }
}
```

Use `cache` for data reconstructible from authoritative current inputs and
restored state without missing history. Rebuilding must preserve subsequent
values, validity, ticks, deltas and effects. Historical counters, running totals
and unconsumed events belong in `state`. In particular, pending schedules and
finite-schedule progress must survive recovery; current native parity alone
does not establish that guarantee.

Multiple scalar cache variables are supported, and a function may declare
`cache` and `state` together: the node then carries both storages, a restored
state keeps its value, and every cache is rebuilt by `start` -- so a cache may
take its value from restored state. Initializers run in declaration order, so
one may name an earlier declaration of either kind. Non-scalar caches and
generic recordable state initialization remain unsupported. See
[ADR 0011](../design/decisions/0011-cache-declarations.md) for the recovery
contract and current limits.

## Injectables

`inject` requests approved hgraph runtime capabilities without adding them to
the callable signature:

```hgl
inject out, logger, clock, scheduler
```

All four are implemented: `out` and `logger` since the first runtime slice,
`clock` and `scheduler` under
[ADR 0010](../design/decisions/0010-lifecycle-capabilities.md).

The comma-separated form may span lines and may have a trailing comma:

```hgl
inject
    out,
    logger,
    clock,
    scheduler,
```

Capabilities are function-level declarations at the same level as `state`.
Calling a value helper silently adds its required injectables to the caller's
list, transitively and without duplicates. This includes native descriptor
imports. Explicit duplicate declarations, unknown capabilities and unsupported
phases remain errors; an omitted caller declaration is not an error.
Any other name is rejected as unapproved, and `out` requires a function
output. Reading or writing `out` inside `start` or `stop` is rejected while
lifecycle output access remains an open question.

## Scheduling, the clock, and input activity

`inject clock` gives the evaluation clock: `clock.evaluation_time()` is the
engine time of the current cycle, `clock.now()` the wall clock, and
`clock.next_cycle_evaluation_time()` the earliest time of the next cycle. All
three return a `datetime`.

`inject scheduler` gives the node scheduler. `scheduler.schedule(delay)`
requests an evaluation `delay` after the current evaluation time;
`scheduler.schedule_at(time)` requests one at a `datetime`. A second `bool`
argument selects the wall clock, which only a real-time executor accepts.
`scheduler.is_scheduled()` and `scheduler.next_scheduled_time()` inspect the
pending alarm. In `start`, `scheduler.schedule(0s)` asks for evaluation in the
starting cycle; that is how a node schedules itself on start.

`scheduled()` is a handler selector: it is true when the current evaluation
is the node's own alarm firing. A handler whose condition names `scheduled()`
at top level gets no implicit `modified()`, and it adds no input to the node's
activation. When no handler names an input, the node's activation set is
explicitly empty and the node evaluates only when scheduled. A runtime
function with no temporal parameters at all is a source and must inject
`scheduler`:

```hgl
fn ticker(const delay: duration, const max_ticks: i64) -> i64 {
    state ticks: i64 = 0
    inject scheduler

    start {
        if ticks < max_ticks {
            scheduler.schedule(0s)
        }
    }

    when scheduled() {
        ticks += 1
        if ticks < max_ticks {
            scheduler.schedule(delay)
        }
        return ticks
    }
}
```

`passivate(input)` stops a temporal parameter from activating the node;
`activate(input)` lets it again. Both are runtime statements whose argument
is a direct parameter of the function, not a projection. They are available
only during evaluation; `start` and `stop` cannot access temporal inputs.
A passive input keeps its value and validity and can still be read:

```hgl
fn first_ticks(value: i64, const count: i64) -> i64 {
    state seen: i64 = 0

    when modified(value) && valid(value) {
        if seen >= count {
            passivate(value)
        } else {
            seen += 1
            if seen >= count {
                passivate(value)
            }
            return value
        }
    }
}
```

See [`lifecycle-capabilities.hgl`](../../examples/lifecycle-capabilities.hgl).

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

An input named by any handler can activate the function. Each handler still
checks its own condition in source order; an input need not satisfy another
handler's validity requirement for its own handler to run.

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

Collection outputs support typed functional mutations. The first argument is
always the injected output:

```hgl
fn latest_by_key(key: str, value: f64) -> map<str, f64> {
    inject out

    when modified(value) && valid(key, value) {
        upsert(out, key, value)
    }
}
```

Use `insert` when absence is required, `update` when presence is required, and
`upsert` when either state is acceptable. Sets support `insert` and `upsert`;
they do not need a separate `update` because membership has no child value.
`remove` requires the member or key to exist, while `discard` silently does
nothing when it is absent. `invalidate(out, key)` keeps a map key but
invalidates its child, which is different from removing the key.

An unbounded list is a stack-shaped mutable output:

```hgl
fn collect_values(value: i64) -> list<i64, unbounded> {
    inject out

    when {
        push(out, value)
    }
}
```

`push` appends and initializes one trailing child. `pop` removes the trailing
child and requires a non-empty list. `clear` is available for sets, maps, and
unbounded lists. Indexed `invalidate(out, index)` preserves list length and
never grows the list. Fixed lists do not support `push`, `pop`, or `clear`.

Whole-output assignments are last-write-wins. Writes to different collection
children accumulate into one output delta; repeated writes to the same child
use the last value. Strict preconditions observe mutations already staged in
the current evaluation. Removal, invalidation, and a scalar `null` value remain
different effects. `inject out` is invalid on an outputless function and `out`
is initially restricted to evaluation code rather than `start` or `stop`.

Once some other node-only construct classifies a function as runtime, omitting
`when` uses hgraph's ordinary policy: any temporal input can activate it and
all ordinary inputs must be valid. `when` is then needed only to customize
activation, validity, or ordered conditional handling. A function whose only
runtime operation is collection traversal cannot yet select this phase without
also using an existing node-only construct. The explicit phase-disambiguation
syntax, if any, remains to be designed.

Non-scalar cache storage remains unsupported. Calls to reusable
value-level helpers use the `const fn` execution role; they
must not be confused with calls that would wire a temporal function during
node evaluation.

Operator calls are implementation-neutral: after source name resolution chooses
one nominal contract, hgraph's overload registry may select a graph or native
node implementation without changing the call site.
