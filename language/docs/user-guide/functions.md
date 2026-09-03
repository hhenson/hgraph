# Functions

`fn` is the only user-facing implementation declaration. Source does not label
an implementation as a graph or node. A bodyless `operator` declaration names
a generic callable contract whose implementations are supplied by `impl fn`
definitions.

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

Parameters are immutable. `let` introduces an immutable local and `var`
introduces a mutable local; either may use an inferred type:

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
public automatically, and every compatible `impl fn` of that name participates
as an implementation candidate. The candidate is reached through the operator
and is not an independently exported exact function. Consequently `export` is
invalid on an `impl fn`: the binding already supplies its public meaning.

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

### Requirements and type constraints

A trailing `requires` clause constrains an otherwise generic declaration. It
follows the signature and precedes the body:

> **Staging status:** this syntax and its semantics are agreed design, but the
> current `hgl check` parser does not implement `requires` yet.

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
{
    // The definition of struct values and field access is specified separately.
    0.0
}
```

`struct` is the working name for the canonical structured-value concept. Its
declaration and field-access design is intentionally left to the dedicated
struct design; the constraint means that `U` is such a type and contains at
least the named fields. `fields(U)` exposes its field-name set when direct
reflection is useful. These are type-level operations, not the runtime `keys`
operation used to traverse collection values.

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
fn double<U>(value: U) -> U
requires add(U, U) -> U
=> value + value
```

The body is valid only when the selected `add` contract has an implementation
for two `U` inputs producing `U`. A qualified operator such as
`math::add(U, U) -> U` names that exact nominal contract.

Requirements are evaluated while the graph is wired. They never become
per-tick conditionals. Every generic needed by a selected implementation must
resolve from call arguments, the expected output, an explicit generic
argument, a declared default, or a solvable equality requirement. An
unresolved or inconsistently rebound generic is a type error.

`U` is therefore a wiring-time unknown, not a dynamically typed `any`. The
compiler may implement a generic runtime function once using hgraph's erased
views when every operation in its body is valid for all admitted `U` values,
or specialize it when concrete representation is required. That
code-generation choice does not change the source-level type relationships.

An `operator` is a nominal contract, similar in role to a Rust trait or Swift
protocol. It declares a call shape and generic relationships but has no body:

```hgl
operator combine<T>(lhs: T, rhs: T) -> T
```

An operator may carry requirements as part of its public contract:

```hgl
operator double<U>(value: U) -> U
requires add(U, U) -> U
```

Every implementation is checked with the operator requirements in scope and
may add stricter candidate requirements. At dispatch, the effective constraint
is the operator requirement combined with the candidate requirement. A
candidate does not need to repeat the public constraint merely to use its
guarantees in the body.

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

A function containing `state`, `inject`, `start`, `when`, `stop`, or a runtime
collection iterator describes runtime evaluation and is compiled as one node:

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

State declarations are function-level declarations. All state variables in a
function are aggregated into one typed state value. Initializers run during
node startup and do not overwrite state restored for record/replay. State that
affects later ticks is recordable by default.

`let` is an immutable lexical binding and `var` is a mutable lexical binding.
Either one declared inside `when` exists only for that evaluation; `var` does
not become persistent merely because it is mutable. Use `state` whenever the
next evaluation must observe the value.

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

Operator calls are implementation-neutral: after source name resolution chooses
one nominal contract, hgraph's overload registry may select a graph or native
node implementation without changing the call site.
