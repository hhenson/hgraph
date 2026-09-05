# Types and expressions

Source types describe canonical values and structures. Temporal context is
supplied by the function signature, so authors do not write `ts`, `tsb`,
`tsl`, `tss`, `tsd`, or `tsw` wrappers.

## Scalar value types

The initial scalar vocabulary is:

| Type | Meaning | Literal |
| --- | --- | --- |
| `bool` | Boolean | `true` |
| `i64` | Signed 64-bit integer | `20` |
| `f64` | 64-bit floating-point value | `2.0` |
| `str` | UTF-8 string | `"bid"` |
| `date` | Calendar date | `@2026-09-03` |
| `time` | Time of day | `@09:30` |
| `datetime` | Instant on the UTC timeline, the engine clock type | `@2026-09-03T09:30Z` |
| `duration` | Signed elapsed time | `5m` |
| `civil_datetime` | Wall-clock date and time, no zone | `@2026-09-03T10:30` |
| `timezone` | Named TZDB zone | `@[Europe/London]` |
| `zoned_datetime` | Instant with its zone and offset | `@2026-09-03T10:30+01[Europe/London]` |
| `zoned_time` | Time of day in a named zone | `@09:30[America/New_York]` |

In an ordinary parameter or result position, a scalar type is an atomic
time-series leaf. In a `const` parameter position, it is a wiring-time scalar.

```hgl
fn scale(value: f64, const factor: f64) -> f64 =>
    value * factor
```

## Temporal values

The temporal scalars are hgraph's RFC 0002 core types. `date` and `time` are
civil values, `datetime` is an instant on the UTC timeline, `duration` is
elapsed time in microseconds with no month or year component, and
`civil_datetime` is a wall-clock reading that means a moment only once a zone
interprets it. Zones are named: `timezone` is a TZDB name, `zoned_datetime`
is an instant together with its zone and the offset the zone had at that
instant, and `zoned_time` is a time of day in a zone, such as a market open,
whose offset depends on the day.

A `@` literal is written in RFC 3339 form, with the zone in brackets as in
RFC 9557, and the shape selects the type; seconds may be omitted. A number
directly followed by a unit (`d`, `h`, `m`, `s`, `ms`, `us`) is a duration,
and several units may run together in descending order:

```hgl
const session_open: zoned_time = @09:30[America/New_York]
const close: time = @16:00
const expiry: date = @2026-12-18
const epoch: datetime = @2026-01-01T00:00Z
const reported: civil_datetime = @2026-09-03T10:30
const fixing: zoned_datetime = @2026-09-03T10:30+01[Europe/London]
const venue: timezone = @[Europe/London]
const cooldown: duration = 1h30m
const settle: duration = 2m30.5s
```

Literals are checked when they are read: `@2026-02-29` is not a date,
`@24:00` is not a time, `0.5us` is not a whole number of microseconds, and
`30m1h` is out of order. `m` is minutes. A date and time without `Z` or an
offset is a `civil_datetime`, never an instant read as UTC, so assigning it
to a `datetime` is a type error. A `zoned_datetime` literal always carries
its offset, which is how the two occurrences of a repeated hour are told
apart; to interpret a wall-clock value in a zone, call `resolve` with explicit
policies for repeated and skipped times. The run's time-zone provider checks
that a zone exists and that a literal's offset agrees with it.

Arithmetic follows hgraph: `datetime ± duration`, `date ± duration`,
`civil_datetime ± duration`, and `zoned_datetime ± duration` keep their type,
`datetime - datetime`, `date - date`, and `civil_datetime - civil_datetime`
produce a `duration`, `date + time` is a `civil_datetime`, and
`date + zoned_time` is a `zoned_datetime` with that day's offset (on a
transition day where the time is repeated or skipped it raises rather than
guessing; `resolve` takes policies). Durations add, subtract,
negate, scale by a number written after them (`cooldown * 2`, not
`2 * cooldown`), and divide by each other into an `f64`. Values of one
zone-free type compare chronologically; zoned values compare only for
equality, structurally, and `same_instant`, `to_instant`, and `to_civil`
give the timeline and civil views. `date` arithmetic uses the whole-day part
of a duration, so `expiry + 36h` is the next day. There are no implicit
conversions between the types and no `time ± duration`, `zoned_time ±
duration`, or `datetime + datetime`. Field accessors such as `year`, `hour`,
`total_seconds`, and `zone_of` and the zone conversions `at_zone`,
`convert_zone`, and `resolve` are ordinary library functions.

## Recursive temporalization

Canonical containers and structured values become structural time-series
shapes recursively:

| Source type | Temporal interpretation |
| --- | --- |
| `f64` | Atomic endpoint carrying `f64` |
| `duration` | Atomic endpoint carrying a `duration` |
| `zoned_time` | Atomic endpoint carrying a `zoned_time` |
| `tuple<f64, f64>` | Structural tuple with positional temporal children |
| `list<f64>` | Unbounded structural list of temporal `f64` values |
| `list<f64, 3>` | Structural list of exactly three temporal `f64` values |
| `set<str>` | Set-valued time series of `str` members |
| `map<str, f64>` | Keyed temporal map from `str` to temporal `f64` |
| `rolling<f64, 20, 5>` | Rolling window of at most 20 `f64` values, valid from 5 values |
| `rolling<f64, 5m>` | Rolling window of the last five minutes of `f64` values |
| a structured value type | Structural bundle whose fields are temporal |

A structural tuple is hgraph's un-named bundle with positional fields, so its
children may have different types and `pair[0]` is positional field access. A
homogeneous `tuple<f64, f64>` is still a tuple, not a two-element list: tuples
are accessed by position, lists are sized and traversed.

## List sizes

A temporal list is unbounded unless it carries a size:

```hgl
list<f64>              // unbounded
list<f64, 3>           // exactly three elements
list<f64, unbounded>   // the same as list<f64>
```

`unbounded` is the sentinel size. A generic `const` size binds whatever size
the argument has, including `unbounded`, so one function can accept both
forms:

```hgl
fn head<T, const n: i64>(entries: list<T, n>) -> T =>
    entries[0]
```

`list<T>` in a parameter position accepts a list of any size; `list<T, 3>`
accepts only a three-element list. The resolved size is part of the type
identity, and an unbounded list is the only one whose elements can be added
and removed after wiring.

## Structured values

> **Implementation status:** declarations, type-generic applications,
> abstract-only single inheritance, construction, optional fields, and sparse
> delta syntax are implemented. Constructor inference, typed `const` generic
> metadata, explicit optional-field clearing, and the remaining nested/runtime
> lowering cases fail closed as listed in the developer roadmap.

A `struct` declares one nominal structured type. It is module-internal unless
it is exported, and its fields are public and immutable:

```hgl
export struct Quote {
    bid: f64
    ask: f64
    venue: str = null
    currency: str = "USD"
}
```

The nominal identity is the module-qualified name, so two separately declared
structs with equal fields remain different types. Field declaration order is
stable schema metadata, but construction uses names rather than positions.
`export struct` exposes the name to other modules in the same way that
`export fn` exposes an ordinary function.

### Abstract data families

An abstract struct defines common data for a polymorphic family. Only an
abstract struct may be inherited, and every concrete struct is implicitly
final:

```hgl
export abstract struct Instrument {
    symbol: str
    venue: str = null
    currency: str = "USD"
}

export struct EuropeanInstrument: Instrument {
    venue = "XLON"
    currency = "GBP"
}

export struct GenericInstrument: Instrument {}
```

An abstract struct cannot be constructed. A concrete child may add no fields,
as `GenericInstrument` does, when its nominal identity is the only additional
information. An abstract struct may inherit other abstract structs, and a
concrete child may implement more than one abstract data aspect. Structs do
not contain methods, so there is no behavior inheritance or method-resolution
order.

The declaration that introduces a field fixes its canonical type and whether
the field is optional for the entire family. A child cannot redeclare that
field or change its optionality. An assignment without a type annotation in a
child changes only the inherited construction default:

```hgl
export abstract struct Request {
    timeout: duration
    venue: str = null
}

export struct StandardRequest: Request {
    timeout = 30s
    venue = "primary"
}
```

Here `timeout` remains non-optional but callers of `StandardRequest` may omit
it because the child supplies a default. `venue` remains optional even though
its child default is non-null, so `venue: null` is still valid when constructing
a `StandardRequest`. A descendant may introduce or replace a default, but the
initial design has no syntax for removing one. `null` may be a descendant
default only for a field that was originally declared optional.

When multiple abstract parents contribute the same field name, its type and
optionality must agree. Equal defaults merge. Different defaults, or a default
from only one of otherwise compatible parents, require the child to choose an
explicit default. A type or optionality conflict is always an error. The exact
stable ordering rule for fields contributed by multiple parents will be fixed
before this syntax is implemented.

A scalar or `atomic<Abstract>` value carries one concrete final member of the
family and preserves its concrete type. The compiled target's module closure
defines the available concrete members, and a graph captures that closed set
when it is wired. A temporal abstract type exposes the fixed bundle of fields
declared by its abstract family; converting a temporal concrete bundle to that
base view is explicit because silently inserting a projection would add graph
work. The spelling of that projection remains to be selected.

### Generic structured types

A struct may declare type parameters and wiring-time constant parameters using
the same parameter-list syntax as a function or operator:

```hgl
export struct Box<T> {
    value: T
}

export struct Pair<K, V> {
    first: K
    second: V
}

export struct Vector<T, const size: i64> {
    values: list<T, size>
}
```

Every applied type is a distinct invariant nominal specialization. For
example, `Box<i64>` and `Box<f64>` are different types, and
`Box<Derived>` is not a subtype of `Box<Base>` even when `Derived` implements
`Base`. A generic type must be fully applied in a type annotation:

```hgl
const box: Box<f64>
const vector: Vector<f64, 3>
```

A bare `Box`, an unresolved argument, and partial application such as
`Pair<_, str>` are errors. Generic parameter defaults are not part of the
initial design, so explicitly applying a type supplies every argument.

Struct constructors may instead infer the complete argument list from their
named fields and expected type:

```hgl
let inferred = Box(value: 1.5)              // Box<f64>
let explicit = Box<f64>(value: 1.5)
let expected: Box<f64> = Box(value: 1.5)
```

Inference unifies every occurrence of a parameter. An expected result and the
supplied fields may contribute bindings, including a fixed list field binding
a `const` size parameter. All parameters must resolve consistently. A
constructor with no evidence needs an expected type or explicit arguments:

```hgl
struct Maybe<T> {
    value: T = null
}

let empty: Maybe<f64> = Maybe()
let also_empty = Maybe<f64>()
let ambiguous = Maybe()                    // error: cannot infer T
```

A generic struct may use the existing `requires` language. The requirements
are checked whenever a specialization is formed:

```hgl
export struct Range<T>
requires T in {i64, f64}
{
    lower: T
    upper: T
}
```

`Range<str>` is therefore invalid. Type sets, categories, reflection,
equalities, constant predicates, and nominal operator requirements have the
same meaning here as on a generic function.

> **Staging status:** The prototype currently performs complete requirement
> evaluation when it checks a struct constructor. Other appearances of an
> applied generic struct retain the normalized requirement in typed HIR, but
> validation there remains fail-closed until canonical nominal-type metadata is
> available to every consumer. Arbitrary constant expressions beyond equality
> and closed-set membership are also still deferred.

In the initial design, a struct's type arguments are canonical value types,
not temporal policies. Put `atomic` at the field where temporal expansion is
controlled:

```hgl
struct LiveBox<T> {
    value: T
}

struct SnapshotBox<T> {
    value: atomic<T>
}
```

`LiveBox<atomic<Quote>>` and `LiveBox<rolling<f64, 20>>` are rejected. This
ensures that one specialization has one canonical Bundle schema and one
deterministic temporal expansion. Generic functions remain broader: their
plain type parameters may still bind complete HGL source shapes, including
`atomic` and `rolling`. The compiler retains HGL source-type arguments in its
IR so a later language version can relax the struct restriction without
redesigning the semantic model.

Generic abstract families and final concrete specializations compose directly:

```hgl
export abstract struct Event<T> {
    payload: T
}

export struct QuoteEvent: Event<Quote> {}

export struct TaggedEvent<T>: Event<T> {
    tag: str
}
```

Each `TaggedEvent<T>` specialization is final. `Event<Quote>` and
`Event<Trade>` have separate closed concrete families, and a child parent
application must be complete after substituting the child's parameters.
Inherited default overrides are checked against the substituted field type.

Each concrete declaration or fully applied generic specialization supplies
three contextual representations:

| Source use | hgraph interpretation |
| --- | --- |
| `const quote: Quote` | Canonical scalar value corresponding to `CompoundScalar` / `Bundle` |
| `quote: atomic<Quote>` | One atomic endpoint carrying a complete `Quote` snapshot |
| `quote: Quote` | A named bundle whose fields are independently temporal |

Temporalization is recursive unless an `atomic` marker stops it. For example:

```hgl
struct Book {
    best: Quote
    quotes: map<str, Quote>
    snapshots: map<str, atomic<Quote>>
    configuration: atomic<BookConfig>
}
```

Temporal `Book.best` is a nested `Quote` bundle, and each value in `quotes` is
also a `Quote` bundle. Each value in `snapshots` is instead one atomic `Quote`,
while `configuration` is one atomic `BookConfig`. `atomic<Book>` stops the
whole expansion and carries a complete scalar `Book`. In a scalar projection,
an atomic marker changes no storage type: its canonical value is the canonical
value of its payload.

Field access follows the same context. Accessing `quote.bid` on a temporal
`Quote` projects its `bid` endpoint. Access through `atomic<Quote>` observes a
field of the complete snapshot, so all such projections share the enclosing
snapshot's tick. Access on a `const Quote` reads the scalar field.

### Complete construction

Struct construction uses named arguments only:

```hgl
Quote(
    bid: 100.0,
    ask: 101.0
)
```

A field without a default is required. A non-null default supplies an omitted
value. A `null` default declares an optional field whose value is initially
unset:

| Declaration | Complete construction |
| --- | --- |
| `bid: f64` | The caller must supply `bid` |
| `currency: str = "USD"` | Omission supplies `"USD"` |
| `venue: str = null` | Omission leaves `venue` unset |

`null` is a polymorphic absence literal accepted only where the expected field
is optional. Passing `null` to a required field is a type error. Defaults and
optionality are authoring metadata; the underlying Bundle validity bitmap
represents whether a field is set. Defaults apply when constructing a struct;
they do not replace an invalid field on an existing temporal input.

Context also selects construction behavior. In scalar context the call creates
a canonical value. In a temporal `Quote` context, temporal arguments retain
their independent shapes and scalar arguments are lifted. In an
`atomic<Quote>` context, the compiler aggregates the supplied temporal fields
and publishes one complete snapshot when any supplied field changes and every
required field is valid.

```hgl
fn make_quote(bid: f64, ask: f64) -> Quote =>
    Quote(bid: bid, ask: ask)

fn make_snapshot(bid: f64, ask: f64) -> atomic<Quote> =>
    Quote(bid: bid, ask: ask)
```

Struct values are immutable. `var` may rebind a complete value; it does not
make fields assignable in place.

### Sparse delta values

Complete-value requirements do not apply to a delta. A contextual
`delta<Struct>(...)` constructor describes only the fields changed in one
evaluation, so even a required, defaultless field may be omitted:

```hgl
delta<Quote>(bid: 100.5)
delta<Box<f64>>(value: 1.5)
```

An omitted delta field means no change, and field defaults are never applied
while constructing a delta. Explicit `null` clears an optional field; it is
not the same as omission. Clearing a required field is a type error. A generic
delta target must be fully applied; delta construction does not infer an
omitted type argument.

Deltas recurse through structural fields and stop at atomic boundaries:

```hgl
delta<Book>(
    best: delta<Quote>(bid: 100.5),
    configuration: BookConfig(mode: "continuous")
)
```

Here `best` receives a sparse nested delta. `configuration` is atomic and must
therefore receive a complete `BookConfig` snapshot.

`delta<T>` is a contextual update value rather than an ordinary source type.
It may appear in runtime output, harness and replay sequences, and temporary
`let` bindings, but not as a struct field or ordinary function parameter or
result. Every temporal value already has an associated delta, so admitting a
normal `delta<delta<T>>` type would describe the wrong abstraction.

A runtime function writes a delta with the ordinary output forms:

```hgl
fn update_quote(price: f64) -> Quote {
    when modified(price) {
        return delta<Quote>(bid: price)
    }
}
```

Use `inject out` when the write must not terminate the evaluation:

```hgl
fn update_quote_and_continue(price: f64) -> Quote {
    inject out

    when modified(price) {
        out = delta<Quote>(bid: price)
    }
}
```

For an `atomic<Quote>` output, a tick is a complete `Quote`; a sparse
`delta<Quote>` is rejected unless user code explicitly retains, patches, and
publishes prior state.

The first structured-value slice does not yet define self-recursive fields,
destructuring, or copy-with-update syntax. Runtime type tests, concrete
downcasts, exhaustive family matching, and the explicit temporal
base-projection spelling also remain to be defined. Generic parameter defaults
and partial generic application are deliberately deferred. Structural
constraints still inspect nominal structs through
`U is struct`, `fields(U)`, `has_fields(U, names)`, and
`field_type(U, name)`; satisfying such a constraint does not make unrelated
nominal structs assignment-compatible.

## Rolling windows

`rolling<T, max_size, min_size>` describes a rolling-window time series. The
sizes are either tick counts (`i64`) or durations, and the minimum is
optional at a use site:

```hgl
rolling<f64, 20>          // the last 20 values
rolling<f64, 20, 5>       // the last 20 values, valid from 5
rolling<f64, 5m>          // everything in the last five minutes
rolling<f64, 5m, 1m>      // the last five minutes, valid once it spans 1m
```

The square brackets in the descriptive form
`rolling<T, max_size[, min_size]>` mean “optional”; they are not source
punctuation. Omitting `min_size` means `min_size = max_size`, so the first
form becomes valid when it contains 20 values and the third once its oldest
and newest values are five minutes apart. Both sizes are wiring-time
constants of one kind: `rolling<f64, 5m, 3>` is a type error. Tick sizes are
positive; a duration maximum is positive and a duration minimum may be `0s`,
meaning valid from the first value. `min_size` cannot exceed `max_size`, and
the kind and resolved sizes are part of the type identity, so
`rolling<f64, 5m>` and `rolling<f64, 300s>` are the same type while
`rolling<f64, 20>` and `rolling<f64, 20s>` are different types.

A tick window keeps the newest `max_size` values and drops the oldest when
full. A duration window keeps every value that ticked within `max_size` of
the current evaluation time and drops older values as time moves on; it has
no fixed capacity, so a fast source makes a large window. Until a window
reaches its minimum it is invalid and its consumers do not evaluate. The
duration minimum is measured across the values the window holds, not the
time since the graph started, so `rolling<f64, 5m, 1m>` with a single value
is still invalid.

`rolling` is itself a temporal type constructor and maps to hgraph's `TSW`
schema. It is not a canonical scalar container, so it cannot appear beneath
`atomic` or as the type of a `const` value. Its element argument is a canonical
value type.

Generic functions and operators may bind the element type and sizes:

```hgl
rolling<T, max_size, min_size>
```

Here `T` is a declared type parameter and `max_size` and `min_size` are declared
`const` generic parameters whose declared type, `i64` or `duration`, fixes
the kind of window the function accepts. A spelling that accepts either kind,
and the way rolling windows participate in `values` or `items` iteration,
remain open; the current collection-iterator table does not implicitly
include them.

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
assigned again. A `var` may use ordinary and compound assignment, but its type
is fixed by its annotation or, when unannotated, by its initializer. The normal
`i64`-to-`f64` widening is allowed when the fixed type is `f64`; assigning an
`f64` to an `i64` variable, including through `/=`, is a type error. Function
parameters and `for` bindings remain immutable.

## Calls and operators

Calls use ordinary positional and named arguments:

```hgl
rolling_mean(value, window)
rolling_mean(value, period: window)
```

Operator calls use hgraph's candidate matching, defaults, scalar lifting,
ranking, and output resolution. Whether the selected implementation is a graph
or node is invisible at the call site. An imported exact `export fn` call has
one declared target and does not enter candidate ranking.

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
| Fixed temporal list, `list<T, n>` (TSL) | `values`, `items` | `modified` |
| Unbounded temporal list, `list<T>` (TSL) | `values`, `items` | `added`, `modified`, `removed` |
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
