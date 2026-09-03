# Syntax and semantics

Status: proposed grammar with provisional runtime-function semantics

This chapter specifies the syntax agreed so far and records the first proposed
rule for distinguishing wiring composition from runtime node evaluation. The
rule is intentionally narrow so it can be refined before the first language
edition is accepted.

## Lexical rules

Source files are UTF-8. The first lexer slice uses ASCII identifiers:

```text
identifier := [A-Za-z_][A-Za-z0-9_]*
```

String contents may be UTF-8. Required escapes initially include `\"`, `\\`,
`\n`, `\r`, and `\t`.

`//` starts a line comment. Newlines separate declarations and statements;
braces delimit blocks, so indentation is non-semantic. Semicolons are not
statement terminators or parameter separators.

A temporal literal is one token: `@` followed by an RFC 3339 date, time, or
instant (`@2026-09-03`, `@09:30:00`, `@2026-09-03T09:30:00Z`), or a number
directly followed by a duration unit (`5m`, `1.5h`). Their grammar and
validation rules are in the temporal scalar section below.

The hard reserved words for the current design surface are:

```text
module use as export impl operator fn const let var state inject return if else
start when stop for
true false
bool i64 f64 str date time datetime duration
```

Every word in that list is reserved everywhere, including the ones such as
`state`, `start`, `stop`, and `when` that are only meaningful at a particular
position in a runtime function body. Variables are introduced by `let`, `var`,
and `state`, and each block keyword carries its own placement rule, so there is
no ambiguity to resolve by making them contextual; they are withheld from
parameter and variable names deliberately to keep a runtime body readable.

`atomic`, `tuple`, `list`, `set`, `map`, and `rolling` are contextual type
keywords, and `unbounded` is a contextual constant in a list-size position.
Outside a type position, the same spelling can resolve to a function,
as in `map(values, fn(value) => value * 2.0)`. `out` and the names of other
injectables are contextual names resolved only by an `inject` declaration.

## Compilation-unit grammar

The grammar is descriptive EBNF. `NL` means one or more newlines, and commas
may trail multiline lists.

```ebnf
source_file     = module_decl, NL,
                  { use_decl, NL },
                  { declaration, NL };

module_decl     = "module", module_path;
module_path     = identifier, { ".", identifier };
qualified_name  = identifier, "::", identifier;

use_decl        = "use", module_path,
                  ( "::", import_set | "as", identifier );
import_set      = "{", identifier, { ",", identifier }, [ "," ], "}";

declaration     = operator_decl | function_decl;
operator_decl   = "operator", identifier, [ generic_parameters ],
                  function_signature;
function_decl   = [ "export" | "impl" ], "fn", identifier,
                  [ generic_parameters ], function_signature, function_body;

generic_parameters
                = "<", generic_parameter,
                  { ",", generic_parameter }, [ "," ], ">";
generic_parameter
                = type_parameter | const_generic_parameter;
type_parameter  = identifier;
const_generic_parameter
                = "const", identifier, ":", value_type;

function_signature
                = "(", [ parameters ], ")", [ "->", type ];
parameters      = parameter, { ",", parameter }, [ "," ];
parameter       = temporal_parameter | const_parameter;
temporal_parameter
                = identifier, ":", type;
const_parameter = "const", identifier, ":", value_type,
                  [ "=", const_expression ];

function_body   = ( "=>", expression ) | block;
const_expression = expression;
```

A function or operator signature with no return arrow is outputless. An
`operator` declaration ends at the newline after its signature and cannot have
a body. A temporal parameter cannot have a default in the agreed slice.
`const` marks wiring-time function parameters and wiring-time generic values;
it is not a general local-variable qualifier. `export` applies only to a named
ordinary exact `fn`. `impl` marks a named `fn` as an implementation of an
operator in scope; the two modifiers are mutually exclusive. Operators are
public without a modifier.

## Anonymous functions

```ebnf
function_expr   = "fn", "(", [ anonymous_parameters ], ")",
                  [ "->", type ], "=>", expression;
anonymous_parameters
                = anonymous_parameter,
                  { ",", anonymous_parameter }, [ "," ];
anonymous_parameter
                = identifier | identifier, ":", type;
```

Context may fill omitted anonymous parameter and result types. Capture and
generic inference rules remain open.

## Types

```ebnf
type            = scalar_type
                | tuple_type
                | list_type
                | set_type
                | map_type
                | rolling_type
                | identifier
                | "atomic", "<", value_type, ">";
value_type      = scalar_type
                | value_tuple_type
                | value_list_type
                | set_type
                | value_map_type
                | identifier;
scalar_type     = "bool" | "i64" | "f64" | "str"
                | "date" | "time" | "datetime" | "duration";
tuple_type      = "tuple", "<", type, { ",", type }, ">";
list_type       = "list", "<", type, [ ",", size_expression ], ">";
size_expression = const_expression | "unbounded";
set_type        = "set", "<", value_type, ">";
map_type        = "map", "<", value_type, ",", type, ">";
rolling_type    = "rolling", "<", value_type, ",",
                  const_expression,
                  [ ",", const_expression ], ">";
value_tuple_type = "tuple", "<", value_type,
                   { ",", value_type }, ">";
value_list_type = "list", "<", value_type, ">";
value_map_type  = "map", "<", value_type, ",", value_type, ">";
```

This grammar permits `atomic<T>` only at a temporal boundary. Nested atomic
values remain expressible because a container's element is itself
temporalized:

```hgl
map<str, atomic<tuple<f64, f64>>>
```

`value_type` excludes `atomic` and `rolling` and is used for `const` parameters
and atomic payloads. `type` allows atomic boundaries recursively inside
structural values. `rolling` is already a temporal endpoint shape and therefore
cannot appear under `atomic` or in a `const` parameter.

The first rolling-window form uses positive tick-count `i64` sizes:

```hgl
rolling<f64, 20>
rolling<f64, 20, 5>
```

Omitting the third argument normalizes the minimum size to the maximum size.
The minimum cannot exceed the maximum, and both resolved values participate in
type identity. Size arguments must be constant expressions formed from literals
or in-scope `const` generics and cannot depend on temporal values.
Duration-window arguments remain a separate design question.

A temporal list is unbounded unless it carries a size:

```hgl
list<f64>              // unbounded; the same as list<f64, unbounded>
list<f64, 3>           // exactly three temporal elements
list<f64, n>           // n is an in-scope const generic
```

`unbounded` is the sentinel size. A `const` generic in a list-size position
binds the argument's actual size, including `unbounded` for an unbounded list,
so one generic `fn` can accept both forms. A fixed size must be a positive
constant expression. In a parameter position `list<T>` accepts a list of any
size, mirroring hgraph's rule that a dynamic `TSL` pattern matches every
concrete size; `list<T, 3>` accepts only a three-element list. The resolved
size is part of the type identity. A separate `array<T, n>` spelling for fixed
lists was considered and rejected: an implementation indifferent to fixedness
would then need two candidates instead of one size-generic declaration.

## Temporal scalar types

Status: proposed in the review of the scaffold. The type set follows RFC 0002
and the literal spelling is open for decision.

HGL has four core temporal scalars. Each is one of hgraph's RFC 0002 core
types, carries no time zone, and is an atomic endpoint when temporalized:

| Type | Value | hgraph type | Range and resolution |
| --- | --- | --- | --- |
| `date` | calendar date, proleptic Gregorian | `CivilDate` (`Date`) | `0001-01-01` to `9999-12-31` |
| `time` | time of day | `CivilTime` (`Time`) | `00:00:00` up to but excluding `24:00:00`, microsecond resolution |
| `datetime` | an instant on the UTC timeline | `Instant` (`DateTime`) | signed 64-bit microseconds from the Unix epoch |
| `duration` | signed elapsed time | `Duration` (`TimeDelta`) | signed 64-bit microseconds |

`datetime` is the type of the engine clock and of `last_modified`. It is not a
civil date-time: it has no local fields to lose, and field accessors such as
`hour` read its UTC fields. A `duration` day is exactly 86 400 seconds, and a
`duration` has no month or year component because those are calendar periods
rather than elapsed time. Leap seconds do not exist in any of the four types.

The remaining RFC 0002 types, the civil date-time, calendar period, zone
identifier, zoned date-time, and the ranges, are library scalars rather than
core vocabulary. They stay reachable under their registered hgraph names
(`civil_datetime`, `period`, `zone_id`, `zoned_datetime`, `instant_range`,
`civil_date_range`) through the scalar surface that extension descriptors will
provide; the four core names are the only temporal reserved words.

### Literals

`@` introduces a date, time, or instant written in its RFC 3339 form, and a
number immediately followed by a unit is a duration:

```ebnf
temporal_literal = date_literal | time_literal | datetime_literal
                 | duration_literal;
date_literal     = "@", calendar_date;
time_literal     = "@", clock_time;
datetime_literal = "@", calendar_date, "T", clock_time, utc_offset;
calendar_date    = digit4, "-", digit2, "-", digit2;
clock_time       = digit2, ":", digit2, ":", digit2, [ ".", digit1to6 ];
utc_offset       = "Z" | ( "+" | "-" ), digit2, ":", digit2;
duration_literal = digits, [ ".", digits ], duration_unit;
duration_unit    = "d" | "h" | "m" | "s" | "ms" | "us";
```

```hgl
@2026-09-03                  // date
@09:30:00                    // time
@09:30:00.250                // time, 250 milliseconds past the second
@2026-09-03T09:30:00Z        // datetime
@2026-09-03T10:30:00+01:00   // the same datetime written with an offset
5m                           // duration: five minutes
1.5h                         // duration: ninety minutes
-250ms                       // unary minus applied to 250ms
1h + 30m                     // a constant expression folded at compile time
```

Every literal is validated and normalized when it is lexed:

- a date must exist in the calendar, so `@2026-02-29` is a diagnostic;
- a time must be earlier than `24:00:00`; `24:00:00` and the leap-second form
  `23:59:60` are diagnostics;
- an instant must carry `Z` or an offset and is stored normalized to UTC;
  `@2026-09-03T09:30:00` is a diagnostic rather than a civil date-time, so
  Python's naive-means-UTC convention never enters the language;
- `T` and `Z` are upper case, every field has exactly the digit count shown,
  and the fraction has one to six digits;
- a duration is its decimal number scaled by the unit using exact decimal
  arithmetic; it must be a whole number of microseconds (`0.5us` is a
  diagnostic) and fit the 64-bit range; the unit is lower case, and `m` is
  minutes because calendar months are not durations;
- a duration literal has no exponent, no internal whitespace, and exactly one
  unit: `1h30m` is not a token, and `1h + 30m` folds to the same value.

Unary minus applies to the literal as an operator; there is no signed literal
token. Because `@` is followed by a fixed digit pattern rather than a run of
operator-free characters, `@2026-09-03-1d` lexes as `date - duration` and
`@2026-09-03T09:30:00Z-1d` as `datetime - duration`, although both read better
with spaces. An identifier directly after a number (`5min`, `2x`) is an
unknown-unit diagnostic rather than a juxtaposition error.

Alternatives considered: bare ISO 8601 tokens conflict with subtraction
(`2026-9-3`) and with named arguments and annotations (`09:30`); typed string
prefixes such as `date"2026-09-03"` read as strings, need one prefix per type,
and defer validation to a later phase; constructor calls (`date(2026, 9, 3)`)
are not literals and cannot appear in a `const` default. The `@` sigil is
otherwise unused in the language, is one lexer rule, and lets the ISO shape
select the type.

### Canonical spelling

Tooling, diagnostics, and constant printing use one spelling per value, and
the lexer accepts every spelling above, so a value round-trips through its
canonical form:

- `date`: `@YYYY-MM-DD`;
- `time`: `@HH:MM:SS`, with a six-digit `.ffffff` only when the microsecond
  part is non-zero;
- `datetime`: `@YYYY-MM-DDTHH:MM:SS[.ffffff]Z`, always in UTC; an offset is an
  input convenience only;
- `duration`: an integer count in the largest unit that divides the value
  exactly, so `5400s` prints as `90m`, `1500000us` as `1500ms`, and zero as
  `0s`.

These are source spellings. Interchange forms (JSON, Arrow, recording) are
hgraph's RFC 0002 codecs and are not part of the language.

### Arithmetic and comparison

The four types have no implicit conversions between them or from numbers. The
defined operations follow RFC 0002 exactly and apply in every phase: in a
composition body they wire the standard hgraph operators, in a runtime body
they are the same checked scalar operations, and in a constant expression they
fold at compile time with identical results.

| Expression | Result | Rule |
| --- | --- | --- |
| `datetime + duration`, `duration + datetime`, `datetime - duration` | `datetime` | checked overflow |
| `datetime - datetime` | `duration` | checked overflow |
| `date + duration`, `date - duration` | `date` | uses the duration's floor-based whole-day component: `d + 36h` is `d + 1d` and `d - 1us` is `d - 1d`, matching Python `date` arithmetic |
| `date - date` | `duration` | a whole number of days |
| `duration + duration`, `duration - duration`, `-duration` | `duration` | checked overflow |
| `duration * i64`, `duration * f64`, and the reversed operand order | `duration` | floating-point scaling rounds to the nearest microsecond, ties to even |
| `duration / i64`, `duration / f64` | `duration` | as above |
| `duration / duration` | `f64` | ratio |
| `<`, `<=`, `>`, `>=`, `==`, `!=` between two values of one type | `bool` | chronological order |

Everything else is a `type` diagnostic, in particular `datetime + datetime`,
`time + duration` and `time - time` (crossing midnight needs a date),
`date + time` (its result is the civil date-time, which is not a core scalar),
`duration * duration`, `%` on any temporal type, and every comparison between
different types. A `duration` in a `rolling` size position is likewise a
`type` diagnostic until duration windows are designed.

Overflow in a constant expression is a compile-time diagnostic. Overflow at
runtime raises through hgraph's checked temporal arithmetic; the language's
runtime scalar error behaviour is an open question shared with the numeric
types.

Field accessors (`year`, `month`, `day`, `weekday`, `hour`, `minute`,
`second`, `microsecond`, `days`, `seconds`, `total_seconds`), `abs`, and the
rounding functions (`temporal_floor`, `temporal_ceil`, `temporal_round`) are
ordinary standard-library functions resolved like any other call, not syntax.

## Generics and nominal operator binding

A plain generic parameter binds a source type. A `const` generic parameter
binds a wiring-time value and may appear in a type-shaping position such as a
rolling-window size. Generic scope covers the declaration signature and an
`fn` body. Repeated uses require an equivalent type or equal constant value.

Every generic needed by a selected implementation must resolve from input
arguments, expected output, explicit generic arguments, or a future declared
default. Unresolved or inconsistently rebound generics are type diagnostics.
The initial syntax for explicit generic arguments, generic defaults, and
constraints remains open; the AST and semantic model must not assume they are
type parameters only.

An `operator` declaration introduces a nominal, bodyless callable contract. Its
identity is `(defining module, declaration name)`, not its short name. The
contract owns public parameter names and order, temporal-versus-`const` roles,
defaults, and generic input/output relationships. Every operator is public by
definition; `export operator` is not a declaration form.

An `impl fn` is an implementation candidate of the operator with the same
name in the module's unqualified declaration scope. That operator is either
declared locally or introduced by a selective import:

```hgl
use my.contracts::{my_op}

impl fn my_op(value: f64) -> f64 => value
```

The binding is explicit. `impl fn` with no such operator in scope is a
`module` diagnostic, which catches a misspelt implementation name at the
declaration instead of leaving a quietly unrelated function. A plain `fn`
never binds to an operator; declaring a plain `fn` whose name is already an
operator in the module's unqualified scope is a `name` conflict, resolved by
writing `impl fn` or renaming the function. Adding or removing a `use` can
therefore never change what an existing declaration means: an import only
makes an `impl fn` resolve or fail to resolve. This rule replaced implicit
same-name binding because the implicit form let a selective import added for
an unrelated call turn a private helper into a globally registered candidate,
and because the nominal systems the operator model borrows from, Rust traits
and Swift protocols, always write the conformance out.

The implementation signature must be a compatible specialization of the
contract. It may introduce its own generics, but cannot change the contract's
public argument roles. Its body still passes through ordinary function
classification and may lower to either graph composition or one runtime node.
Several `impl fn` declarations may share a name; each is a separate candidate
of the same operator. An `impl fn` contributes a public candidate to the
operator and cannot also be marked `export`; it is not an independently named
exact function.

A module alias creates only a namespace:

```hgl
use my.contracts as mc

mc::my_op(value)
```

It does not introduce `my_op` as an unqualified name, so an `impl fn my_op`
cannot bind through it. Two selective imports that introduce different
nominal operators under the same local short name are rejected before
implementation binding.
Multiple aliased modules may expose the same short operator name because each
qualified reference resolves directly to one nominal identity.

A named `fn` without `impl` is an ordinary exact function. Repeating an
ordinary function name does not implicitly create an overload family. It is
module-internal unless marked `export fn`, which places that exact signature
in the public module interface.
Exported exact functions must still have unique names.

Both selective and aliased imports expose only operators and exported exact
functions. The first language edition has no `export use` or other re-export
form: implementation providers never create alternate import identities for an
operator defined elsewhere.

## Blocks and expressions

A composition block contains lexical `let` and `var` bindings, calls,
conditionals, explicit `return`, and a possible final expression:

```hgl
fn smooth(value: f64, const window: i64) -> f64 {
    let averaged = rolling_mean(value, window)
    averaged
}
```

`=> expression` is the concise equivalent:

```hgl
fn double(value: f64) -> f64 => value * 2.0
```

The parser records block structure, tail expressions, and explicit returns
without assigning graph or node semantics. Control-flow restrictions belong
after function classification.

Blocks admit lexical declarations and `for` statements. Runtime function
bodies additionally admit function-level state, injectable, lifecycle, and
activation forms:

```ebnf
local_decl     = ( "let" | "var" ), identifier, [ ":", type ],
                 "=", expression;
state_decl     = "state", identifier, [ ":", value_type ],
                 "=", expression;
inject_decl    = "inject", identifier,
                 { ",", identifier }, [ "," ];
lifecycle_block = ( "start" | "stop" ), block;
when_statement = "when", expression, block;
for_statement  = "for", iteration_pattern, "in", expression, block;
iteration_pattern
               = identifier | identifier, ",", identifier;
mutation_statement
               = place, assignment_operator, expression;
place          = identifier,
                 { "[", expression, "]" | ".", identifier };
assignment_operator
               = "=" | "+=" | "-=" | "*=" | "/=";
```

State and inject declarations precede executable blocks. The first slice
requires a state initializer and permits at most one `start` and one `stop`
block. It permits multiple `when` blocks and preserves their source order.
These are semantic restrictions rather than parser shortcuts so diagnostics
can identify the misplaced or duplicate construct precisely.

An inject declaration may span lines after `inject`; newlines around commas do
not terminate it. Duplicate injectable names are rejected after name
resolution.

Mutation statements are restricted to declared `state` variables, injected
`out`, declared `var` bindings, and their writable projections. Parameters,
`let` bindings, and `for` bindings remain immutable. Compound assignment reads
the previous value and therefore follows the same validity rules as an explicit
read followed by assignment.

`let` and `var` are lexical declarations and both require an initializer in the
first slice. In a `CompositionFn`, their initializer may produce a scalar or a
port handle; assigning a `var` only changes that local handle. In a `RuntimeFn`,
they hold canonical scalar values local to the executing block. Runtime `var`
storage is recreated on every block execution and is never added to the
function's recordable state. A value that crosses evaluations must use `state`.

Expression precedence is:

| Precedence, high to low | Tokens |
| --- | --- |
| Postfix | calls, indexing, field access |
| Unary | `-`, `!` |
| Multiplicative | `*`, `/`, `%` |
| Additive | `+`, `-` |
| Comparison | `<`, `<=`, `>`, `>=` |
| Equality | `==`, `!=` |
| Boolean AND | `&&` |
| Boolean OR | `\|\|` |

Calls use positional arguments followed by named arguments:

```hgl
rolling_mean(value, window)
rolling_mean(value, period: window)
analytics::rolling_mean(value, period: window)
```

A qualified callee begins with an alias introduced by `use module.path as
alias` and uses `::` between namespace and declaration names. Dots remain the
syntax of canonical module paths in `module` and `use` declarations; they are
not expression member access.

Duplicate names, unknown names, positional arguments after named arguments,
and missing required arguments are source diagnostics.

## Canonical temporalization

The frontend first resolves a canonical value type, then expands it in temporal
context:

```text
temporalize(bool | i64 | f64 | str)
    = atomic hgraph endpoint carrying that scalar

temporalize(date | time | datetime | duration)
    = atomic hgraph endpoint carrying that RFC 0002 scalar

temporalize(tuple<T0, T1, ...>)
    = structural un-named bundle with positional fields
      _0: temporalize(T0), _1: temporalize(T1), ...

temporalize(list<T>)
    = unbounded structural list of temporalize(T) children

temporalize(list<T, n>)
    = structural list of exactly n temporalize(T) children

temporalize(set<T>)
    = set-valued hgraph endpoint carrying canonical T members

temporalize(map<K, V>)
    = keyed temporal map with canonical key K
      and temporalize(V) values

temporalize(rolling<T, Max, Min>)
    = hgraph TSW endpoint carrying canonical T values
      with resolved tick sizes Max and Min

temporalize(record fields)
    = structural bundle of temporalized fields

temporalize(atomic<T>)
    = one atomic endpoint carrying canonical value T
```

`const x: T` bypasses `temporalize` and resolves to canonical value `T`.
`const x: atomic<T>` is invalid because atomicity describes a temporal
boundary.

The compiler must map every expanded shape to an existing public hgraph schema.
It must not create a language-only runtime representation. A structural tuple
is therefore hgraph's un-named structural bundle with index-named fields
(`_0`, `_1`, ...), which already admits heterogeneous children; two tuples with
the same element types intern to the same schema. Indexing a structural tuple
with a literal index is positional field access. A homogeneous
`tuple<f64, f64>` is deliberately not a `list<f64, 2>`: a tuple is accessed by
position, a list is sized and traversed.

## Temporal metadata syntax

Metadata is expressed with functions:

```hgl
modified(value)
valid(value)
modified(bid, ask)
valid(bid, ask)
all_valid(book)
last_modified(value)
delta(value)
```

The parser treats these as ordinary calls resolved through the prelude or
intrinsic declarations. Source member spellings such as `value.modified`,
`value.valid`, and `value.value` are not part of the language.

These calls are phase-polymorphic in the same way as `key_set`. In a
composition function they wire the standard hgraph operators of the same
meaning: `valid` and `modified` produce a `bool` time series and
`last_modified` a `datetime` time series, with the multi-argument forms
composing through the standard Boolean operators. In a runtime function,
`modified` and `valid` inspect evaluator-local endpoint metadata rather than
construct Boolean time series. Both require at least one
argument and fold over their arguments with complementary rules:

```text
modified(a, b, c) = modified(a) || modified(b) || modified(c)
valid(a, b, c)    = valid(a) && valid(b) && valid(c)
```

The compiler may consume these calls while deriving node input policies, so
they need not remain as runtime calls in generated C++. `valid(value)` tests
the top-level endpoint even when the endpoint is structural or a collection;
recursive child validity is expressed separately as `all_valid(value)`. The
result shape of `delta` remains open.

`last_modified(value)` is a runtime metadata operation returning `datetime`.
It lowers to the endpoint's public `last_modified_time` view and does not
construct another time series. Its result before the endpoint's first
modification follows hgraph's native endpoint contract.

## Runtime collection traversal

`key_set(tsd)` is phase-polymorphic. In a `CompositionFn` it resolves to the
registered live TSS projection and has temporal source type `set<K>`. In a
`RuntimeFn` it produces an evaluation-local borrowed set view over the current
TSD key set.

`keys`, `values`, and `items` produce runtime-only iterator types. They accept
the collection followed by an optional predicate:

```ebnf
collection_iterator
               = ( "keys" | "values" | "items" ), "(", expression,
                 [ ",", expression ], ")";
```

The calls are parsed as ordinary call expressions. The grammar above records
their checked intrinsic shapes rather than adding special parser nodes. A
runtime collection iterator is a node-only construct and therefore classifies
its containing function as `RuntimeFn`. The iterator must be consumed directly
by `for`; it is neither a canonical value nor a temporal port and cannot escape
the current evaluation.

Traversal and built-in delta-predicate support is:

| Runtime shape | Traversals | Delta predicates |
| --- | --- | --- |
| TSB, including structural tuples | `keys`, `values`, `items` | `modified` for values/items |
| TSD | `keys`, `values`, `items` | `added`, `modified`, `removed` |
| `list<T, n>` (fixed TSL) | `values`, `items` | `modified` |
| `list<T>` (unbounded TSL) | `values`, `items` | `added`, `modified`, `removed` |
| TSS | `values` | `added`, `removed` |

`items` yields two bindings. TSB yields `str` field names and the corresponding
field bindings; TSD yields its canonical key type and value-child bindings;
TSL yields `i64` indices and element-child bindings. `keys` and TSS membership
iteration yield scalar values. Other value bindings retain their child endpoint
identity so metadata calls continue to work inside the predicate and loop body.

A predicate may be a built-in name, a compatible named function, or an inline
concise `fn`. It is invoked with one argument for `keys` and `values`, or two
arguments for `items`, and must produce a runtime Boolean scalar. A bare
metadata predicate is resolved contextually against the iterator entry. For
example, `items(tsd, modified)` selects modified entries; it does not mean
`modified(key, value)`. `added` and `removed` likewise inspect membership-slot
provenance.

```hgl
items(book, fn(key, value) =>
    valid(value) && last_modified(value) > some_time)
```

Predicates are phase-checked into their containing runtime function. A known
source predicate is inlined or emitted as a direct static call; it is not
materialized as an allocated callable value. Captures obey ordinary runtime
validity rules. Predicates are pure: mutation of `var`, `state`, or `out`, and
calls with runtime effects, are rejected.

For a heterogeneous TSB, traversal is statically expanded in schema order. The
predicate and loop body must type-check for every selected field; the compiler
must not erase heterogeneous children into a dynamic language value. TSL items
are traversed in ascending index order. TSB fields use schema order. TSD and TSS
iteration preserve the native hgraph view order and do not promise sorting.

## Function classification boundary

After parsing, name resolution, and canonical type-shape resolution, each
source function is still an `UnclassifiedFn`. A later semantic stage must
determine whether its body describes:

- wiring-time graph composition;
- runtime compute or sink behavior;
- or another explicitly admitted hgraph implementation kind.

The current provisional classifier applies these rules:

1. A body containing no node-only construct becomes `CompositionFn`.
2. The presence of `state`, `inject`, `start`, `when`, `stop`, or a runtime
   collection iterator makes the complete function a `RuntimeFn`, even when
   nested syntax is later rejected by phase checking.
3. A body that mixes wiring-only and runtime-only constructs is rejected.

Classification is based on resolved source syntax. It must not be guessed
from which imported overload happens to win, inferred from generated C++, or
changed between scripted and AOT modes.

After classification, phase and effect checking gives identifiers different
meanings in the two phases. A temporal parameter is a port in a composition
body. In a runtime expression it denotes the current admitted payload, while
`modified(parameter)`, `valid(parameter)`, `last_modified(parameter)`, and
`delta(parameter)` retain access to its endpoint metadata.

Runtime validity checking is flow-sensitive. A payload read is valid when the
input is statically admitted by the outer `when` predicate or the read is
dominated by a successful `valid(input)` check. Otherwise it is a diagnostic.

Multiple `when` blocks execute as independent ordered conditions. Classification
and phase checking derive one safe node policy across the complete body:

- the active input set is the union of inputs that can activate any handler;
- node-level required validity contains only requirements common to every
  executable handler;
- handler-specific activation, validity, and other predicates remain ordered
  runtime conditions.

A runtime function with no `when` uses hgraph's default policy: every ordinary
temporal input is active and required-valid.

## Runtime state, injectables, and lifecycle

Each `state` declaration introduces a mutable function-lifetime binding. The
compiler aggregates all declarations into one typed recordable-state schema.
Initializers run during startup only for fields that were not restored by
record/replay. Initializers may depend on `const` parameters and admitted pure
scalar expressions, but not current temporal input values. `state` is by
definition temporal: it is part of the node's recorded and replayed data, and
there is no non-recordable `state` form. Values that are not time series, such
as cached adaptor handles, are not `state`; they belong to a separate resource
concept that is still to be designed.

An `inject` declaration requests compiler-approved runtime selectors without
adding parameters to the callable contract. Each generated hook requests only
the selectors it uses. Unknown capabilities and use from an unsupported phase
are diagnostics. `out` is a special injectable inferred from the result type;
it is invalid on an outputless function and is initially available only during
evaluation, not in `start` or `stop`.

`start` runs once after replay-aware state initialization. `stop` runs once at
teardown. State storage and injected capabilities are runtime-owned and are
destroyed automatically; `stop` represents semantic finalization. The language
does not use lifecycle blocks to acquire arbitrary native resources.

## Runtime output

In a composition function, `return expression` returns the wired result. In a
runtime function it assigns the complete output, ticks it, and terminates the
current evaluation. A runtime path reaching the end without a return or output
mutation produces no output tick.

`inject out` exposes a typed, mutable output binding. Reading it requires
`valid(out)` unless flow analysis proves validity. Whole-output assignment
continues evaluation and uses last-write-wins within one evaluation. Writes to
different structural or collection children accumulate into one delta;
repeated writes to the same child use the last value. Later `when` blocks see
writes made by earlier blocks.

`return value` is semantically equivalent to assigning the complete output and
then returning. A bare `return` terminates evaluation after any preceding
incremental output mutations. Detailed collection mutation operations remain
part of structural-type design.

The classifier applies to named and anonymous functions wherever their body
grammar admits runtime constructs. The first concise anonymous-function slice
has no runtime block and therefore produces composition helpers only.

## Operator resolution

Every operator call carries a resolved nominal identity before overload
matching begins. An unqualified call obtains that identity from a local
declaration or one selective import. A qualified call obtains it from its
module alias. Operators with equal short names but different defining modules
never share a candidate set.

The candidate set for that identity comes from every module in the resolved
application target and locked dependency closure, not only the modules named by
source imports. Descriptors make this set available during `hgl check`; packages
outside the target cannot affect resolution.

The compiler passes that identity, resolved temporal shapes, canonical `const`
values, generic bindings, and source call arguments to hgraph's operator
resolver. The result includes:

- selected candidate identity and implementation kind;
- normalized positional and named arguments;
- resolved input and output schemas;
- defaults and admitted scalar lifts;
- candidate rejection reasons.

The language compiler must not copy hgraph's ranking algorithm or use source
syntax to expose graph-versus-node implementation details at call sites.
Concrete and constrained candidates may outrank broader generic candidates
under hgraph's normal rules. Equal-ranked candidates within the selected
operator are an ambiguity error; source order, import order, and registration
order do not break the tie.

The typed HIR records whether a named `fn` is an exact ordinary function or,
through its `impl` modifier, an implementation of a canonical operator
identity. Import changes only make an `impl fn` resolve or fail to resolve;
they never reinterpret an ordinary function as a candidate.

## Generated module lifecycle

There is no source grammar for top-level `init`, `deinit`, or disposal blocks.
The module compiler synthesizes lifecycle entry points and a registration handle
from the module's exports, operator candidates, types, and dependencies.

Initialization attaches the module once and records a replayable installer for
the current and later hgraph registry generations. Deinitialization removes the
provider from future resolution, removes its installer intent and active
registrations, and releases resources only after dependent graphs and plans
release their provider leases. Dependency order is forward for initialization
and reverse for deinitialization.

The module ABI must distinguish logical deactivation and registration removal
from physical native-library unloading. The latter is invalid while any
generated code, callback, or type metadata from the module remains reachable.

## AST requirements

Every token and AST node retains a half-open source range. The AST preserves:

- comments as source trivia;
- literal spelling, with the normalized value of a temporal literal;
- positional and named argument order;
- `const` on parameters;
- type and `const` generic parameters;
- `atomic` boundaries in types;
- rolling-window size arguments and omitted minimum-size syntax;
- list sizes, including the `unbounded` sentinel;
- nominal operator declarations and explicit `impl` bindings;
- explicit `export` on ordinary exact functions;
- selective imports, module aliases, and qualified references;
- `let` versus `var` local mutability;
- concise versus block function bodies;
- tail expressions and explicit returns;
- state and grouped inject declarations;
- `start`, ordered `when`, and `stop` blocks;
- collection traversal calls, predicate arguments, and `for` patterns;
- complete and projected output assignments;
- explicit versus context-inferred anonymous types.

Parser recovery should synchronize at closing braces, `export`, `impl`,
`operator`, `fn`, and top-level newlines.

## Diagnostics

Suggested categories are `parse`, `name`, `type`, `shape`, `function-kind`,
`phase`, `injectable`, `operator`, `module`, and `build`.

Examples:

```text
type: 'atomic<T>' is not valid on const parameter 'settings'
shape: map key type 'list<str>' is not a supported canonical key
function-kind: 'state' must be declared before runtime handlers
function-kind: wiring operation is not available inside a runtime function
injectable: 'out' requires a function output
phase: 'out' is not available during stop
operator: '+' has no hgraph overload for f64 and str
phase: runtime collection iterator cannot be stored in 'saved'
type: predicate for 'items' must accept (key, value) and return bool
module: hgraph.analytics does not export 'rolling_mean'
module: function 'validate_price' is internal to market.pricing
module: 'export impl fn add' is invalid; the implementation is public through hgraph.std::add
module: 'impl fn valeu' has no operator named 'valeu' in scope
name: 'fn value' conflicts with operator market.pricing::value; declare 'impl fn value' or rename it
module: operator 'value' is imported unqualified from both market.pricing and risk.pricing
operator: 'impl fn value' is not compatible with market.pricing::value
type: list size must be a positive constant or 'unbounded'
parse: '@2026-02-29' is not a calendar date
parse: '@2026-09-03T09:30:00' has no UTC offset; write 'Z' or '+HH:MM'
parse: '0.5us' is not a whole number of microseconds
parse: '5min' has unknown duration unit 'min'; units are d h m s ms us
type: 'datetime + datetime' is not defined; subtract two instants for a duration
type: 'date + time' has no core result type; civil_datetime is a library scalar
type: cannot compare 'date' with 'datetime'
module: cannot remove provider user.money while 2 live graphs retain it
type: rolling minimum size 25 exceeds maximum size 20
```

No-match and ambiguity diagnostics attach hgraph's candidate rejection reasons.

## Other semantic questions

Before code generation, an RFC must also define:

- `i64` overflow and conversion behavior;
- division by zero and NaN comparison;
- complete string escape and Unicode normalization rules;
- generic constraints, explicit generic arguments, generic defaults,
  output-directed inference, and overlapping-implementation coherence;
- general anonymous capture and type inference beyond inline iterator
  predicates;
- callable scalar kernels inside runtime functions;
- remaining recursive metadata and `delta` result shapes;
- duration rolling-window semantics (`rolling<T, 5m>` now parses and is
  rejected) and rolling-window iteration;
- ephemeral caches, lifecycle output access, and runtime sinks;
- runtime scalar error behavior.
