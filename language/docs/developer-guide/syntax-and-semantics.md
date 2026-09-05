# Syntax and semantics

Status: implemented parser grammar plus agreed, not-yet-implemented generic
constraints and structured-value syntax, with provisional runtime-function
semantics

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

A newline is a terminator only where a declaration or statement can end.
The parser ignores newlines inside `()`, `[]`, and generic `<>` lists;
directly after a comma, a binary or assignment operator, `=`, `=>`, `->`,
or the `:` that introduces a type or a named argument; and directly before
a line whose first token is a binary operator, a `->` result arrow, or the
`else` of an `if`. The binary-operator rule is what lets a long expression
continue on the next line:

```hgl
assert eval(midpoint, tob: [(1.0, _), (_, 3.0), (2.0, _)])
    == [_, 2.0, 2.5]
```

Inside `{}` a newline ends the statement, so a block body cannot be
continued by indentation alone; a `-` at the start of a line is a
continuation, not a negation, which is why the rule is stated in terms of
the first token rather than of indentation. `!` never continues a line.
The postfix forms never cross a newline: `f\n(x)` is two statements. The
`{` of a body or block is on the same line as its header, and two
statements on one line are a diagnostic. An `inject` list continues past
a trailing comma and newline only when the next line starts with an
identifier.

A temporal literal is one token: `@` followed by an RFC 3339 date, time, or
instant, optionally with an RFC 9557 zone annotation (`@2026-09-03`,
`@09:30`, `@2026-09-03T09:30Z`, `@09:30[America/New_York]`,
`@[Europe/London]`), or a number directly followed by a duration unit (`5m`,
`1h30m`). Their grammar and validation rules are in the temporal scalar
section below.

The hard reserved words for the current design surface are:

```text
module use as export abstract impl operator fn struct const requires is let var state inject return if else
start when stop for test assert eval
true false null
bool i64 f64 str date time datetime duration
civil_datetime zoned_datetime zoned_time timezone
```

`_` on its own is the placeholder token, not an identifier. Every word in
that list is reserved everywhere, including the ones such as
`state`, `start`, `stop`, and `when` that are only meaningful at a particular
position in a runtime function body. Variables are introduced by `let`, `var`,
and `state`, and each block keyword carries its own placement rule, so there is
no ambiguity to resolve by making them contextual; they are withheld from
parameter and variable names deliberately to keep a runtime body readable.

`atomic`, `tuple`, `list`, `set`, `map`, and `rolling` are contextual type
keywords, and `unbounded` is a contextual constant in a list-size position.
Outside a type position, the same spelling can resolve to a function,
as in `map(values, fn(value) => value * 2.0)`. `in` is contextual: it is the
separator of a `for` statement, the membership relation in a `requires`
clause, and an ordinary identifier elsewhere. `out` and the names of other
injectables are contextual names resolved only by an `inject` declaration.
`struct` is both a declaration keyword and the corresponding constraint
category. `delta` is contextual: followed by `<` it introduces a structured
delta constructor, while `delta(value)` remains the temporal metadata function.
It is not a general type constructor. `fields`, `has_fields`, and `field_type`
are compile-time reflection intrinsics inside a `requires` clause.

The lexer never produces a `>>` token, so nested generic lists such as
`list<tuple<f64, f64>>` need no spacing. A number followed directly by a
letter is a duration literal candidate: `1h30m` and `2m30.5s` are single
tokens, and `5min` is a `parse` diagnostic naming the unknown unit rather
than a number followed by an identifier.

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

declaration     = struct_decl | operator_decl | function_decl | test_decl;
struct_decl     = [ "export" ], [ "abstract" ], "struct", identifier,
                  [ generic_parameters ],
                  [ ":", struct_parent, { ",", struct_parent } ],
                  [ requires_clause ], "{", [ NL ],
                  [ struct_member, { NL, struct_member }, [ NL ] ], "}";
struct_parent   = named_type;
struct_member   = struct_field | inherited_default;
struct_field    = identifier, ":", type, [ "=", const_expression ];
inherited_default
                = identifier, "=", const_expression;
operator_decl   = "operator", identifier, [ generic_parameters ],
                  function_signature, [ requires_clause ];
function_decl   = [ "export" | "impl" ], "fn", identifier,
                  [ generic_parameters ], function_signature,
                  [ requires_clause ], function_body;

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

requires_clause = "requires", constraint_expression;
constraint_expression
                = constraint_term,
                  { ( "&&" | "||" ), constraint_term };
constraint_term = "!", constraint_term
                | "(", constraint_expression, ")"
                | constraint_relation
                | constraint_call
                | operator_requirement;
constraint_relation
                = constraint_operand, "==", constraint_operand
                | constraint_operand, "in", constraint_operand
                | constraint_operand, "is", identifier;
constraint_operand
                = identifier | type | const_expression | constraint_call
                | constraint_set;
constraint_set  = "{", constraint_operand,
                  { ",", constraint_operand }, [ "," ], "}";
constraint_call = identifier, "(", [ constraint_arguments ], ")";
constraint_arguments
                = constraint_operand,
                  { ",", constraint_operand }, [ "," ];
operator_requirement
                = ( identifier | qualified_name ), "(",
                  [ type, { ",", type } ], ")", [ "->", type ];
```

A function or operator signature with no return arrow is outputless. An
`operator` declaration ends after its optional `requires` clause and cannot
have a body. A temporal parameter cannot have a default in the agreed slice.
`const` marks wiring-time function parameters and wiring-time generic values;
it is not a general local-variable qualifier. `export` applies to a named
ordinary exact `fn` or a `struct`; other declarations reject it. `impl` marks
a named `fn` as an implementation of an operator in scope; the two function
modifiers are mutually exclusive. Operators are public without a modifier.

A struct has a module-qualified nominal identity. Its fields are public,
immutable, and ordered metadata, with newline separators and no semicolons.
Only an `abstract struct` may be named as a parent. Abstract structs are not
constructible and may inherit abstract parents; concrete structs may inherit
one or more abstract parents and are implicitly final. An empty concrete body
is valid. There are no methods, behavior inheritance, visibility modifiers, or
self-recursive fields in the first slice.

Struct generic parameters use the common `generic_parameters` production, and
their trailing `requires` clause uses the same constraint grammar as a function
or operator. A parent application must be fully resolved after substituting the
child's parameters. Generic parameter defaults and partial applications are
not accepted in the initial design.

A typed `struct_field` introduces a field. Its canonical type and optionality
are invariant in every descendant. Its default is checked against the field's
canonical value projection; `null` on the introducing declaration marks the
field optional and initially unset. An untyped `inherited_default` must name an
inherited field and only introduces or replaces that child's construction
default. It cannot change the field type or optionality, and a default cannot
be removed. `null` is accepted as an inherited default only when the original
field is optional.

When multiple parents contribute a field name, type and optionality must agree.
Equal defaults merge; differing defaults or a default supplied by only one
parent require an explicit `inherited_default` in the child. Other conflicts
are diagnosed. The stable field-linearization rule remains open and must be
settled before implementation.

The constraint grammar is intentionally smaller than the general expression
grammar. Its operands are types, generic parameters, wiring-time constants,
and pure compiler-provided reflection operations. It cannot call an arbitrary
language function or inspect a temporal value. Newlines may follow an infix
constraint operator, allowing an aligned multiline `requires` expression.

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
                | ref_type
                | named_type
                | "atomic", "<", value_type, ">";
value_type      = scalar_type
                | value_tuple_type
                | value_list_type
                | set_type
                | value_map_type
                | named_type;
named_type      = ( identifier | qualified_name ), [ generic_arguments ];
generic_arguments
                = "<", generic_argument,
                  { ",", generic_argument }, [ "," ], ">";
generic_argument
                = type | const_expression;
scalar_type     = "bool" | "i64" | "f64" | "str"
                | "date" | "time" | "datetime" | "duration"
                | "civil_datetime" | "zoned_datetime"
                | "zoned_time" | "timezone";
tuple_type      = "tuple", "<", type, { ",", type }, ">";
list_type       = "list", "<", type, [ ",", size_expression ], ">";
size_expression = const_expression | "unbounded";
set_type        = "set", "<", value_type, ">";
map_type        = "map", "<", value_type, ",", type, ">";
rolling_type    = "rolling", "<", value_type, ",",
                  const_expression,
                  [ ",", const_expression ], ">";
ref_type        = "ref", "<", type, ">";
value_tuple_type = "tuple", "<", value_type,
                   { ",", value_type }, ">";
value_list_type = "list", "<", value_type, ">";
value_map_type  = "map", "<", value_type, ",", value_type, ">";
```

A generic argument is initially parsed without deciding whether an identifier
names a type or a wiring-time value. Name resolution interprets each position
from the referenced declaration's type or `const` parameter. Every declared
argument must be supplied; `_` and parameter defaults are not accepted.

A contextual type keyword introduces a container type only when it is
directly followed by `<`; `rolling` alone is a named type and `list(1, 2)`
is a call. A size expression is parsed at additive precedence and above, so
`list<f64, n + 1>` needs no parentheses but a comparison inside a size
does; `unbounded` is the sentinel only when it directly precedes the
closing `>`. Generic parameter lists and tuple types are non-empty.

This grammar permits `atomic<T>` only at a temporal boundary. Nested atomic
values remain expressible because a container's element is itself
temporalized:

```hgl
map<str, atomic<tuple<f64, f64>>>
```

`value_type` excludes `atomic`, `rolling`, and `ref` and is used for `const`
parameters, atomic payloads, map keys, set elements, and rolling values. `type` allows
atomic and reference boundaries recursively inside structural values. `rolling`
is already a temporal endpoint shape and therefore cannot appear under `atomic`
or in a `const` parameter.

`ref<T>` is a temporal reference boundary, not a named scalar type. It is
therefore legal only in positions described by the full `type` production,
including temporal parameters, results, and collection values. It is rejected
where the grammar requires `value_type`: a `const` annotation, an atomic
payload, a map or set key, or a rolling value. `ref` is a contextual type
keyword when directly followed by `<`; otherwise normal name resolution
applies. Reference access and compatibility rules are recorded in
[Imported values, reference types, and SIGNAL inputs](../design/type-extensions.md).

A rolling window is sized by tick count or by duration. The size arguments
are constant expressions, and their type selects the kind: `i64` sizes
describe a tick window and `duration` sizes a duration window.

```hgl
rolling<f64, 20>          // the last 20 values, valid once it holds 20
rolling<f64, 20, 5>       // the last 20 values, valid once it holds 5
rolling<f64, 5m>          // the last five minutes, valid once it spans 5m
rolling<f64, 5m, 1m>      // the last five minutes, valid once it spans 1m
```

Omitting the third argument normalizes the minimum to the maximum for both
kinds. Both arguments must be of one kind; `rolling<f64, 5m, 3>` is a `type`
diagnostic. Tick sizes are positive. A duration maximum is positive and a
duration minimum may be `0s`, the one spelling of a duration window that is
valid from its first value. The minimum cannot exceed the maximum. Size
arguments must be constant expressions formed from literals or in-scope
`const` generics and cannot depend on temporal values; a `const` generic in
a size position has its declared type, `i64` or `duration`, so one generic
declaration accepts one kind.

The window semantics are hgraph's. A tick window holds the most recent
`max_size` values and evicts the oldest when full. A duration window holds
every value whose tick time lies within `max_size` of the evaluation time
and evicts older values before each push; it has no element bound, so its
memory follows the tick rate. A window is invalid, and does not evaluate its
consumers, until it reaches its minimum: a tick window once it holds
`min_size` values, a duration window once the span from its oldest to its
newest value reaches `min_size`. That span is measured over the captured
values, not the run's elapsed time, so a positive duration minimum needs at
least two values. The kind and both resolved sizes participate in type
identity: `rolling<f64, 5m>` and `rolling<f64, 300s>` are one type,
`rolling<f64, 20>` and `rolling<f64, 20s>` are two, and the kinds never
unify. A parameter that accepts either kind (hgraph's `TSWAny` wildcard) has
no spelling yet and is listed under the open questions.

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

Status: agreed in the review of the scaffold (2026-09-03): the eight types,
their literals, and the arithmetic table below. The civil and zoned design
was accepted as written and is the record until the project owner revises a
point; a revision replaces the text here. The type set follows RFC 0002,
plus one hgraph-side addition recorded below.

HGL has eight temporal scalars. Each carries no time zone, a named zone, or is
a zone, and each is an atomic endpoint when temporalized:

| Type | Value | hgraph type | Range and resolution |
| --- | --- | --- | --- |
| `date` | calendar date, proleptic Gregorian | `CivilDate` (`Date`) | `0001-01-01` to `9999-12-31` |
| `time` | time of day | `CivilTime` (`Time`) | `00:00:00` up to but excluding `24:00:00`, microsecond resolution |
| `datetime` | an instant on the UTC timeline | `Instant` (`DateTime`) | signed 64-bit microseconds from the Unix epoch |
| `duration` | signed elapsed time | `Duration` (`TimeDelta`) | signed 64-bit microseconds |
| `civil_datetime` | a date and time with no zone or offset | `CivilDateTime` | local microseconds from the civil epoch |
| `timezone` | a named TZDB zone | `ZoneId` | interned zone name, such as `Europe/London` |
| `zoned_datetime` | an instant with its zone and resolved offset | `ZonedDateTime` | instant, zone, and offset |
| `zoned_time` | a time of day in a named zone | `ZonedTime` (hgraph-side addition) | time of day and zone, no offset |

`datetime` is the type of the engine clock and of `last_modified`. It is not a
civil date-time: it has no local fields to lose, and field accessors such as
`hour` read its UTC fields. A `duration` day is exactly 86 400 seconds, and a
`duration` has no month or year component because those are calendar periods
rather than elapsed time. Leap seconds do not exist in any of the types.

A `civil_datetime` is what a wall clock shows: it names a moment only once a
zone interprets it. A `zoned_datetime` is a resolved moment, so it stores the
instant, the zone, and the offset the zone had at that instant; the offset is
what distinguishes the two occurrences of a repeated hour and what detects a
rule change when the value is read under a later TZDB. Values for the same
instant in two zones are therefore different values, and `same_instant` is
the library function that compares timelines. A `zoned_time` is a wall-clock
time in a named zone with no date and hence no offset, because the offset of
`09:30` in `America/New_York` depends on the day; it resolves to a
`zoned_datetime` only when combined with a `date`. A `timezone` is an interned
TZDB name; TZDB links are not canonicalized, so `US/Eastern` and
`America/New_York` are two distinct values.

`zoned_time` is not part of RFC 0002. It is the language's one hgraph-side
addition to the temporal types: a core scalar (`CivilTime` plus `ZoneId`,
registered name `zoned_time`) with the resolution operations listed below, a
JSON form of the time followed by the bracketed zone, and a Python wrapper.
It is tracked with the other hgraph-side requirements in the roadmap until an
RFC promotes it.

The remaining RFC 0002 types, the calendar period and the ranges, are library
scalars rather than core vocabulary. They stay reachable under their
registered hgraph names (`period`, `instant_range`, `civil_date_range`)
through the scalar surface that extension descriptors will provide; the eight
core names are the only temporal reserved words.

### Literals

`@` introduces a value written in its RFC 3339 form, extended with the
RFC 9557 bracketed zone annotation, and a number immediately followed by a
unit is a duration. The shape selects the type. Shorthand is supported
wherever it stays unambiguous: seconds may be omitted, an offset may omit its
minutes, and a duration may run several units together:

```ebnf
temporal_literal = date_literal | time_literal | datetime_literal
                 | civil_datetime_literal | zoned_datetime_literal
                 | zoned_time_literal | timezone_literal
                 | duration_literal;
date_literal     = "@", calendar_date;
time_literal     = "@", clock_time;
datetime_literal = "@", calendar_date, "T", clock_time, utc_offset;
civil_datetime_literal
                 = "@", calendar_date, "T", clock_time;
zoned_datetime_literal
                 = "@", calendar_date, "T", clock_time, utc_offset,
                   zone_annotation;
zoned_time_literal
                 = "@", clock_time, zone_annotation;
timezone_literal = "@", zone_annotation;
calendar_date    = digit4, "-", digit2, "-", digit2;
clock_time       = digit2, ":", digit2,
                   [ ":", digit2, [ ".", digit1to6 ] ];
utc_offset       = "Z" | ( "+" | "-" ), digit2, [ ":", digit2 ];
zone_annotation  = "[", zone_name, "]";
zone_name        = zone_component, { "/", zone_component };
zone_component   = ( letter | digit | "_" | "-" | "+" | "." ),
                   { letter | digit | "_" | "-" | "+" | "." };
duration_literal = duration_part, { duration_part };
duration_part    = digits, [ ".", digits ], duration_unit;
duration_unit    = "d" | "h" | "m" | "s" | "ms" | "us";
```

```hgl
@2026-09-03                            // date
@09:30                                 // time; seconds default to zero
@09:30:15.250                          // time, 250 milliseconds past the second
@2026-09-03T09:30Z                     // datetime
@2026-09-03T10:30:00+01:00             // the same datetime, with an offset
@2026-09-03T10:30+01                   // the same again, both shorthands
@2026-09-03T10:30                      // civil_datetime: no offset, no zone
@2026-09-03T10:30+01:00[Europe/London] // zoned_datetime
@2026-11-01T01:30-04:00[America/New_York] // the first 01:30 of the fold day
@2026-11-01T01:30-05:00[America/New_York] // the second 01:30 of the fold day
@09:30[America/New_York]               // zoned_time
@[Europe/London]                       // timezone
5m                                     // duration: five minutes
1h30m                                  // duration: ninety minutes, as one token
1.5h                                   // the same value
-250ms                                 // unary minus applied to 250ms
1h + 30m                               // the same value, folded at compile time
```

Every literal is validated and normalized when it is lexed:

- a date must exist in the calendar, so `@2026-02-29` is a diagnostic;
- a time must be earlier than `24:00:00`; `24:00:00` and the leap-second form
  `23:59:60` are diagnostics;
- an instant carries `Z` or an offset and is stored normalized to UTC, and
  that UTC value must itself lie in the years 0001 to 9999 so that every
  instant has a canonical spelling (`@0001-01-01T00:00+23:59` is a
  diagnostic); the
  same shape with no offset is a `civil_datetime`, a different type, so
  `const t: datetime = @2026-09-03T09:30` is a `type` diagnostic rather than
  a value silently read as UTC, and Python's naive-means-UTC convention never
  enters the language;
- a `zoned_datetime` literal carries both an offset and a zone; the offset
  fixes the instant, and the zone is retained with it, so a repeated local
  hour is written unambiguously by its offset and a nonexistent one cannot be
  written at all; the offset-free form `@2026-09-03T10:30[Europe/London]`
  is a diagnostic with a hint to `resolve` the civil value with explicit
  policies, because a literal never chooses a fold or gap policy silently;
- a zone name is validated syntactically by the RFC 0002 rules (ASCII letters,
  digits, `.`, `_`, `-`, `+`, and `/`; no empty, leading, trailing, or
  repeated `/`; no `.` or `..` components; at most 255 bytes); whether the
  zone exists and whether a zoned literal's offset agrees with its zone are
  checked by the run's time-zone provider under hgraph's strict decoding rule,
  so a literal's meaning never depends on the compiler's TZDB;
- `T` and `Z` are upper case, every field present has exactly the digit
  count shown, omitted seconds and offset minutes are zero, and the fraction
  has one to six digits;
- a duration is the sum of its parts, each a decimal number scaled by its
  unit using exact decimal arithmetic; the total must be a whole number of
  microseconds (`0.5us` is a diagnostic) and fit the 64-bit range; units are
  lower case, and `m` is minutes because calendar months are not durations;
- the parts of a duration literal appear in strictly descending unit order,
  each unit at most once, with a fraction only on the last part, and with no
  exponent or internal whitespace: `1h30m` and `2m30.5s` are tokens, `30m1h`,
  `1h1h`, and `1.5h30m` are diagnostics.

Unary minus applies to the literal as an operator; there is no signed literal
token. Because `@` is followed by a fixed digit pattern rather than a run of
operator-free characters, `@2026-09-03-1d` lexes as `date - duration` and
`@2026-09-03T09:30Z-1d` as `datetime - duration`, although both read better
with spaces; an instant whose offset is directly followed by an operand, as in
`@2026-09-03T09:30+01-1d`, needs the spaces. A zone annotation is delimited by
its brackets, so no zone name is ever confused with an operator, and a
bracketed annotation directly after a value is always part of the literal
rather than an index. An identifier directly after a number (`5min`, `2x`) is
an unknown-unit diagnostic rather than a juxtaposition error.

Alternatives considered: bare ISO 8601 tokens conflict with subtraction
(`2026-9-3`) and with named arguments and annotations (`09:30`); typed string
prefixes such as `date"2026-09-03"` read as strings, need one prefix per type,
and defer validation to a later phase; constructor calls (`date(2026, 9, 3)`)
are not literals and cannot appear in a `const` default. The `@` sigil is
otherwise unused in the language, is one lexer rule, and lets the ISO shape
select the type. A bare zone spelling (`@Europe/London`) reads better than the
bracketed one but would make `+`, `-`, and `/` part of a token; the bracketed
form is the RFC 9557 annotation and can be relaxed later, whereas a bare
spelling could not be withdrawn.

### Canonical spelling

Tooling, diagnostics, and constant printing use one spelling per value, and
the lexer accepts every spelling above, so a value round-trips through its
canonical form:

- `date`: `@YYYY-MM-DD`;
- `time`: `@HH:MM`, then `:SS` only when the seconds or fraction are
  non-zero, then the fraction only when non-zero and with trailing zeros
  removed, so `@09:30`, `@09:30:15`, and `@09:30:15.25`;
- `datetime`: the date, `T`, the time in the same shortest form, and `Z`,
  always in UTC; an offset is an input convenience only;
- `civil_datetime`: the date, `T`, and the time in the shortest form;
- `zoned_datetime`: the date, `T`, the time in the shortest form, the offset
  as `Z` when zero and otherwise `±HH` or `±HH:MM` in the shortest form, and
  the bracketed zone, so `@2026-09-03T10:30+01[Europe/London]`;
- `zoned_time`: the time in the shortest form and the bracketed zone;
- `timezone`: `@[Zone]` with the interned name exactly as written;
- `duration`: the non-zero parts in descending unit order with integer counts,
  so `5400s` prints as `1h30m`, `1500000us` as `1s500ms`, `36h` as `1d12h`,
  and zero as `0s`; a negative value prints with a leading `-`, which
  re-parses as unary minus.

These are source spellings. Interchange forms (JSON, Arrow, recording) are
hgraph's RFC 0002 codecs and are not part of the language.

### Arithmetic and comparison

The temporal types have no implicit conversions between them or from numbers.
The defined operations follow RFC 0002 exactly and apply in every phase: in a
composition body they wire the standard hgraph operators, in a runtime body
they are the same checked scalar operations, and in a constant expression they
fold at compile time with identical results. Zoned arithmetic needs the run's
time-zone provider and therefore never folds at compile time.

| Expression | Result | Rule |
| --- | --- | --- |
| `datetime + duration`, `duration + datetime`, `datetime - duration` | `datetime` | checked overflow |
| `datetime - datetime` | `duration` | checked overflow |
| `date + duration`, `date - duration` | `date` | uses the duration's floor-based whole-day component: `d + 36h` is `d + 1d` and `d - 1us` is `d - 1d`, matching Python `date` arithmetic |
| `date - date` | `duration` | a whole number of days |
| `date + time` | `civil_datetime` | combines the fields |
| `civil_datetime + duration`, `civil_datetime - duration` | `civil_datetime` | checked overflow, no zone involved |
| `civil_datetime - civil_datetime` | `duration` | checked overflow |
| `zoned_datetime + duration`, `duration + zoned_datetime`, `zoned_datetime - duration` | `zoned_datetime` | timeline arithmetic: shifts the instant, keeps the zone, re-resolves the offset through the provider |
| `date + zoned_time` | `zoned_datetime` | the wall-clock time on that date in that zone, with the offset the zone has that day; only on a transition day where the time is repeated or skipped does it raise instead of guessing (hgraph's `Reject` fold and gap policies), and `resolve` then takes explicit policies |
| `duration + duration`, `duration - duration`, `-duration` | `duration` | checked overflow |
| `duration * i64`, `duration * f64` | `duration` | floating-point scaling rounds to the nearest microsecond, ties to even |
| `duration / i64`, `duration / f64` | `duration` | as above |
| `duration / duration` | `f64` | ratio |
| `<`, `<=`, `>`, `>=`, `==`, `!=` between two `date`, `time`, `datetime`, `duration`, or `civil_datetime` values of one type | `bool` | chronological order; civil order for `civil_datetime` |
| `==`, `!=` between two `zoned_datetime`, `zoned_time`, or `timezone` values | `bool` | structural: instant, zone, and offset must all agree; `same_instant` compares timelines |

Everything else is a `type` diagnostic, in particular `datetime + datetime`,
`time + duration` and `time - time` (crossing midnight needs a date),
`zoned_datetime - zoned_datetime` (subtract `to_instant` of each), ordering
of `zoned_datetime`, `zoned_time`, or `timezone` values (order `to_instant`
or `to_civil` explicitly, as RFC 0002 requires), `zoned_time + duration`,
`duration * duration`, `%` on any temporal type, and every comparison between
different types. Scaling is written duration first: `2 * cooldown` is a
`type` diagnostic with a hint to write `cooldown * 2`. The operation is
commutative, but one spelling reads better, hgraph registers only that order,
and a permitted spelling is easy to add later and hard to withdraw. A
`duration` constant in a `rolling` size position is not arithmetic: it
selects a duration window, as the type section describes.

Overflow in a constant expression is a compile-time diagnostic. Overflow at
runtime raises through hgraph's checked temporal arithmetic; the language's
runtime scalar error behaviour is an open question shared with the numeric
types.

Field accessors (`year`, `month`, `day`, `weekday`, `hour`, `minute`,
`second`, `microsecond`, `days`, `seconds`, `total_seconds`), `abs`, the
rounding functions (`temporal_floor`, `temporal_ceil`, `temporal_round`), and
the zone operations are ordinary standard-library functions resolved like any
other call, not syntax. The zone operations are hgraph's:
`at_zone(datetime, timezone) -> zoned_datetime`,
`resolve(civil_datetime, timezone, ambiguous:, nonexistent:) -> zoned_datetime`
(hgraph's `resolve_civil`, with the same policy enumerations), and, as a
roadmap ask, `resolve(date, zoned_time, ambiguous:, nonexistent:)`,
`convert_zone(zoned_datetime, timezone)`, `to_instant(zoned_datetime)`,
`to_civil(zoned_datetime)`, `same_instant`, and the accessors `zone_of`,
`offset_of` (a `duration`), `date_of`, and `time_of`. Calendar-period
arithmetic on zoned values is the explicit three-step civil pipeline of
RFC 0002 and uses the `period` library scalar.

## Generics and nominal operator binding

A plain generic parameter ranges over any HGL source type admitted by all of
its occurrences. A `const` occurrence restricts that variable to `value_type`;
a temporal occurrence additionally admits `atomic` and `rolling` types. A
`const` generic parameter binds a wiring-time value and may appear in a
type-shaping position such as a rolling-window size. Generic scope covers the
declaration signature and body or, for a struct, its parents, fields, default
overrides, and `requires` clause. Repeated uses require an equivalent type or
equal constant value.

Every generic needed by a selected implementation must resolve from input
arguments, expected output, or a solvable type equality in a `requires` clause.
Unresolved or inconsistently rebound generics are type diagnostics. Explicit
generic arguments are additionally accepted when applying or constructing a
struct type. Generic parameter defaults and explicit generic arguments on
ordinary function or operator calls remain open. The AST and semantic model
must not assume generic parameters are types only.

Different type-parameter names are independent. Repetition means equality:

```hgl
fn independent<U, V>(a: U, b: V) -> U => a

fn same<U>(a: U, b: U) -> U => a
```

`independent` permits unrelated types for `a` and `b`; `same` requires one
source type for both, including equal atomic boundaries and rolling sizes.
Contextual temporalization occurs after this source-level relationship is
established. A plain generic is consequently neither a time-series-only
variable nor a runtime dynamically typed value.

On a struct declaration, each type parameter is restricted to the canonical
`value_type` domain. `atomic` and `rolling` cannot be supplied as generic
struct arguments; temporal policy belongs in the field declaration, such as
`value: atomic<T>`. Constant parameters retain their declared wiring-time
value type and form part of specialization identity:

```hgl
struct Vector<T, const size: i64> {
    values: list<T, size>
}
```

An applied generic struct is invariant and nominal by both origin and complete
argument list. `Vector<f64, 3>` and `Vector<f64, 4>` differ, as do `Box<Base>`
and `Box<Derived>`. A bare generic origin and a partial application are not
types. The checked IR nevertheless retains HGL source-type arguments rather
than only their native scalar projections, leaving room to relax the
canonical-only restriction in a later language version.

Constructor inference matches supplied named fields and an optional expected
result against the generic field schemas, unifies all bindings, and rejects
any unresolved or conflicting parameter. An explicit constructor application
supplies the complete list. There are no `_` placeholders or generic parameter
defaults in this slice.

Generic abstract parents are applied using the same rules. Each concrete
generic specialization remains final, parent arguments must be complete after
child substitution, and closed abstract-family membership is computed for the
exact parent specialization rather than for every specialization of its
generic origin.

A `requires` clause is a compile-time constraint expression:

```hgl
fn numeric<U>(a: U, b: U) -> U
requires U in {f64, i64}
=> a + b

fn with_prices<U>(value: U) -> f64
requires U is struct
      && has_fields(U, {"bid", "ask"})
=> 0.0
```

The initial constraint vocabulary is:

- `U in {A, B, ...}` restricts a type variable to a closed set;
- `U is category` restricts it to a compiler-known type category;
- `X == Y` states type or wiring-time constant equality;
- `fields(U)` returns the field-name set of a structured source type;
- `has_fields(U, names)` tests that every named field is present;
- `field_type(U, name)` returns the canonical type of a named field;
- `op(A, B) -> O` requires a selected nominal operator to be callable with
  those source types and result relationship.

The same vocabulary constrains a generic struct family. Its declaration is
checked symbolically, and every concrete specialization must satisfy the
constraint before construction, inheritance, or temporal expansion. Thus a
failed `Range<str>` application is a type diagnostic at the application site,
not a partly registered nominal schema.

Within constraints, `struct` denotes the canonical structured-value category
covering concrete and abstract nominal declarations. Reflection includes
inherited fields after hierarchy validation and operates on the canonical
source schema rather than either its atomic or recursively temporalized
representation.

Type equality participates in inference. If exactly one side is an unbound
generic and the other side can be evaluated from known bindings and `const`
values, the compiler binds the generic. If both sides are known, equality
validates them. The compiler repeats these steps to a fixed point; an
unresolved dependency or cycle is diagnosed rather than deferred to generated
C++. Only equalities in a positive conjunction can introduce a binding. An
equality beneath `||` or `!` is an admission predicate and requires both sides
to have been resolved elsewhere.

```hgl
operator get_field<U, V>(value: U, const name: str) -> V
requires U is struct
      && name in fields(U)
      && V == field_type(U, name)
```

In this example the first two predicates establish that field lookup is valid,
and the equality resolves or checks `V`. `fields` is deliberately distinct
from collection traversal with `keys`; constraint reflection operates on
the canonical source type and does not expose its expanded hgraph schema.

An operator requirement is resolved against the nominal operator selected by
ordinary name resolution. It proves that the operation used by a generic body
is valid for the admitted substitution:

```hgl
fn double<U>(value: U) -> U
requires add(U, U) -> U
=> value + value
```

`math::add(U, U) -> U` would select the exact qualified operator identity.
Operator requirements do not search unrelated same-named contracts.

An `operator` declaration introduces a nominal, bodyless callable contract. Its
identity is `(defining module, declaration name)`, not its short name. The
contract owns public parameter names and order, temporal-versus-`const` roles,
defaults, generic input/output relationships, and any public `requires`
clause. Every operator is public by definition; `export operator` is not a
declaration form.

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
public argument roles. Operator requirements are in scope while its body is
checked, and an implementation may add stricter candidate requirements. Its
effective dispatch constraint is the conjunction of the mapped operator and
candidate constraints. The body still passes through ordinary function
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

## Scopes and name lookup

Status: first-pass rule (2026-09-03); the shadowing question of the review
stays open and this section records what the compiler does meanwhile.

An unqualified name resolves, innermost first, to:

1. a `let`, `var`, or `for` binding of the enclosing blocks, declared
   earlier in its block;
2. a parameter or generic parameter of the enclosing function;
3. a declaration of the module: a `fn`, an `operator`, or a `test`
   (a `test` is not a value and is a `name` diagnostic in an expression);
4. a selectively imported operator;
5. a prelude intrinsic: `valid`, `modified`, `all_valid`, `last_modified`,
   `delta`, `key_set`, `keys`, `values`, `items`, `added`, `removed`.

A module alias is only a qualifier: `alias::name` resolves `name` in that
module's public interface and nothing else. Declaring a name twice in one
block, twice among a function's parameters, or twice among a module's
declarations is a `name` diagnostic; an inner binding may shadow an outer
one, including a parameter shadowing a module declaration, so
`first_modified_index(values: list<f64>)` is legal and its body reads the
parameter. An unknown name is `name: unknown name 'x'`.

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
block. It permits multiple function-level `when` blocks and preserves their
source order; a `when` nested in another block is rejected because it cannot
contribute safely to the node's activation policy.
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

The agreed
[conditional-result design](../design/control-flow.md#results-used-after-the-conditional)
extends this baseline with typed, uninitialized declarations such as
`var r: i64`. The declaration introduces the enclosing variable; branch
assignments supply its output connection, and lowering remaps the binding
after the switch. Count any used expression result alongside the escaping
bindings: one result is returned directly; several are returned through a
compiler-generated bundle. The grammar above still
describes the current initializer-required implementation. No default value
or runtime state cell is implied by the new declaration form.

For each escaping result, lowering uses the declared non-`REF` temporal schema
as the common branch-output slot. A branch that forwards an incoming binding
still captures it through `REF`, but explicitly dereferences it before returning
the slot or packing its field into a generated multi-result bundle. REF is not
allowed to leak into only one branch's output schema; branch bundles must match
recursively before they reach the native switch.

The target design also requires definite-assignment analysis for escaping
variables. At a use, every path reaching it must supply a binding, either by
assignment or by forwarding an existing incoming binding. Otherwise reject
the use at compile time; do not synthesize a never-ticking source to fill the
gap. This is static binding analysis, not runtime time-series validity
checking. See [Definite assignment](../design/control-flow.md#definite-assignment).

The statement and expression productions are:

```ebnf
block          = "{", [ NL ], { statement, NL }, [ statement ], "}";
statement      = local_decl | state_decl | inject_decl | lifecycle_block
               | when_statement | for_statement | mutation_statement
               | return_statement | assert_statement | expression;
return_statement
               = "return", [ expression ];

expression     = or_expr;
or_expr        = and_expr, { "||", and_expr };
and_expr       = equality_expr, { "&&", equality_expr };
equality_expr  = comparison_expr, { ( "==" | "!=" ), comparison_expr };
comparison_expr
               = additive_expr,
                 { ( "<" | "<=" | ">" | ">=" ), additive_expr };
additive_expr  = multiplicative_expr,
                 { ( "+" | "-" ), multiplicative_expr };
multiplicative_expr
               = unary_expr, { ( "*" | "/" | "%" ), unary_expr };
unary_expr     = ( "-" | "!" ), unary_expr | postfix_expr;
postfix_expr   = primary_expr,
                 { "(", [ argument, { ",", argument }, [ "," ] ], ")"
                 | "[", expression, "]"
                 | ".", identifier };
primary_expr   = literal | placeholder | identifier | qualified_name
               | "(", expression, ")" | tuple_literal | sequence_literal
               | generic_constructor | delta_expression
               | function_expr | if_expression | eval_expression | block;
generic_constructor
               = ( identifier | qualified_name ), generic_arguments,
                 "(", [ struct_arguments ], ")";
delta_expression
               = "delta", "<", type, ">", "(",
                 [ struct_arguments ], ")";
struct_arguments
               = named_argument, { ",", named_argument }, [ "," ];
named_argument = identifier, ":", expression;
if_expression  = "if", expression, block,
                 [ "else", ( block | if_expression ) ];
```

The final statement of a block is its tail expression when it is an
expression. `if` is a primary expression, so it is an operand only when
parenthesized (`(if c { 1 } else { 2 }) + 1`); as a statement its value is
discarded. A bare `{ ... }` in expression position is a block expression.
The parser accepts `if` uniformly. Its meaning depends on context: a
wiring-time Boolean chooses composition, a temporal Boolean in composition
uses the agreed native switch strategy, and a runtime-node condition is an
ordinary current-value conditional. See
[Conditional control flow](../design/control-flow.md). The temporal composition
case remains unimplemented in the current backends; their rejection is an
implementation limit rather than an unresolved choice of strategy.

Under the agreed temporal composition design, `return` targets the enclosing
HGL function, not a compiler-generated branch lambda. Lowering must identify
early-return paths and place the remaining function body in the non-returning
path's continuation before deriving branch captures and result signatures.
The continuation's computations share that branch's lifetime. Definite
assignment considers only paths reaching a use, excluding paths that return
before it. See [Early returns](../design/control-flow.md#early-returns-and-continuations).
This is a target lowering requirement, not implemented backend behavior.

An outputless temporal conditional uses the native sink-switch path without a
synthetic output. Sinks inside the branch are wired through the switch; sinks
outside it remain unconditionally wired. The graph body still describes
wiring, and the sink nodes perform runtime effects. Result and escape analysis
determines whether a switch is outputless, independently of the enclosing
function's return annotation. See
[Outputless conditionals](../design/control-flow.md#outputless-conditionals).

Expression results and escaping assignments may coexist in one conditional.
Include both in the generated branch output signature and remap each to its
consumer. The binding receiving the whole expression result is not an
escaping variable; only the variables assigned inside the branches require
prior declarations. This uses the existing expression and assignment syntax;
no source-level bundle declaration or reserved result-field name is needed.
See [Mixed results](../design/control-flow.md#expression-results-and-escaping-assignments).

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

```ebnf
argument         = [ identifier, ":" ], expression;
sequence_literal = "[", [ sequence_element,
                   { ",", sequence_element }, [ "," ] ], "]";
sequence_element = expression
                 | ( duration_literal | datetime_literal ), ":",
                   expression;
tuple_literal    = "(", expression, ",",
                   [ expression, { ",", expression } ], [ "," ], ")";
placeholder      = "_";
```

A qualified callee begins with an alias introduced by `use module.path as
alias` and uses `::` between namespace and declaration names. Dots remain the
syntax of canonical module paths in `module` and `use` declarations; they are
not expression member access. A named argument is an identifier directly
followed by `:`, and a timed sequence element is a temporal literal directly
followed by `:`. A sequence literal is a constant `list` value
of one element type, and a tuple literal a constant tuple; a single
parenthesized expression is grouping, so a one-element tuple needs the
trailing comma and `()` is a diagnostic. Timed elements and the `_`
placeholder are valid only in the harness sequences of the evaluation
section below.

Duplicate names, unknown names, positional arguments after named arguments,
and missing required arguments are source diagnostics.

A call whose callee resolves to a struct type is a complete-value constructor
and accepts named arguments only. Required fields must be supplied, ordinary
defaults fill omitted fields, and a field declared with `= null` may remain
unset. The literal `null` is accepted only when the expected field is optional;
it is not an untyped runtime object.

An explicitly applied constructor such as `Box<f64>(value: 1.5)` must supply
every generic argument. The `name<...>(...)` syntax is reserved for struct
construction in the initial design; a callee that resolves to an ordinary
function or operator is diagnosed because explicit generic function-call
syntax remains open. Without the list, constructor inference binds parameters
from the expected result and supplied fields, unifies repeated occurrences,
then evaluates the struct's `requires` clause. Every parameter must resolve;
`Maybe()` without either a type-bearing field or an expected `Maybe<T>` type is
an inference error.

`delta<S>(...)` requires a fully applied nominal struct `S`, accepts named
fields only, and
produces a contextual update value rather than an ordinary source type. Every
field may be omitted independently of the complete constructor's requirements
or defaults. Omission means no change and does not apply a default. Explicit
`null` means clear an optional field and is distinct from omission; it is an
error for a required field. Delta constructors may appear only where runtime
output, a harness or replay sequence, or a temporary `let` binding supplies an
expected delta shape. They are not admitted as function parameter, function
result, state, collection-element, or struct-field types.

## Canonical temporalization

The frontend first resolves a canonical value type, then expands it in temporal
context:

```text
temporalize(bool | i64 | f64 | str)
    = atomic hgraph endpoint carrying that scalar

temporalize(date | time | datetime | duration | civil_datetime
            | zoned_datetime | zoned_time | timezone)
    = atomic hgraph endpoint carrying that temporal scalar

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
      with resolved sizes Max and Min, both tick
      counts or both durations

temporalize(struct S fields)
    = named structural bundle S whose fields are temporalized recursively

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
the same children are structurally equal. A concrete struct instead resolves
to one module-qualified named `Bundle` for scalar use and one matching named
`TSB` whose field schemas are obtained by recursive temporalization. Its
nominal name remains part of both identities. `atomic<S>` resolves to
`TS<Bundle<S>>`.

A fully applied generic struct first substitutes and validates every type and
constant argument, then follows the same rule. The nominal specialization
identity contains the module-qualified origin and complete invariant argument
list, while its fields contain the substituted canonical schemas. Recursive
temporalization starts only after specialization, so `SnapshotBox<Quote>` can
place `atomic<Quote>` at its declared field boundary without admitting
`atomic<Quote>` as a generic argument.

An abstract struct contributes hierarchy metadata and a fixed base-field TSB,
but no constructible scalar instance. Scalar and atomic uses of the abstract
name accept its registered final concrete descendants through hgraph's closed
polymorphic value plan. Temporalization never changes bundle shape at runtime,
so converting a concrete temporal bundle to an abstract base bundle requires
an explicit graph projection.

An `atomic` marker nested in a struct or container stops expansion only at that
point. In the canonical scalar projection the marker contributes the canonical
value of its payload, not another wrapper. This is why
`map<str, atomic<Quote>>` becomes a temporal map of atomic Quote snapshots while
the same field inside a scalar struct remains a canonical map of Quote values.

Optionality and constructor defaults are separate source metadata rather than
new Bundle field schemas. The introducing declaration fixes optionality; the
effective descendant default determines whether a constructor argument may be
omitted. A non-optional field may therefore have a default, and an optional
field may have a non-null default while remaining clearable. A `null` optional
field is represented by the Bundle field validity bit being unset. A
structural delta derives separately from the expanded temporal shape, so every
field is omittable regardless of that metadata.
Indexing a structural tuple with a literal index is positional field access. A
homogeneous
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

In runtime evaluation, `keys`, `values`, and `items` produce evaluation-local
iterator types. They accept the collection followed by an optional predicate:

```ebnf
collection_iterator
               = ( "keys" | "values" | "items" ), "(", expression,
                 [ ",", expression ], ")";
```

The calls are parsed as ordinary call expressions. The grammar above records
their checked intrinsic shapes rather than adding special parser nodes.
Under the agreed phase-dependent design, these calls no longer force the
containing function to be a `RuntimeFn`. This section describes their runtime
interpretation: the iterator must be consumed directly by `for`; it is neither
a canonical value nor a temporal port and cannot escape the current
evaluation. In graph composition, a supported wiring-time iterable provides
scalar values and a fixed temporal structure provides child connections.
Independent dynamic graph-loop bodies lower through per-key or per-index
mapping. The initial dynamic subset rejects assignments to enclosing variables
and loop-carried reductions; it must not silently change the function's phase.
Unordered map reduction and ordered, linear list reduction are deferred
options, not initial lowering support. See [Iteration](../design/iteration.md)
for the target design and its separate compiler implementation work.

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

The implemented provisional classifier currently applies these rules:

1. A body containing no node-only construct and no iteration becomes
   `CompositionFn`.
2. The presence of `state`, `inject`, `start`, `when`, or `stop` makes the
   complete function a `RuntimeFn`, even when nested syntax is later rejected
   by phase checking. The current compiler also treats every `for` statement as
   a runtime-classification trigger.
3. A body that mixes wiring-only and runtime-only constructs is rejected.

The target classifier will instead let iteration inherit its containing phase;
`for` will not by itself distinguish composition from runtime behavior. That
change is an explicit compiler migration rather than current behavior. It also
leaves iterator-only runtime functions ambiguous because they contain no
existing node-only construct. The language must resolve that boundary before
the classifier migration, but this document does not invent an explicit phase
marker or another disambiguation rule.

Classification is based on resolved source syntax. It must not be guessed
from which imported overload happens to win, inferred from generated C++, or
changed between scripted and AOT modes.

After classification, phase and effect checking gives identifiers different
meanings in the two phases. A temporal parameter is a port in a composition
body. In a runtime expression it denotes the current admitted payload, while
`modified(parameter)`, `valid(parameter)`, `last_modified(parameter)`, and
`delta(parameter)` retain access to its endpoint metadata.

Runtime validity checking is flow-sensitive and follows Boolean short-circuit
order. A payload read is valid when the node's ordinary no-`when` policy admits
the input, or the read is dominated by a successful `valid(input)` check in an
enclosing `when` or `if`. Otherwise it is a diagnostic; in particular,
`value > 0.0 && valid(value)` is not safe, while the reversed order is.

Multiple `when` blocks execute as independent ordered conditions. Classification
and phase checking derive one safe node policy across the complete body:

- the active input set is the union of inputs that can activate any handler;
- node-level required validity contains only requirements common to every
  executable handler;
- handler-specific activation, validity, and other predicates remain ordered
  runtime conditions.

A function classified as runtime by another node-only construct but containing
no `when` uses hgraph's default policy: every ordinary temporal input is active
and required-valid. This does not by itself classify an otherwise ordinary or
iterator-only body as runtime.

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

For a structural result, returning a complete struct writes every field while
returning `delta<S>(...)` writes only the named fields. An omitted delta field
does not tick. At an atomic boundary a tick is a complete canonical value, so a
sparse delta cannot be returned for `atomic<S>` without explicit stateful
patching.

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

Assigning `delta<S>(...)` to `out` performs the same sparse structural update
and continues evaluation. Explicit `null` on an optional delta field requests
field invalidation; it must not be lowered as the absent field used for “no
change”. The current native delta value uses an unset scalar child for
omission, so this clear operation requires a distinct public hgraph mutation or
delta encoding before generated code may claim support for it.

The classifier applies to named and anonymous functions wherever their body
grammar admits runtime constructs. The first concise anonymous-function slice
has no runtime block and therefore produces composition helpers only.

## Tests and the evaluation harness

Status: proposed (2026-09-03) so that the first compiler pass can execute
programs; the project owner revises the spelling as needed.

A `test` declaration is a named wiring-time block that exercises functions
of its module with hgraph's evaluation harness:

```hgl
module examples.quotes

fn midpoint(tob: tuple<f64, f64>) -> f64 =>
    (tob[0] + tob[1]) / 2.0

test midpoint_ticks {
    assert eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) == [1.5, 2.5]
}

test midpoint_waits_for_both_sides {
    assert eval(midpoint, tob: [(1.0, _), (_, 3.0), (2.0, _)])
        == [_, 2.0, 2.5]
}
```

```ebnf
test_decl        = "test", identifier, block;
assert_statement = "assert", expression;
eval_expression  = "eval", "(", expression, { ",", argument }, ")";
```

`test`, `assert`, and `eval` are hard reserved words. `_` on its own is the
placeholder token rather than an identifier; identifiers may still begin
with an underscore. A `test` block sees its module's scope, including
unexported functions, so tests live beside the code they cover; a test in
another module sees only that module's public interface. Test names are
unique within a module. A `test` body is a composition-phase block plus
`assert` statements: `state`, `inject`, lifecycle, and `when` forms are
`phase` diagnostics there. Test declarations never lower into the module's
artifact; `hgl test` discovers and runs them, and `hgl build` omits them.

`eval` is syntax, not a function, because its arguments are typed by the
callee. The first argument names a function or operator, unqualified or
qualified; the remaining arguments bind to that callee's parameters with the
ordinary positional-then-named call rules. A `const` parameter receives a
constant expression. A temporal parameter receives a *harness sequence*: a
sequence literal whose elements are the per-cycle deltas of that input, with
`_` for a cycle in which the input does not tick. An element's shape follows
the parameter's canonical type, so a `tuple<f64, f64>` parameter takes tuple
literals whose positions are values or `_` (that field did not tick). The
result of `eval` is the harness sequence of the callee's output, compared
with `==` against a sequence literal of the same shape. An outputless callee
may still be evaluated as a statement, which runs it to completion; its
result cannot be compared.

Elements of a dense sequence are consecutive engine cycles: element `i` is
the cycle at the run's start plus `i` engine steps, hgraph's `eval_node`
alignment, so a test written this way means the same as the equivalent
Python or C++ harness test. The observed output sequence has one element per
cycle from the first cycle through the later of the last input cycle and the
last output tick, with `_` where the output did not tick, and `==` requires
equal length and element-wise equality under hgraph's canonical delta
equality (`Value::equals`; scalars compare exactly).

A *timed* sequence places each element at an explicit time: a `duration`
key is an offset from the run's start and a `datetime` key is an absolute
instant. Keys strictly increase, every element of a timed sequence is timed,
`_` is not permitted (an untimed cycle is simply absent), and every input and
the expected output of one `eval` are either all dense or all timed:

```hgl
test recent_mean_spans_five_minutes {
    assert eval(recent_mean, price: [0s: 1.0, 2m: 3.0, 5m: 5.0, 9m: 7.0])
        == [5m: 3.0, 9m: 5.0]
}
```

A timed run seeds hgraph's absolute-time replay buffers and records sparsely,
so a timed expected sequence lists exactly the ticks the output produced, at
their times. The run's start is hgraph's simulation origin unless a
`datetime` key fixes it; the run ends when nothing remains scheduled. An
explicit end bound and approximate float comparison are open. The first
compiler pass runs dense sequences only; a timed sequence is a `test`
diagnostic until the sparse harness lands.

A literal in a harness sequence takes the parameter's scalar type: an
integer literal in an `f64` position is the corresponding `f64`, and any
other mismatch is a `type` diagnostic naming the parameter. An expected
element is read the same way against the callee's result type.

`assert` accepts any wiring-time `bool` expression. A failing assertion
reports the first differing cycle or time with the expected and observed
elements. A harness sequence is a value only inside a `test` body: it can be
bound with `let` and compared, but it cannot be passed to a function or
placed in a temporal position, and `eval` outside a `test` body is a `phase`
diagnostic. Outside a `test` body a sequence literal is a constant `list`
value of one element type and a tuple literal a constant tuple; both reject
`_`. Constructing a structural tuple from temporal values with a tuple
literal, and delta spellings for set, map, and list elements in harness
sequences, share the open delta-shape question below.

## Running a module

An *entry* is an `export fn` with no temporal parameters. Its `const`
parameters, with or without defaults, are bound from the run configuration,
and its result, if any, is the run's output. The language has no `main`, no
`run(fn, config)` expression, and no in-source run configuration: a module
describes graphs, and a run binds one entry to an evaluation mode, a clock,
and parameter values from outside the source, so the same module runs
unchanged as a simulation backtest and as a real-time process. Those
alternatives were considered and rejected for that reason.

```text
hgl run path/to/program.hgl [--entry name] [--mode sim|realtime]
        [--start <datetime>] [--end <datetime|duration>]
        [--set name=<constant expression>]... [--config run.toml]
```

When a module has exactly one entry it is the default; otherwise `--entry`
is required. `--set` values are HGL constant expressions checked against the
parameter's declared type. The configuration file mirrors the command line
and the command line overrides it:

```toml
[run]
entry = "main_graph"
mode = "realtime"
start = 2026-09-03T08:00:00Z
end = "1d"

[run.params]
window = 20
symbols = ["AAPL", "MSFT"]
```

TOML integers, floats, strings, booleans, offset date-times, local dates,
local times, local date-times, and arrays bind to `i64`, `f64`, `str`,
`bool`, `datetime`, `date`, `time`, `civil_datetime`, and `list` parameters;
a string binds to any temporal parameter type through the HGL literal
spelling (`"1d"`, `"09:30[America/New_York]"`). Defaults are hgraph's
`run_graph` defaults: a simulation starts at the engine origin and ends when
nothing remains scheduled; a real-time run starts now and ends at `--end` or
on interruption. Each tick of the entry's output is written as a `time value`
line, the time in the canonical `datetime` spelling without its `@`. The
configuration file format is versioned with the command. The first compiler
pass implements the command line without `--config`.

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
- `requires` expressions and their type, constant, and operator operands;
- `atomic` boundaries in types;
- rolling-window size arguments and omitted minimum-size syntax;
- list sizes, including the `unbounded` sentinel;
- nominal operator declarations and explicit `impl` bindings;
- explicit `export` on ordinary exact functions;
- selective imports, module aliases, and qualified references;
- `test` declarations, `assert` statements, `eval` forms, and dense or timed
  harness sequences with `_` placeholders;
- `let` versus `var` local mutability;
- concise versus block function bodies;
- tail expressions and explicit returns;
- state and grouped inject declarations;
- `start`, ordered `when`, and `stop` blocks;
- collection traversal calls, predicate arguments, and `for` patterns;
- complete and projected output assignments;
- explicit versus context-inferred anonymous types.

Parser recovery synchronizes at closing braces, `export`, `impl`,
`operator`, `fn`, `test`, and top-level newlines: a bad statement is
skipped to the end of its line or the closing `}` of its block, a bad
declaration to the next line that starts a declaration, and each bad
construct produces one diagnostic. A reserved word in a name position is
reported and then taken as the name so the enclosing declaration is still
built. Rendered diagnostics are ordered by source position.

## Diagnostics

Suggested categories are `parse`, `name`, `type`, `shape`, `constraint`,
`function-kind`, `phase`, `injectable`, `operator`, `module`, and `build`.

Examples:

```text
type: 'atomic<T>' is not valid on const parameter 'settings'
shape: map key type 'list<str>' is not a supported canonical key
constraint: U resolved inconsistently as f64 and i64
constraint: cannot resolve V because field_type(U, name) depends on unresolved U
constraint: type Order does not contain required field 'ask'
constraint: operator requirement add(Order, Order) -> Order has no implementation
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
type: '@2026-09-03T09:30' is a civil_datetime, not a datetime; add an offset
parse: '@2026-09-03T10:30[Europe/London]' has no offset; add it or use resolve()
parse: '[Europe//London]' is not a valid zone name
parse: '0.5us' is not a whole number of microseconds
parse: '5min' has unknown duration unit 'min'; units are d h m s ms us
parse: '30m1h' lists duration units out of descending order
type: 'i64 * duration' is not defined; write the duration first
type: 'datetime + datetime' is not defined; subtract two instants for a duration
type: zoned_datetime is not ordered; compare to_instant() or to_civil()
type: cannot compare 'date' with 'datetime'
module: cannot remove provider user.money while 2 live graphs retain it
type: rolling minimum size 25 exceeds maximum size 20
type: rolling sizes must both be tick counts or both be durations
type: rolling minimum span 10m exceeds maximum span 5m
phase: eval is only valid inside a test body
type: timed and dense sequences cannot be mixed in one eval
test: midpoint_ticks failed at cycle 1: expected 2.0, observed _
```

No-match and ambiguity diagnostics attach hgraph's candidate rejection reasons.

## Other semantic questions

Before code generation, an RFC must also define:

- `i64` overflow and conversion behavior;
- division by zero and NaN comparison;
- complete string escape and Unicode normalization rules;
- self-recursive fields, destructuring, and copy-with-update syntax;
- explicit generic arguments on function and operator calls, generic parameter
  defaults, partial generic type application, and specialization relationships
  beyond invariant applied types and the defined pattern ranking and ambiguity
  rule;
- general anonymous capture and type inference beyond inline iterator
  predicates;
- callable scalar kernels inside runtime functions;
- collection delta constructors and a first-class public native encoding for
  explicitly clearing an optional TSB field;
- rolling-window iteration over hgraph's window view (`values`,
  `time_values`, `value_times`, `removed_value`), which both window kinds
  share, and a parameter spelling that accepts either kind (hgraph's
  `TSWAny`);
- ephemeral caches, lifecycle output access, and runtime sinks;
- runtime scalar error behavior;
- an explicit end bound and approximate comparison for `eval`, delta
  spellings for set, map, and list harness elements, and tuple construction
  from temporal values.
