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

The hard reserved words for the current design surface are:

```text
module use fn const let var state inject return if else
start when stop for
true false
bool i64 f64 str datetime
```

`atomic`, `tuple`, `list`, `set`, and `map` are contextual type keywords.
Outside a type position, the same spelling can resolve to a function, as in
`map(values, fn(value) => value * 2.0)`. `out` and the names of other
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

use_decl        = "use", module_path, "::", import_set;
import_set      = "{", identifier, { ",", identifier }, [ "," ], "}";

declaration     = function_decl;
function_decl   = "fn", identifier, function_signature, function_body;

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

A function with no return arrow is outputless. A temporal parameter cannot
have a default in the agreed slice. `const` is a parameter modifier, not a
general variable qualifier.

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
                | identifier
                | "atomic", "<", value_type, ">";
value_type      = scalar_type
                | value_tuple_type
                | value_list_type
                | set_type
                | value_map_type
                | identifier;
scalar_type     = "bool" | "i64" | "f64" | "str" | "datetime";
tuple_type      = "tuple", "<", type, { ",", type }, ">";
list_type       = "list", "<", type, ">";
set_type        = "set", "<", value_type, ">";
map_type        = "map", "<", value_type, ",", type, ">";
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

`value_type` excludes `atomic` and is used for `const` parameters and atomic
payloads. `type` allows atomic boundaries recursively inside structural values.

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
```

Duplicate names, unknown names, positional arguments after named arguments,
and missing required arguments are source diagnostics.

## Canonical temporalization

The frontend first resolves a canonical value type, then expands it in temporal
context:

```text
temporalize(bool | i64 | f64 | str)
    = atomic hgraph endpoint carrying that scalar

temporalize(datetime)
    = atomic hgraph endpoint carrying an engine timestamp

temporalize(tuple<T...>)
    = structural tuple of temporalize(T) children

temporalize(list<T>)
    = structural list of temporalize(T) children

temporalize(set<T>)
    = set-valued hgraph endpoint carrying canonical T members

temporalize(map<K, V>)
    = keyed temporal map with canonical key K
      and temporalize(V) values

temporalize(record fields)
    = structural bundle of temporalized fields

temporalize(atomic<T>)
    = one atomic endpoint carrying canonical value T
```

`const x: T` bypasses `temporalize` and resolves to canonical value `T`.
`const x: atomic<T>` is invalid because atomicity describes a temporal
boundary.

The compiler must map every expanded shape to an existing public hgraph schema.
It must not create a language-only runtime representation. Exact mapping of
heterogeneous tuples remains open.

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

In a runtime function, `modified` and `valid` inspect evaluator-local endpoint
metadata rather than construct Boolean time series. Both require at least one
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
| TSB | `keys`, `values`, `items` | `modified` for values/items |
| TSD | `keys`, `values`, `items` | `added`, `modified`, `removed` |
| fixed TSL | `values`, `items` | `modified` |
| unbounded TSL | `values`, `items` | `added`, `modified`, `removed` |
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
scalar expressions, but not current temporal input values.

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

## Imported function resolution

An imported name is a function contract. The compiler passes resolved temporal
shapes, canonical `const` values, and source call arguments to hgraph's
operator resolver. The result includes:

- selected candidate identity and implementation kind;
- normalized positional and named arguments;
- resolved input and output schemas;
- defaults and admitted scalar lifts;
- candidate rejection reasons.

The language compiler must not copy hgraph's ranking algorithm or use source
syntax to expose graph-versus-node implementation details at call sites.

## AST requirements

Every token and AST node retains a half-open source range. The AST preserves:

- comments as source trivia;
- literal spelling;
- positional and named argument order;
- `const` on parameters;
- `atomic` boundaries in types;
- `let` versus `var` local mutability;
- concise versus block function bodies;
- tail expressions and explicit returns;
- state and grouped inject declarations;
- `start`, ordered `when`, and `stop` blocks;
- collection traversal calls, predicate arguments, and `for` patterns;
- complete and projected output assignments;
- explicit versus context-inferred anonymous types.

Parser recovery should synchronize at closing braces, `fn`, and top-level
newlines.

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
```

No-match and ambiguity diagnostics attach hgraph's candidate rejection reasons.

## Other semantic questions

Before code generation, an RFC must also define:

- `i64` overflow and conversion behavior;
- division by zero and NaN comparison;
- complete string escape and Unicode normalization rules;
- tuple-to-hgraph structural mapping;
- general anonymous capture and type inference beyond inline iterator
  predicates;
- callable scalar kernels inside runtime functions;
- remaining recursive metadata and `delta` result shapes;
- ephemeral caches, lifecycle output access, and runtime sinks;
- runtime scalar error behavior.
