# Imported values, reference types, `signal` inputs, and enums

Status: agreed source semantics, 2026-09-05; explicit `ref<T>` parsing, type
checking, metadata, descriptors, and generated reference-routing nodes are
implemented. The lowercase `signal` input marker is also implemented through
parsing, semantic checking, descriptor validation, direct-wiring type
materialization, generated C++, and scripted runtime behavior tests.
Wiring-time access through a reference and imported native types remain
compiler work. The collection-reference mapping noted below still needs
clarification. This record introduces no native declaration syntax.

## Enum types

Status: enum declarations, qualified member references, explicit/automatic
numbering within the signed `i64` range with compile-time overflow errors,
member-name stringification, rejection of duplicate numbers,
distinct enum identity, explicit integer conversion, checked construction from
integers or strings through the enum type name, and the `keys`, `values`, and
`elements` calls on enum types returning immutable fixed-size scalar lists
are agreed, 2026-09-06. Members are
constant switch case values. Duplicate resolved switch cases are rejected;
covering all declared members establishes exhaustiveness, while partial
switches remain permitted with the existing no-match failure. String conversion
uses the agreed Python-style `str(value)` spelling. Remaining conversion details and native
mapping are still open; compiler support is not implemented.

The agreed declaration form is:

```hgl
enum Mode {
    first = 10,
    second,
    third = 20
}
```

A member is referenced as `Mode::first`, including `case Mode::first:` in a
switch. The declaration resolves `first` to `10`, `second` to `11`, and `third`
to `20`.

The numbering rules are:

- `member = constant` assigns an explicit integer constant value.
- Assigned numbers must be in the inclusive signed `i64` range,
  `-9223372036854775808` through `9223372036854775807`. Negative numbers and
  both endpoints are permitted.
- An unnumbered first member starts at zero.
- Every later unnumbered member takes the immediately preceding member's
  resolved number plus one. Explicit assignments therefore reset the next
  automatic number; numbering does not depend on the largest number used.
- An explicit number outside the range, or an automatic successor past its
  maximum, is a compile-time error. Numbering never wraps or clamps.
- An explicit assignment after the maximum may restart numbering at any
  in-range value, subject to the duplicate-number rule. Do not compute the
  unused automatic successor before applying that explicit assignment.
- Duplicate resolved numbers within one enum are rejected initially, whether
  the collision comes from explicit or automatic numbering. Numeric aliases
  are not admitted in this first design.

For example:

```hgl
enum Direction {
    reverse = -1,
    stopped,
    forward
}
```

The assigned numbers are `-1`, `0`, and `1`. The
[range and overflow examples](../developer-guide/enum-cpp-mappings.md#signed-range-and-overflow)
also cover both endpoints, a reset after the maximum, and rejected values.
The frontend must admit the complete signed minimum literal without first
requiring its positive decimal magnitude to fit in `i64`. Validate the
resolved signed number and each required automatic increment before native
emission; do not inherit C++ literal or overflow behavior accidentally.

This fixes the source enum number domain, not the physical native ABI or a
new backing-type annotation. It does not settle overflow for unrelated runtime
integer arithmetic or native enum import mapping.

Stringification returns the declared member name without a type prefix or
numeric value: `Mode::first` becomes `"first"`, `Mode::second` becomes
`"second"`, and `Mode::third` becomes `"third"`. Rejecting numeric aliases keeps
that name unambiguous within an enum. The source call uses Python-style
`str(value)`:

```hgl
const first_mode_name: str = str(Mode::first)
```

This produces the constant string `"first"`. The spelling follows Python;
it does not change the agreed enum representation to Python's qualified enum
display or require Python execution. `str` remains the string type name in
type positions and is the conversion operation in this expression position.
See [string conversion](../user-guide/types-and-expressions.md#string-conversion)
for constant, node, and temporal graph use.

[Numbered source examples and C++ mappings](../developer-guide/enum-cpp-mappings.md)
show explicit values, automatic values starting at zero, stringification,
and duplicate-number errors. The positive and intentionally invalid HGL
declarations are also in the [standard-library design corpus](../../stdlib/README.md#enum-values).

Enums should let source give names to the values used by a selector and its
cases, rather than relying on unexplained integers or strings. Their members
must be usable as source constants under the
[switch case-value rule](switch.md#source-form-and-constant-case-values).
Using a named member as a case label does not make the selector wiring-time;
node dispatch and temporal graph switching still follow the selector's phase.

### Enum identity and explicit conversion

An enum remains a distinct atomic scalar type, not an integer alias. Its
members retain that enum's identity even when another enum uses the same
names or assigned numbers. There is no implicit conversion to an integer or
to another enum. Equality and switch matching therefore require the same
enum type; an integer case label is not a substitute for an enum member.

Obtaining the assigned integer is an explicit conversion, using a type-name
call in the same style as `str(value)`. For example, converting `Mode::first`
to an integer produces `10`, while string conversion produces `"first"`.
The precise integer conversion spelling needs confirmation before adding a
source example. This does not authorize implicit arithmetic.

### Constructing an enum from a number or name

Use the enum type as the callee, with one integer or string argument:

```hgl
const mode_from_number: Mode = Mode(10)
const mode_from_name: Mode = Mode("first")
```

Both results are `Mode::first`. An integer is looked up by assigned number,
not ordinal position. A string is looked up by exact declared member name,
with the same spelling returned by `str` on that member. Names are
case-sensitive; numeric text and qualified display text are not alternate
names. For example, `Mode(12)`, `Mode("First")`, `Mode("10")`, and
`Mode("Mode::first")` fail for this declaration.

Unknown numbers or names cause a conversion error; conversion never creates
an unnamed member or substitutes another member. Failure follows the phase in
which the operand's value is available:

- An invalid constant is a checking error.
- An invalid wiring-time scalar is a wiring error.
- An invalid runtime value is an evaluation error. Inside a node the lookup
  runs locally; in graph composition a temporal operand wires a checked
  conversion and the lookup runs when that input is evaluated.

The result retains the target enum type. Conversion does not select the
containing function's phase, bypass input validity or REF/SIGNAL restrictions,
or turn failure into a no-tick result. A switch `default` handles an unmatched
selector, not an error while converting that selector.

This settles integer/string conversion through `Mode(...)`, not a general
type-constructor API, additional argument forms, or the handling of an
already-typed native enum value arriving through an import boundary. The
[paired HGL/C++ conversion examples](../developer-guide/enum-cpp-mappings.md#checked-conversion-into-an-enum)
include successful lookups and rejected constants. Compiler support remains
separate work.

### Enumerating members

Call the enumeration operations on the enum type. For the three-member `Mode`:

| Call | Scalar result type | Contents |
| --- | --- | --- |
| `keys(Mode)` | `list<str, 3>` | `"first"`, `"second"`, `"third"` |
| `values(Mode)` | `list<i64, 3>` | `10`, `11`, `20` |
| `elements(Mode)` | `list<Mode, 3>` | `Mode::first`, `Mode::second`, `Mode::third` |

Each result is an immutable, fixed-size scalar list. Its length is the number
of declared members. Names match `str` on each member; numbers are assigned
values, not ordinal positions; enum elements preserve the enum's identity.
The operand is the type, not a current enum time-series value.

```hgl
const mode_keys: list<str, 3> = keys(Mode)
const mode_values: list<i64, 3> = values(Mode)
const mode_elements: list<Mode, 3> = elements(Mode)
```

These are ordinary constant data, not time-series inputs or evaluation-local
borrowed iterators. They may be bound, indexed, reused, and iterated during
graph wiring. Such a loop receives known scalar constants, not child temporal
connections. The same data remain scalar when used inside a node. The calls
do not manufacture ticks or classify the containing function as a node.
The native constant-storage representation remains an implementation choice.

All three views iterate in declaration order, not numeric or alphabetical
order. Explicit numbering does not reorder members. The views remain aligned:
each position exposes the name, assigned number, or typed instance of the same
declared member. The [out-of-order numbering example](../developer-guide/enum-cpp-mappings.md#declaration-order-enumeration)
makes this distinction explicit.

`elements` is also the agreed element-iteration spelling for lists and sets;
see [collection iteration](iteration.md#elements-for-lists-and-sets). This does
not add `elements` for maps or bundles. Enum-type calls return the constant
lists above; collection-value calls retain their own phase and borrowed-view
rules. This agreement covers the one-argument enum-type forms and does not
introduce a new dynamic `for` lowering or change temporal collection traversal.
Compiler support for enum enumeration remains separate work.

### Switch checks

An enum selector admits case constants of that same enum. Resolve constants
before rejecting duplicate cases: `Mode::first` and `Mode(10)` select the
same member, even though the source expressions differ. Covering all declared
members establishes exhaustiveness and needs no default. Partial coverage is
permitted, with a supplied default handling unmatched members and no-match
failure otherwise. Generated dispatch retains that failure path even for
exhaustive coverage. These rules apply equally to node and graph forms and
do not replace definite-assignment checks on successful branches. See
[switch coverage](switch.md#duplicate-cases-and-enum-coverage) and the
[paired HGL/C++ examples](../developer-guide/enum-switch-cpp-mappings.md).

### Remaining enum decisions

The next design discussion needs to settle:

- treatment of already-typed native enum values not associated with a declared
  member; checked integer/string construction itself rejects unknown values;
- the exact spelling for conversion from an enum to its assigned integer;
- exposure of native C++ and Python enums without losing their type identity;
- the concrete native enum switch-key representation and its integration with
  the public wiring API.

Checked conversion timing for constant, wiring-time, and temporal operands is
agreed above; it is not an open phase decision. Native representation and
import-boundary work must preserve those semantics.

The existing native [enum registration contract](../../../include/hgraph/types/metadata/type_registry.h)
accepts an ordered member-name/assigned-integer table. Its
[enum value operations](../../../src/hgraph/types/metadata/type_registry.cpp)
currently stringify a known number using its member name, and fall back to
numeric text for an unknown number. This is native implementation context,
not an agreement that HGL admits unknown enum values or must use that fallback.
HGL must reject duplicate member numbers before registering the table; native
registration is not a substitute for that source check.

No implicit integer conversion, flag-enum behaviour, or exemption from
no-match failure is introduced by this agreement. Enum declarations and these
design fixtures remain outside the implemented compiler surface.

## Imported types are atomic values

Imported C++ and Python types are not time-series types. They participate as
scalar value types, like `i64`, `f64`, and `str`:

- An ordinary temporal parameter carries complete imported values on one
  atomic endpoint.
- A `const` parameter carries a wiring-time scalar value.
- An explicit `atomic` annotation is not required.
- An imported type remains an atomic leaf inside an HGL list, map, or struct.
- Native fields do not cause recursive temporalization or acquire independent
  tick histories merely because the type has been imported.

HGL-declared structs retain their existing structural temporalization. An
imported structured object does not acquire that behavior. Importing a type
preserves its native value identity and supported operations; it does not
import a time-series shape. The declaration mechanism for exposing those types
and operations remains to be discussed.

## Reference spelling and type compatibility

The agreed spelling is `ref<T>`, wrapping an existing HGL type. The user or
component designer specifies REF to declare what the component intends to
access or pass through. Use it only where the component intends to forward a
time series without observing or interacting with its values. It is not a
default annotation for every connection or a general inferred rewrite of
user-declared types.

For compiler-generated conditional branch callables, the compiler specifies
that component's access intent: a branch which only forwards an enclosing
variable's incoming binding takes it by reference. An ordinary temporal input
is sufficient when the branch is known to process it. This narrowly scoped
capture rule leaves the author's variable declaration unchanged; see
[Forwarding an existing binding](control-flow.md#forwarding-an-existing-binding).

For example:

```text
ref<map<i64, str>> -> REF[TSD[int, TS[str]]]
```

Reference wrappers do not participate in underlying type compatibility:

```text
ref<map<i64, str>> ~ map<i64, str>
```

The same compatibility principle applies within a type's structure. The
underlying types must still match. Compatibility does not erase the reference
from its endpoint representation or grant a node access through it.

Type formation is checked separately. A map key cannot be a reference:

```text
map<ref<i64>, str> -> error
```

Ignoring REF for compatibility does not make an invalid type formation legal.

## Node access and ticks

Inside node evaluation, including a `when` handler, a reference is an opaque
value. Code cannot inspect the values of the schema below the reference:
field access, collection indexing, and traversal through that reference are
not value-reading operations available to the node.

The reference ticks only when its binding changes. A value tick on the
referenced endpoint does not itself tick the reference. A binding change is
observable even when the old and new targets currently hold equal values.

REF-transparent type compatibility and opaque node access are distinct rules.
An ordinary non-reference input can consume a reference producer through
normal hgraph binding adaptation. A reference-typed node input retains the
opaque reference view; it does not implicitly dereference itself during
evaluation.

## Selecting and forwarding a reference

The routing example from the discussion illustrates the intended use. It is
deliberately fixed-size so that every bound in the guard is numeric:

```text
fn route3<T>(r: i64, l: list<ref<T>, 3>) -> ref<T> {
    when valid(r) && r >= 0 && r < 3 && valid(l[r]) && (modified(r) || modified(l[r])) {
        return l[r]
    }
}
```

The first guard establishes that the index can be read, and the next two prove
that it is in the fixed list's half-open range `[0, 3)`. Short-circuit order
therefore makes the selected entry available safely to its own validity and
modification checks. The list itself is accessible, and indexing it obtains one
opaque `ref<T>` element. This does not cross that element's reference boundary
or read a value of `T`. The node returns the selected reference, establishing
the connection through which consumers observe the selected time series.

A `const` list-size generic may also bind `unbounded`, whose sentinel is not a
numeric upper bound. The language has not yet decided the dynamic-list size and
safe-indexing surface, so this example does not generalise the comparison to
`list<ref<T>, S>` or invent a constraint that excludes `unbounded`.

After that connection is established, value ticks from the selected target
do not require this routing node to evaluate, copy the value, or emit it
again. The explicit handler observes both routing changes and reference-binding
changes: `modified(r)` selects a different entry, while `modified(l[r])`
republishes a selected entry that retargets without changing the index.
Reference inputs observe those binding changes, rather than value ticks behind
the bindings; the compiler does not silently add either activation condition.

The position of the boundary determines which operations are available:

| Node input | Permitted access in evaluation |
| --- | --- |
| `list<ref<T>, S>` | Access the list and select an opaque reference element. |
| `ref<list<T, S>>` | Handle the opaque reference; do not index the list beneath it. |

An ordinary list of temporal values instead requires a value-forwarding node
to observe and copy each selected value tick that it forwards. REF expresses
that this component is responsible for selecting the connection, without
processing the values subsequently carried by it.

## Wiring-time access

During wiring, code may access elements beneath a reference layer. Such access
causes dereferencing to obtain the element of interest. The result of wiring
is a connection to that element, not a read of its current runtime value.

This permits graph composition to select fields or collection elements through
reference-backed sources while preserving the node-level access restriction.
Reference binding and adaptation belong to the existing hgraph runtime.

This is not implemented in composition bodies yet. The compiler currently
rejects field or index access through `ref<T>` in every phase rather than
silently reading a value. Simple reference forwarding and normal hgraph
endpoint adaptation are supported.

## `signal` inputs

`signal` is an input-only observation contract. It accepts a time-series input
without exposing the input's value or structure. Its only observation
operations are:

- `modified`: whether the input was modified in the current evaluation cycle;
- `valid`: whether the input is valid;
- `last_modified`: the input's last modification time.

Value access is unavailable, including any value or delta payload that an
underlying native representation may technically expose. The intended source
contract does not permit arithmetic, Boolean value tests, field access,
indexing, or traversal of the connected data through a `signal` input.

There is no `signal` output in the language contract. There is consequently no
signal-emission operation or literal to design. Native implementation details
must not broaden this source contract.

The source spelling is lowercase `signal`. It is a contextual type marker that
is legal only as the complete type of a non-`const` function or operator
parameter. It cannot be nested, used as a field or result, declared `const`, or
given a default value. Uppercase `SIGNAL` remains unknown in HGL. The generated
C++ schema spelling is `hgraph::SIGNAL`.

`signal` is primarily meaningful inside a node, where those observation
operations can control evaluation. Graph functions may also accept `signal`
inputs and pass them to components with compatible inputs. This does not make
the graph body execute on ticks or expose a runtime value during wiring.

## Collection-reference mapping to clarify

The second mapping supplied during the discussion was:

```text
map<i64, ref<str>> -> REF[TSD[int, REF[TS[str]]]]
```

This includes an outer REF without an explicit outer `ref` in the source.
Whether that outer REF is intentional, and the rule that would add it, are
awaiting clarification. Do not infer a general reference-propagation rule or
silently remove the outer REF from this example.

The current compiler therefore rejects `map<K, ref<V>>`. It also rejects a
nested `ref<ref<T>>` boundary instead of relying on native REF normalization;
that is a fail-closed implementation boundary, not an additional source-level
decision.

## Scope of this agreement

This record does not settle native declaration syntax, reference construction
or mutation operations, or additional restrictions on reference placement.
Those remain separate discussion items. Implemented
reference forms are exercised by `examples/reference-routing.hgl`; unresolved
forms continue to fail closed.
