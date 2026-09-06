# Enum source and C++ mappings

Status: worked examples of the agreed [enum value rules](../design/type-extensions.md#enum-types),
not output from an implemented HGL enum compiler. Source appears before its
corresponding C++ representation. String conversion uses `str(value)` and
checked construction uses the enum type name, as in `Mode(value)`. Assigned
numbers use the signed `i64` range with compile-time overflow errors. The
complete native type/ABI mapping remains open.

## Numbered declarations

The source is also in [enum-values.hgl](../../stdlib/examples/enum-values.hgl):

```hgl
enum Mode {
    first = 10,
    second,
    third = 20
}

enum Sequence {
    first,
    second,
    reset = 10,
    after_reset
}
```

Resolving members in declaration order gives `Mode` the numbers `10, 11, 20`
and `Sequence` the numbers `0, 1, 10, 11`. Duplicate checking is within each
enum: the same number occurring in a different enum is not a duplicate member
of this declaration.

An illustrative C++ representation makes the resolved numbers explicit:

```cpp
#include <cstdint>
#include <stdexcept>
#include <string_view>

enum class Mode : std::int64_t {
    first = 10,
    second = 11,
    third = 20
};

enum class Sequence : std::int64_t {
    first = 0,
    second = 1,
    reset = 10,
    after_reset = 11
};
```

The `std::int64_t` representation here accommodates the agreed source range;
it does not require this physical representation in HGL's public native ABI.
Nor does declaring this C++ enum automatically register it as an hgraph type.
Actual lowering must preserve enum metadata at native value boundaries.

## Signed range and overflow

Assigned numbers range from `-9223372036854775808` to `9223372036854775807`,
inclusive. Negative numbers are permitted. These HGL declarations exercise
negative automatic numbering, both endpoints, and an explicit reset:

```hgl
enum Direction {
    reverse = -1,
    stopped,
    forward
}

enum Bounds {
    minimum = -9223372036854775808,
    after_minimum,
    maximum = 9223372036854775807,
    reset = -1,
    after_reset
}
```

`Direction` receives `-1, 0, 1`. `Bounds::after_minimum` receives
`-9223372036854775807`, and `Bounds::after_reset` receives `0`. The explicit
reset after the maximum is valid: only an implicit successor would overflow.
No member repeats a number within either declaration.

An illustrative C++ representation uses safe expressions for the endpoints:

```cpp
#include <limits>

enum class Direction : std::int64_t {
    reverse = -1,
    stopped = 0,
    forward = 1
};

enum class Bounds : std::int64_t {
    minimum = std::numeric_limits<std::int64_t>::min(),
    after_minimum = std::numeric_limits<std::int64_t>::min() + 1,
    maximum = std::numeric_limits<std::int64_t>::max(),
    reset = -1,
    after_reset = 0
};

static_assert(static_cast<std::int64_t>(Direction::stopped) == 0);
static_assert(static_cast<std::int64_t>(Bounds::minimum) == std::numeric_limits<std::int64_t>::min());
static_assert(static_cast<std::int64_t>(Bounds::maximum) == std::numeric_limits<std::int64_t>::max());
static_assert(static_cast<std::int64_t>(Bounds::after_reset) == 0);
```

The HGL minimum literal is valid as written. A frontend must not reject it
because the positive magnitude of its negative literal exceeds the signed
maximum. The C++ mapping avoids copying that magnitude into an unsuitable
signed literal. Range checks belong to HGL source checking, before native
emission; C++ diagnostics or an unchecked signed increment are not the source
contract. These examples define the allowed numbers, not a new backing-type
annotation, native import rule, or general runtime integer-overflow policy.

Each of the following HGL declarations is independently invalid:

```hgl
enum AboveRange {
    value = 9223372036854775808
}
```

```hgl
enum BelowRange {
    value = -9223372036854775809
}
```

```hgl
enum AutomaticOverflow {
    maximum = 9223372036854775807,
    next
}
```

The explicit values exceed the permitted endpoints, and the automatic value
would be `9223372036854775808`. All three are compile-time errors; never wrap,
clamp, or manufacture another member. No C++ representation is emitted for
these invalid declarations.

The positive source is [enum-number-range.hgl](../../stdlib/examples/enum-number-range.hgl).
The invalid fixtures are [enum-number-above-range.hgl](../../stdlib/examples/invalid/enum-number-above-range.hgl),
[enum-number-below-range.hgl](../../stdlib/examples/invalid/enum-number-below-range.hgl),
and [enum-number-overflow.hgl](../../stdlib/examples/invalid/enum-number-overflow.hgl).
They record agreed source checks, not implemented compiler tests.

## Member-name stringification

For the HGL declarations above, the required strings are:

| HGL member | Resolved number | Stringification |
| --- | --- | --- |
| `Mode::first` | `10` | `"first"` |
| `Mode::second` | `11` | `"second"` |
| `Mode::third` | `20` | `"third"` |

HGL requests the conversion with `str(value)`:

```hgl
const first_mode_name: str = str(Mode::first)
```

An illustrative C++ helper and constant for this source are:

```cpp
constexpr std::string_view mode_member_name(Mode value)
{
    switch (value) {
    case Mode::first:
        return "first";
    case Mode::second:
        return "second";
    case Mode::third:
        return "third";
    }
    throw std::invalid_argument("not a declared Mode member");
}

constexpr std::string_view first_mode_name = mode_member_name(Mode::first);
static_assert(first_mode_name == "first");
```

`mode_member_name` is an internal C++ illustration; the source operation is
`str`. The throwing path only makes the C++ illustration
defensive against a manually constructed unknown C++ value; it neither admits
such HGL values nor settles their handling at native import boundaries.

The existing native [enum registration contract](../../../include/hgraph/types/metadata/type_registry.h)
carries the member-name/assigned-number table. Its
[value operations](../../../src/hgraph/types/metadata/type_registry.cpp)
already render a registered member by name. Lowering must retain that table
rather than erase the enum to an ordinary integer and stringify the integer.
The native unknown-number fallback is not a new HGL source guarantee.

## Checked conversion into an enum

Use the target enum type name with an assigned integer or exact member name.
For the `Mode` declaration above:

```hgl
const mode_from_number: Mode = Mode(10)
const mode_from_name: Mode = Mode("first")
```

Both constants are `Mode::first`. The C++ mapping must check membership rather
than perform an unchecked cast to the enum:

```cpp
constexpr Mode lookup_mode(std::int64_t number)
{
    switch (number) {
    case 10:
        return Mode::first;
    case 11:
        return Mode::second;
    case 20:
        return Mode::third;
    default:
        throw std::invalid_argument("unknown Mode number");
    }
}

constexpr Mode lookup_mode(std::string_view name)
{
    if (name == "first") { return Mode::first; }
    if (name == "second") { return Mode::second; }
    if (name == "third") { return Mode::third; }
    throw std::invalid_argument("unknown Mode member name");
}

constexpr Mode mode_from_number = lookup_mode(std::int64_t{10});
constexpr Mode mode_from_name = lookup_mode(std::string_view{"first"});
static_assert(mode_from_number == Mode::first);
static_assert(mode_from_name == Mode::first);
```

`lookup_mode` is an illustrative generated helper, not an HGL function name or
a new native API. The source remains `Mode(...)`. Result values retain enum
identity; strings match member names exactly, with no case folding, whitespace
trimming, numeric-string parsing, or qualified-name interpretation. For this
enum, only the assigned numbers `10`, `11`, and `20` succeed; neither an
ordinal nor any other number in the interval from `10` to `20` is sufficient.

These two HGL examples are independently invalid constants:

```hgl
const missing_mode: Mode = Mode(12)
```

```hgl
const missing_mode: Mode = Mode("First")
```

The first has no member with number `12`; the second does not exactly match
`"first"`. Checking must reject either source rather than emit a runtime cast
or manufacture an unnamed member. Complete source fixtures are
[enum-conversion-unknown-number.hgl](../../stdlib/examples/invalid/enum-conversion-unknown-number.hgl)
and [enum-conversion-unknown-name.hgl](../../stdlib/examples/invalid/enum-conversion-unknown-name.hgl).
Successful constants are mirrored in [enum-values.hgl](../../stdlib/examples/enum-values.hgl).

Failure timing depends on when the operand is available. A constant is checked
before runtime; a wiring-time configuration value is checked while wiring.
Inside node evaluation the checked lookup runs on the admitted current value.
In a temporal graph, wiring composes the conversion and the runtime lookup
fails if an evaluated input has no matching member. It must not silently skip
the tick or reinterpret failure as a switch `default` selection. An input that
has no valid value is still governed by the existing input-validity rules.

The helper's `std::invalid_argument` illustrates failure; it does not settle
HGL exception types or error-catching syntax. The compiler must implement its
own source diagnostic for invalid constants. A public SDK carrier and native
registration/lowering for enum-typed graph results remain separate work; no
unimplemented native `Port` or operator API is assumed here. Likewise, this
checked constructor does not settle import handling for an already-typed
native enum containing an unknown value.

## Declaration-order enumeration

All three enum views (`keys`, `values`, and `elements`) iterate in declaration
order, independent of assigned numbers or member names. Consider this HGL:

```hgl
enum EnumerationOrder {
    high = 20,
    low = 5,
    next
}
```

The required sequences are:

| View | First | Second | Third |
| --- | --- | --- | --- |
| `keys` | `"high"` | `"low"` | `"next"` |
| `values` | `20` | `5` | `6` |
| `elements` | `EnumerationOrder::high` | `EnumerationOrder::low` | `EnumerationOrder::next` |

An illustrative C++ representation retains the declaration sequence:

```cpp
#include <array>

enum class EnumerationOrder : std::int64_t {
    high = 20,
    low = 5,
    next = 6
};

constexpr std::array<std::string_view, 3> enumeration_keys{"high", "low", "next"};
constexpr std::array<std::int64_t, 3> enumeration_values{20, 5, 6};
constexpr std::array<EnumerationOrder, 3> enumeration_elements{
    EnumerationOrder::high, EnumerationOrder::low, EnumerationOrder::next
};

static_assert(enumeration_keys[1] == "low");
static_assert(enumeration_values[0] == 20 && enumeration_values[1] == 5);
static_assert(static_cast<std::int64_t>(enumeration_elements[2]) == 6);
```

These arrays illustrate ordered metadata, not a chosen HGL return container
or new native enum API. The source enum remains a distinct type in `elements`;
only `values` exposes integers. Do not sort by number, scan a numeric interval,
or use unordered-table iteration to implement any of these views. The exact
enum enumeration invocation syntax and returned collection/iterator shape
remain open.
The source declaration is mirrored in
[enum-enumeration-order.hgl](../../stdlib/examples/enum-enumeration-order.hgl);
no speculative enumeration call syntax is included.

## Conversion in nodes and graphs

The following examples use `i64` so their public C++ signatures do not assume
an unresolved enum SDK carrier type. The call still follows the same phase
rules; stringifying a registered enum must preserve its member metadata.

HGL node source:

```hgl
fn integer_text_node(value: i64) -> str {
    when modified(value) && valid(value) {
        return str(value)
    }
}
```

The native node converts the admitted current value within evaluation:

```cpp
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_ops.h>

using namespace hgraph;

struct IntegerTextNode
{
    static constexpr auto name = "integer_text_node";

    static void eval(In<"value", TS<Int>> value, Out<TS<Str>> out)
    {
        if (value.modified() && value.valid()) {
            const Int payload = value.value();
            out.set(ops_for<Int>().to_string(&payload));
        }
    }
};
```

HGL graph source:

```hgl
fn integer_text_graph(value: i64) -> str {
    return str(value)
}
```

The native graph wires conversion without reading a runtime payload:

```cpp
struct IntegerTextGraph
{
    static constexpr auto name = "integer_text_graph";

    static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> value)
    {
        return wire<stdlib::str_>(w, value).as<TS<Str>>();
    }
};
```

The graph assumes standard operators have been registered before wiring. For
valid integer ticks `10, 11`, both forms produce `"10", "11"`; a cycle with no
input tick contributes no output tick. The `when` guard admits a payload read
in the node. The graph's activity and validity follow the native conversion
node. A `str` call does not select the containing function's phase.

These map HGL's `str` spelling to existing C++ value conversion and the native
[`str_` operator](../../../include/hgraph/lib/std/operators/conversion.h),
respectively. The integer implementation and graph contract are visible in
the [conversion implementations](../../../include/hgraph/lib/std/operators/impl/conversion_impl.h)
and [native operator tests](../../../tests/cpp/test_std_operators.cpp).
The same approach must retain enum-specific value operations for enum inputs;
using ordinary integer operations for an enum would incorrectly print its
number.

The spelling decision alone does not equate every native formatting path:
for example, native `str_` renders Boolean values differently from the
Python-style `convert`-to-string overload. These integer and enum examples
do not settle formatting for every other type or add Python execution to HGL.

The source functions are mirrored in
[string-conversion.hgl](../../stdlib/examples/string-conversion.hgl).

## Duplicate numbers are a source error

This source is intentionally invalid:

```hgl
enum DuplicateExplicit {
    first = 10,
    second = 10
}
```

So is this collision introduced by automatic numbering:

```hgl
enum DuplicateAutomatic {
    existing = 11,
    restart = 10,
    collision
}
```

`collision` receives `11` because it follows `restart`, not `12` from the
largest previous number. That duplicates `existing`, so source checking must
reject the declaration before native registration or C++ emission. C++ itself
permits numeric aliases; emitting valid C++ therefore cannot prove this HGL
rule was checked. No C++ representation is emitted for either invalid source.

The fixtures are [enum-duplicate-number.hgl](../../stdlib/examples/invalid/enum-duplicate-number.hgl)
and [enum-implicit-duplicate-number.hgl](../../stdlib/examples/invalid/enum-implicit-duplicate-number.hgl).
They record intended source errors, not currently passing compiler diagnostics.

## Validation boundary

The first five C++ blocks compile together without hgraph and can be exercised
for signed range endpoints, resolved numbers, member strings, checked
conversion, and declaration-order views. The node and graph blocks additionally require the public hgraph
headers. Syntax checking those blocks does not prove runtime execution, HGL
parsing, native enum registration, Python exposure, or temporal enum behaviour.
All HGL sources here remain design fixtures outside
the executable `language/examples/` corpus.
