# Enum source and C++ mappings

Status: worked examples of the agreed [enum value rules](../design/type-extensions.md#enum-types),
not output from an implemented HGL enum compiler. Source appears before its
corresponding C++ representation. Conversion uses the agreed `str(value)`
spelling. The source integer range and complete native type/ABI mapping remain
open.

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

The `std::int64_t` representation here accommodates these example values; it
does not settle HGL's backing-width, numeric conversion, or public ABI rules.
Nor does declaring this C++ enum automatically register it as an hgraph type.
Actual lowering must preserve enum metadata at native value boundaries.

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
enum invocation syntax and returned collection/iterator shape remain open.
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

The first three C++ blocks compile together without hgraph and can be exercised
for resolved numbers, member strings, and declaration-order views. The node
and graph blocks additionally require the public hgraph headers. Syntax checking
those blocks does not prove runtime execution, HGL parsing, native enum
registration, Python exposure, or
temporal enum behaviour. All HGL sources here remain design fixtures outside
the executable `language/examples/` corpus.
