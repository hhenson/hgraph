# Enum source and C++ mappings

Status: worked examples of the agreed [enum value rules](../design/type-extensions.md#enum-types),
not output from an implemented HGL enum compiler. Source appears before its
corresponding C++ representation. Conversion-call spelling, the source integer
range, and the complete native type/ABI mapping remain open.

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

An illustrative C++ helper for those values is:

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
```

`mode_member_name` is a C++ illustration, not a newly agreed HGL function
name. The HGL conversion-call spelling is still open, so no invented source
call precedes this helper. The throwing path only makes the C++ illustration
defensive against a manually constructed unknown C++ value; it neither admits
such HGL values nor settles their handling at native import boundaries.

The existing native [enum registration contract](../../../include/hgraph/types/metadata/type_registry.h)
carries the member-name/assigned-number table. Its
[value operations](../../../src/hgraph/types/metadata/type_registry.cpp)
already render a registered member by name. Lowering must retain that table
rather than erase the enum to an ordinary integer and stringify the integer.
The native unknown-number fallback is not a new HGL source guarantee.

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

The two C++ blocks compile together without hgraph and can be exercised for
resolved numbers and member strings. That validates the illustrations only,
not HGL parsing, native enum registration, Python exposure, or temporal enum
behaviour. All HGL declarations here remain design fixtures outside the
executable `language/examples/` corpus.
