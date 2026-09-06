# Enum switch source and C++ mappings

Status: worked examples of the agreed [switch checks](../design/switch.md#duplicate-cases-and-enum-coverage),
not output from an implemented compiler. The HGL source is collected in
[enum-switch.hgl](../../stdlib/examples/enum-switch.hgl). These are design
fixtures, not executable compiler examples. Native enum carrier, registration,
and import-boundary integration remain separate work.

## The selector type

All positive examples use the same HGL declaration:

```hgl
enum Mode {
    first = 10,
    second,
    third = 20
}
```

An illustrative C++ representation preserves the nominal enum and numbers:

```cpp
#include <cstdint>
#include <stdexcept>

enum class Mode : std::int64_t {
    first = 10,
    second = 11,
    third = 20
};
```

As in the [enum value mappings](enum-cpp-mappings.md), this is not a public
native ABI or automatic hgraph type registration. HGL case constants must be
members of this enum; integers or another enum's members are not substitutes.

## Exhaustive graph composition

This HGL has no runtime-only constructs. Its selector is temporal, so its
body composes a graph:

```hgl
fn choose(mode: Mode, x: i64, y: i64) -> i64 {
    var r: i64
    switch mode {
        case Mode::first:
            r = x
        case Mode::second:
            r = y
        case Mode::third:
            r = x + y
    }
    return r * 2
}
```

Every member is covered and every successful case supplies `r`. The compiler
must generate native `switch_` branch wiring, not a C++ payload switch in
`compose`. The case graphs' computations can be expressed using existing C++
ports and operators:

```cpp
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/graph_wiring.h>

using namespace hgraph;

struct EnumFirstBranch
{
    static constexpr auto name = "enum_first_branch";
    static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> x, Port<TS<Int>>)
    {
        return x;
    }
};

struct EnumSecondBranch
{
    static constexpr auto name = "enum_second_branch";
    static Port<TS<Int>> compose(Wiring &, Port<TS<Int>>, Port<TS<Int>> y)
    {
        return y;
    }
};

struct EnumThirdBranch
{
    static constexpr auto name = "enum_third_branch";
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x, Port<TS<Int>> y)
    {
        return wire<stdlib::add_>(w, x, y).as<TS<Int>>();
    }
};
```

The complete generated boundary must preserve the following mapping:

| HGL component | Native graph responsibility |
| --- | --- |
| Temporal `mode: Mode` | Bind the enum-typed scalar selector to `stdlib::switch_`; never read its payload during composition. |
| Three member labels | Build `stdlib::switch_cases` entries from resolved enum-typed keys and the three branch callables. |
| Captured `x`, `y` | Form shared input slots; first/second branches forward a binding and third wires addition, using the existing REF/capture adaptation rules. |
| Escaping `r` | Return the single branch result directly and remap the switch output to `r`. |
| `return r * 2` | Wire `stdlib::mul_` with the selected result and scalar `Int{2}` outside the switch. |
| No source default | Leave the native default absent, retaining native no-match failure even though all declared members are covered. |

The branch snippets show computations, not a replacement for compiler-generated
boundary signatures and REF adaptation. A complete enum-typed parent signature
and construction of native enum key values await the native enum mapping;
this guide does not invent that SDK API or erase `Mode` to `Int` to bypass it.
Until that mapping exists, the compiler must reject the unsupported lowering.
The existing [integer-selector mappings](control-flow-cpp-mappings.md#scenario-4-a-temporal-selector-creates-native-switch-branches)
show the complete native switch call shape without assuming an enum carrier.

Exhaustiveness does not flatten the branches or change their lifetime: only
the selected child is active, and input updates propagate through its binding
and computations. A wiring-time scalar selector instead chooses topology
during composition; it uses the same case-type, duplicate, and coverage checks.

## Exhaustive node-local dispatch

Adding an explicit handler makes this a node-style function. All three inputs
must be valid for this example, and a tick on any of them may activate it:

```hgl
fn choose_node(mode: Mode, x: i64, y: i64) -> i64 {
    when modified(mode, x, y) && valid(mode, x, y) {
        var r: i64
        switch mode {
            case Mode::first:
                r = x
            case Mode::second:
                r = y
            case Mode::third:
                r = x + y
        }
        return r * 2
    }
}
```

The admitted payload calculation has this C++ mapping:

```cpp
std::int64_t choose_enum_payload(Mode mode, std::int64_t x, std::int64_t y)
{
    std::int64_t r;
    switch (mode) {
    case Mode::first:
        r = x;
        break;
    case Mode::second:
        r = y;
        break;
    case Mode::third:
        r = x + y;
        break;
    default:
        throw std::runtime_error("unmatched enum switch selector");
    }
    return r * 2;
}
```

The generated node reads admitted inputs, performs this calculation, and
writes its output only on success. No child graph is created. The C++ default
implements failure, not an implicit HGL default branch. Do not eliminate it
because all declared members appear: even this exhaustive dispatch retains
the no-match failure path. Native exception plumbing is illustrative, not a
new source exception type or permission to construct unknown HGL enum values.

For `x = 10` and `y = 20`, the three declared members produce `20`, `40`, and
`60`. Small inputs avoid deciding general integer-overflow behavior. The graph
and node forms share branch arithmetic, not necessarily activation/validity
behavior: this handler requires both data inputs to be valid, whereas a graph
case that forwards only `x` need not observe the unrelated `y` input.

## Partial coverage without a default

Leaving out the third case is permitted source, not an exhaustiveness error:

```hgl
fn choose_partial_node(mode: Mode, x: i64, y: i64) -> i64 {
    when modified(mode, x, y) && valid(mode, x, y) {
        var r: i64
        switch mode {
            case Mode::first:
                r = x
            case Mode::second:
                r = y
        }
        return r * 2
    }
}
```

The corresponding C++ payload mapping is:

```cpp
std::int64_t choose_partial_enum_payload(Mode mode, std::int64_t x, std::int64_t y)
{
    std::int64_t r;
    switch (mode) {
    case Mode::first:
        r = x;
        break;
    case Mode::second:
        r = y;
        break;
    default:
        throw std::runtime_error("unmatched enum switch selector");
    }
    return r * 2;
}
```

`Mode::third` now fails before the result is used or an output is written.
Both successful cases still assign `r`; the failing path never reaches its
use. In a temporal graph the analogous partial case table leaves the native
default absent; a wiring-time scalar choice fails while composing instead.

## A supplied default

This source supplies a real fallback branch:

```hgl
fn choose_default_node(mode: Mode, x: i64, y: i64) -> i64 {
    when modified(mode, x, y) && valid(mode, x, y) {
        var r: i64
        switch mode {
            case Mode::first:
                r = x
            case Mode::second:
                r = y
            default:
                r = x + y
        }
        return r * 2
    }
}
```

The C++ payload mapping uses that branch for no-match dispatch:

```cpp
std::int64_t choose_default_enum_payload(Mode mode, std::int64_t x, std::int64_t y)
{
    std::int64_t r;
    switch (mode) {
    case Mode::first:
        r = x;
        break;
    case Mode::second:
        r = y;
        break;
    default:
        r = x + y;
        break;
    }
    return r * 2;
}
```

`Mode::third` selects the default and produces `60` for the same inputs. In a
graph, the default becomes a native switch child with the same capture/result
analysis as the named cases. A default does not catch errors in a selected
branch or errors while constructing the selector with `Mode(...)`.

## Source-checking errors

Each of these examples is independently invalid; see the linked complete
fixtures. No C++ dispatch should be emitted for them.

| Fixture | Required source diagnostic |
| --- | --- |
| [Duplicate cases](../../stdlib/examples/invalid/enum-switch-duplicate-case.hgl) | `Mode::first` duplicates `Mode(10)`, or a named constant holding that member, after constant resolution. Reject both forms before backend emission. |
| [Integer label](../../stdlib/examples/invalid/enum-switch-integer-case.hgl) | `case 10:` is not a `Mode` member; reject the type mismatch before coverage analysis. |
| [Other enum label](../../stdlib/examples/invalid/enum-switch-other-enum-case.hgl) | `OtherMode::first` is not a `Mode` member, despite the same assigned number. |
| [Unassigned result](../../stdlib/examples/invalid/enum-switch-unassigned-result.hgl) | All members are covered, but the third branch reaches `return r * 2` without assigning `r`. |

Duplicate detection compares resolved typed constants, not spelling or ordinal
position. C++ switch diagnostics alone cannot implement these checks for all
source phases and admitted key types. Full declared-member coverage is a
separate fact from definite assignment and must not suppress it.

## Validation boundary

The enum declaration and three payload blocks compile and run together without
hgraph. They can exercise successful dispatch, partial no-match failure,
default selection, and retained failure for an unknown C++ representation in
the exhaustive helper. That defensive test does not make unknown values legal
HGL values or settle native import handling.

The branch-graph block additionally requires hgraph headers and syntax checking;
it is not a complete enum graph or proof of temporal execution. The source
fixtures and intended errors require future compiler tests. No compiler,
runtime, Python bridge, or native enum SDK implementation is added here.
