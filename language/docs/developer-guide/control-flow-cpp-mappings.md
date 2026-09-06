# Control-flow scenarios and C++ mappings

Status: worked mappings of the agreed [switch contract](../design/switch.md)
and [conditional result rules](../design/control-flow.md), not output from an
implemented HGL switch compiler. Each scenario starts with HGL source using
the agreed `switch selector { case value: ... default: ... }` form, followed
by its C++ mapping and behaviour. Case labels are source-expressible constants.
The same HGL functions are collected in
[switch-scenarios.hgl](../../stdlib/examples/switch-scenarios.hgl); they are
design fixtures, not passing compiler tests. These examples use `i64` selectors
and integer constants; [enum numbering and stringification](../design/type-extensions.md#enum-types)
remain a separate design step after the agreed enum declaration/member form.

These are reference fragments, not standalone hgraph applications. The native
examples share the following preamble and assume standard operators have been
registered before wiring. They omit module registration, sources, sinks used
only to collect test results, and the evaluation harness:

```cpp
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/wired_fn.h>

#include <cstdint>
#include <stdexcept>

using namespace hgraph;
```

Generated code must additionally preserve source activation, validity, error
reporting, and source locations. The examples use small integer values to
avoid making decisions about unresolved arithmetic-overflow semantics. They
illustrate integer selectors, not an exhaustive selector-type admission rule.

## Scenario 1: a node computes a result and uses it later

The node has readable inputs `mode`, `x`, `y`, and `fallback`. For this scenario
all four must be valid, and any input tick may activate the evaluation. The
selected case assigns an evaluation-local `r`; the function then returns
`r * 2`:

| Selector | Assignment to `r` | Result when `x = 10`, `y = 20`, `fallback = 7` |
| --- | --- | --- |
| `0` | `x + 1` | `22` |
| `1` | `y - 1` | `38` |
| Any other valid value, via default | `fallback * 3` | `42` |

HGL source:

```hgl
fn local_switch_example(mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    when modified(mode, x, y, fallback) && valid(mode, x, y, fallback) {
        var r: i64
        switch mode {
            case 0:
                r = x + 1
            case 1:
                r = y - 1
            default:
                r = fallback * 3
        }

        return r * 2
    }
}
```

The payload calculation has this ordinary C++ mapping:

```cpp
std::int64_t choose_payload(std::int64_t mode, std::int64_t x,
                            std::int64_t y, std::int64_t fallback)
{
    std::int64_t r;
    switch (mode) {
    case 0:
        r = x + 1;
        break;
    case 1:
        r = y - 1;
        break;
    default:
        r = fallback * 3;
        break;
    }
    return r * 2;
}
```

The corresponding native node boundary is:

```cpp
struct LocalSwitchExample
{
    static constexpr auto name = "local_switch_example";

    static void eval(In<"mode", TS<Int>> mode,
                     In<"x", TS<Int>> x,
                     In<"y", TS<Int>> y,
                     In<"fallback", TS<Int>> fallback,
                     Out<TS<Int>> out)
    {
        out.set(choose_payload(mode.value(), x.value(), y.value(),
                               fallback.value()));
    }
};
```

This all-valid example deliberately reads all arguments. It is not a template
for a source handler that permits inactive-case inputs to be invalid: that
handler requires selective validity guards and reads inside the selected
branch. There is one node evaluation and one output write here. No nested
graph is created, and changing `mode` does not restart node-owned state.

## Scenario 2: a missing default must fail in node code

Now remove the source default:

```hgl
fn local_switch_required(mode: i64, x: i64, y: i64) -> i64 {
    when modified(mode, x, y) && valid(mode, x, y) {
        var r: i64
        switch mode {
            case 0:
                r = x + 1
            case 1:
                r = y - 1
        }

        return r * 2
    }
}
```

The generated C++ still needs an error path. As in Scenario 1, a native node
boundary reads the admitted inputs, calls this payload function, and writes
the result only on success:

```cpp
std::int64_t choose_payload_required(std::int64_t mode, std::int64_t x,
                                     std::int64_t y)
{
    std::int64_t r;
    switch (mode) {
    case 0:
        r = x + 1;
        break;
    case 1:
        r = y - 1;
        break;
    default:
        throw std::runtime_error("unmatched switch selector");
    }
    return r * 2;
}
```

The C++ `default` above implements the mandatory failure for an absent HGL
default; it does not invent a user-supplied branch. Selector `99` must fail
before writing an output or reading an unassigned `r`. Exact exception text and
generated diagnostic plumbing are implementation details. Normal evaluation
error handling remains responsible for propagation or capture of that failure.

## Scenario 3: a wiring-time selector chooses topology

Use Scenario 1's cases, but make `mode` a wiring-time scalar and the remaining
inputs temporal:

```hgl
fn wiring_time_switch_example(const mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    var r: i64
    switch mode {
        case 0:
            r = x + 1
        case 1:
            r = y - 1
        default:
            r = fallback * 3
    }

    return r * 2
}
```

The following C++ branch graphs will also be used by the
temporal-switch example. Each returns only the case's `r` connection:

```cpp
struct CaseZeroExample
{
    static constexpr auto name = "case_zero_example";
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x,
                                 Port<TS<Int>>, Port<TS<Int>>)
    {
        return wire<stdlib::add_>(w, x, Int{1}).as<TS<Int>>();
    }
};

struct CaseOneExample
{
    static constexpr auto name = "case_one_example";
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>,
                                 Port<TS<Int>> y, Port<TS<Int>>)
    {
        return wire<stdlib::sub_>(w, y, Int{1}).as<TS<Int>>();
    }
};

struct CaseDefaultExample
{
    static constexpr auto name = "case_default_example";
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>,
                                 Port<TS<Int>>, Port<TS<Int>> fallback)
    {
        return wire<stdlib::mul_>(w, fallback, Int{3}).as<TS<Int>>();
    }
};

struct WiringTimeSwitchExample
{
    static constexpr auto name = "wiring_time_switch_example";
    static Port<TS<Int>> compose(Wiring &w, Scalar<"mode", Int> mode,
                                 Port<TS<Int>> x, Port<TS<Int>> y,
                                 Port<TS<Int>> fallback)
    {
        auto r = [&]() -> Port<TS<Int>> {
            switch (mode.value()) {
            case 0:
                return wire<CaseZeroExample>(w, x, y, fallback);
            case 1:
                return wire<CaseOneExample>(w, x, y, fallback);
            default:
                return wire<CaseDefaultExample>(w, x, y, fallback);
            }
        }();
        return wire<stdlib::mul_>(w, r, Int{2}).as<TS<Int>>();
    }
};
```

The local C++ lambda executes once during composition. With `mode = 0`, only
the `x + 1` case and the subsequent multiplication are wired. Later value
ticks do not revisit the scalar switch. If the source had no default, the
unmatched C++ path would throw during wiring instead of composing
`CaseDefaultExample`.

## Scenario 4: a temporal selector creates native switch branches

Now `mode` is temporal. There is no `when` or other runtime-only construct in
this function, so its body composes a graph:

```hgl
fn temporal_switch_example(mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    var r: i64
    switch mode {
        case 0:
            r = x + 1
        case 1:
            r = y - 1
        default:
            r = fallback * 3
    }

    return r * 2
}
```

Reuse the three C++ case graphs, but compose a native switch instead of
executing a C++ switch on the current payload:

```cpp
struct TemporalSwitchExample
{
    static constexpr auto name = "temporal_switch_example";
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> mode,
                                 Port<TS<Int>> x, Port<TS<Int>> y,
                                 Port<TS<Int>> fallback)
    {
        auto r = wire<stdlib::switch_>(
            w, mode,
            stdlib::switch_cases(
                {{Value{Int{0}}, fn<CaseZeroExample>()},
                 {Value{Int{1}}, fn<CaseOneExample>()}},
                fn<CaseDefaultExample>()),
            x, y, fallback).as<TS<Int>>();
        return wire<stdlib::mul_>(w, r, Int{2}).as<TS<Int>>();
    }
};
```

`r` is the switch output connection. The final multiplication is wired outside
the switch. It is not duplicated inside each branch, and `mode` is never read
as a runtime payload during composition.

The source-level captures are different even though the illustrative C++
wrappers deliberately accept the same explicit argument list:

| Branch | Temporal captures actually used | Result connection |
| --- | --- | --- |
| `0` | `x` | `x + 1` |
| `1` | `y` | `y - 1` |
| Default | `fallback` | `fallback * 3` |

The outer temporal slots are `mode`, `x`, `y`, and `fallback`. Literal scalar
configuration such as `1` and `3` belongs to branch composition, not to the
outer capture list. The selector itself is not a branch capture here because
none of the bodies reads it.

These uniform wrappers satisfy native explicit-argument arity. A compiler may
instead use the existing captured-input boundary remapping to preserve smaller
branch interfaces. It must not pass the union of arguments to unequal explicit
signatures and assume the native binder discards the extras. Unused slots must
not acquire payload reads or extra validity requirements merely because the
outer switch carries them.

For valid source values, selecting modes `0`, `1`, and `99` yields the numerical
results in Scenario 1, now through different live child graphs. While mode
`0` stays selected, ticks of `x` can update the result without a new mode tick.
Work used only by inactive branches does not become active case work.

Without a source default, omit the default callable from `switch_cases`.
An unmatched temporal key then fails through native switch execution, as
covered by `switch_: an unmatched key with no default branch is a runtime
error` in [test_switch.cpp](../../../tests/cpp/test_switch.cpp). This is not the
never-ticking false branch of a value-producing temporal `if` without `else`.

## Scenario 5: multiple results and pure forwarding

Consider a predeclared `r` and `adjustment`, both used after the switch. One
case computes `r` and forwards an existing `adjustment`; another computes both;
the default forwards their incoming bindings. This uses the same output
analysis as the agreed [multiple-result conditional](../design/control-flow.md#multiple-escaping-variables).

```hgl
fn multiple_switch_results(mode: i64, x: i64, y: i64) -> i64 {
    var r: i64 = x
    var adjustment: i64 = y
    switch mode {
        case 0:
            r = x + 1
        case 1:
            r = y - 1
            adjustment = x + y
        default:
    }

    return r * 2 + adjustment
}
```

Case `0` forwards the incoming `adjustment` binding; the explicit empty
default forwards both incoming bindings. Nothing reads or copies their
payloads merely to pass them through. The final expression consumes the
remapped results as ordinary `i64` time series.

For two ordinary `i64` result slots, the generated common native shape is:

```cpp
using OrdinarySwitchResults = UnNamedTSB<
    Field<"r", TS<Int>>,
    Field<"adjustment", TS<Int>>>;
```

This is a schema illustration; field names are internal, not reserved source
names. Each branch must return this same field-level schema. A pure-forwarding
input capture may be `REF<TS<Int>>`, but packing it into an ordinary `TS<Int>`
result slot requires native binding adaptation at the branch-output boundary.
If the author explicitly declares a reference result, that slot instead stays
`REF<TS<Int>>`. Do not copy payloads merely to discard the reference intent or
pretend that REF-transparent compatibility makes unlike native bundle schemas
identical.

After the switch, the outer bindings refer to the respective bundle-field
connections; subsequent C++ wiring consumes those projections. They are not a
simultaneous scalar snapshot. If the public native boundary cannot express a
required per-field adaptation, reject that HGL lowering until support exists;
this sketch does not assert that every such product is implemented today.

If a branch reaches later code without supplying one of these bindings and
there is no incoming binding to forward, reject it for definite assignment.
An unmatched selector without a default takes the failure path instead and
does not reach that later use.

## Scenario 6: an early return changes the continuation boundary

Suppose case `0` returns `x + 1` from the enclosing graph function. Case `1`
assigns `r = y - 1`, and the default assigns `r = fallback * 3`. The statements
after the switch return `r * 2`.

```hgl
fn early_return_switch(mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    var r: i64
    switch mode {
        case 0:
            return x + 1
        case 1:
            r = y - 1
        default:
            r = fallback * 3
    }

    return r * 2
}
```

| Generated C++ branch | Captures | Child output |
| --- | --- | --- |
| `0` | `x` | Connection for `x + 1`; no continuation multiplication. |
| `1` | `y` | Connection for `(y - 1) * 2`, including the continuation. |
| Default | `fallback` | Connection for `(fallback * 3) * 2`, including the continuation. |

The parent C++ graph returns the native switch output directly. Unlike
Scenario 4, placing one multiplication after that switch would be wrong: it
would also multiply case `0`'s early-return result. Nodes and sinks belonging
to the continuation must therefore be compiled into the non-returning case
graphs. Any producer wired before the switch remains outside them.

Inside a node, the equivalent C++ uses a direct return from the current
evaluation path. It does not transform the remaining statements into nested
graphs. See [early returns](../design/control-flow.md#early-returns-and-continuations).

## Scenario 7: conditional sinks and an unconditional sink

Mode `0` enables a debug sink; the explicit default does nothing. An additional
debug sink after the switch is always wired:

```hgl
fn sink_switch_example(mode: i64, value: i64) {
    switch mode {
        case 0:
            debug_print("selected", value)
        default:
    }

    debug_print("always", value)
}
```

Its C++ wiring is:

```cpp
struct SelectedSinkExample
{
    static constexpr auto name = "selected_sink_example";
    static void compose(Wiring &w, Port<TS<Int>> value)
    {
        wire<stdlib::debug_print>(w, Str{"selected"}, value);
    }
};

struct EmptySinkExample
{
    static constexpr auto name = "empty_sink_example";
    static void compose(Wiring &, Port<TS<Int>>) {}
};

struct SinkSwitchExample
{
    static constexpr auto name = "sink_switch_example";
    static void compose(Wiring &w, Port<TS<Int>> mode, Port<TS<Int>> value)
    {
        wire<stdlib::switch_sink_>(
            w, mode,
            stdlib::switch_cases(
                {{Value{Int{0}}, fn<SelectedSinkExample>()}},
                fn<EmptySinkExample>()),
            value);
        wire<stdlib::debug_print>(w, Str{"always"}, value);
    }
};
```

The label comes before the time series. The default child is empty because
the scenario explicitly supplies an empty default, not because a missing
default is silently filled in. Without that default, an unmatched key must
fail even though the switch has no output. Logging itself happens in sink
evaluation, never by printing the current payload in `compose`.

## Scenario 8: state across case changes

The node-style source owns both counters in the enclosing node. It increments
only on a valid `value` tick, not on a selector-only tick:

```hgl
fn local_switch_counters(mode: i64, value: i64) -> i64 {
    state zero_count: i64 = 0
    state one_count: i64 = 0

    when modified(value) && valid(mode, value) {
        switch mode {
            case 0:
                zero_count += 1
                return zero_count
            case 1:
                one_count += 1
                return one_count
        }
    }
}
```

The graph-style source instead composes a counter **inside** each branch:

```hgl
fn branch_tick_count(value: i64) -> i64 {
    state count: i64 = 0

    when modified(value) && valid(value) {
        count += 1
        return count
    }
}

fn graph_switch_counters(mode: i64, value: i64) -> i64 {
    var r: i64
    switch mode {
        case 0:
            r = branch_tick_count(value)
        case 1:
            r = branch_tick_count(value)
    }

    return r
}
```

The C++ node mapping stores `zero_count` and `one_count` in the enclosing
node's `RecordableState`, with replay-aware initialization to zero and local
dispatch inside `eval`. The graph mapping uses native `switch_` with two case
entries whose child callable wires `branch_tick_count`'s native counter node.
Each active child instance owns its counter's `RecordableState`. That call
must not be hoisted outside the switch merely because its source spelling
is identical in both branches. Neither source has a default; unmatched keys
fail when dispatch is attempted.

Assume each selected case increments its own counter on a valid data tick,
with a data tick on every row below. In a node, the two counters belong to the
enclosing node's recordable state and increment only in their selected case.
In the graph variant, each selected branch owns a counter node and the normal
switch policy is used, without reload-on-every-selector-tick:

| Selected case | Node-owned selected counter | Graph branch's counter |
| --- | --- | --- |
| `0` | `1` | `1` |
| `0` again | `2` | `2` |
| `1` | `1` | `1` |
| `0` again | `3` | `1` |

Local C++ dispatch preserves the enclosing node's two counters. Native
`switch_` stops the old child and later starts a fresh instance when returning
to case `0`. It does not suspend and resume that child's counter. The native
test `switch_: switching back reuses the old slot with a fresh branch graph`
in [test_switch.cpp](../../../tests/cpp/test_switch.cpp) pins this restart rule.
The same distinction applies if a default containing a counter is added.

The native policy compares selector values, not just the selected callable.
For example, changing from unmatched key `99` to unmatched key `100` starts a
fresh default child even though both keys select the same default function.
Repeating the same unmatched key does not itself request a reload. This follows
the key comparison in [switch execution](../../../src/hgraph/runtime/switch_node.cpp),
not a separate HGL default-state policy.

## Scope and validation boundary

These mappings pair agreed HGL source with the expected native execution and
wiring shapes. They do not change the parser or backend, or settle graph-loop
predicates or reductions. Enum numbering/stringification details, the full
native selector-type coverage, and any HGL spelling for reload policy remain
open.

The standalone payload functions can be compiled and exercised without hgraph.
The node/graph fragments use the public C++ authoring surface and can be
syntax-checked together with the preamble. A syntax check is not a graph
execution test, an installed-SDK acceptance run, or proof of HGL compiler
support. Runtime wiring/lifetime references are
[native switch tests](../../../tests/cpp/test_switch.cpp),
[static node tests](../../../tests/cpp/test_static_node.cpp), and
[graph wiring tests](../../../tests/cpp/test_graph_wiring.cpp).
