# Explicit switch dispatch

Status: node-style and graph-style dispatch, selector validation, branch
capture/result analysis, and `switch selector { case value: ... default: ... }`
agreed, 2026-09-06. Case values must be expressible as constants in source.
Duplicate cases after constant resolution and enum exhaustiveness checks are
also agreed. Partial enum coverage remains permitted with no-match failure.
Compiler implementation remains separate work.

[Paired HGL and C++ examples](../developer-guide/control-flow-cpp-mappings.md)
describe node and graph scenarios, captures and results, defaults and failures,
sinks, early returns, and state lifetime. The source examples also live in the
[standard-library design corpus](../../stdlib/examples/switch-scenarios.hgl).

## Source form and constant case values

```hgl
const first_mode: i64 = 0
const second_mode: i64 = 1

fn choose(mode: i64, x: i64, y: i64, fallback: i64) -> i64 {
    var r: i64
    switch mode {
        case first_mode:
            r = x + 1
        case second_mode:
            r = y - 1
        default:
            r = fallback * 3
    }

    return r * 2
}
```

`switch` introduces the selector and a braced set of cases. `case` introduces
a constant value followed by `:` and its body; `default:` introduces the
no-match body. A body ends at the next case/default label or the switch's
closing brace. Statements retain the existing newline rules. There is no
source `break` requirement or implicit fallthrough. An explicitly empty
`default:` body is permitted and differs from omitting the default.

Each case value must be expressible as a constant in HGL source, compatible
with the selector's admitted key type. Literals and named constants such as
those above use the existing constant-value rules; constant expressions must
resolve before evaluation. A temporal input, a state read, or another
evaluation-dependent expression cannot be a case value, even if its value
happens not to change during a particular run. See the
[invalid temporal-label fixture](../../stdlib/examples/invalid/switch-temporal-case.hgl).

The selector is independent of this restriction: it may be wiring-time or
temporal, according to the function phase. Constant case labels do not make
a temporal selector a wiring-time choice.

Enum declarations and qualified member references are agreed: a member such
as `Mode::first` can be used in `case Mode::first:`. Explicit/automatic
numbering, member-name stringification, and initial rejection of duplicate
enum numbers are also agreed. String conversion uses `str(value)`; the complete
native enum mapping remains open. See
[enum requirements](type-extensions.md#enum-types).

An enum selector retains its enum type: case labels must be members of that
same enum, not integers or members of another enum with coincident numbers.
Explicit conversion to an integer is a separate operation; numbering alone
does not change the selector's type.

## Duplicate cases and enum coverage

Resolve and type-check each case constant before checking for duplicates.
Reject repeated resolved case values within the same switch, even when their
source expressions differ. For the numbered `Mode` declaration, `Mode::first`
and `Mode(10)` both denote the same member and cannot label separate cases.
A named constant resolving to that member has the same effect. This is a
source-checking error, not first-match or last-match dispatch. It is distinct
from rejecting duplicate numbers in an enum declaration.

For an enum selector, covering every declared member establishes exhaustiveness
over that enum's declared values. No `default` is required. Partial coverage
is also permitted: an uncovered member selects the supplied default or fails
if there is none. These checks apply to wiring-time graph choices, temporal
graph dispatch, and node-local dispatch alike.

Using the three-member `Mode` from [enum types](type-extensions.md#enum-types):

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

Every member has a case and each case assigns `r` before its later use.
Exhaustive coverage does not by itself prove definite assignment: a case that
reaches that use must still supply `r`. Conversely, a partial switch's
no-match failure path does not reach the use and need not assign it.

Generated dispatch retains the no-match failure path even when checking proves
coverage of all declared enum members. Do not replace it with silent
continuation or an unchecked unreachable assumption. This does not admit
unknown enum values at native import boundaries; that remains separate design
work. A supplied default remains a real branch with the existing capture,
result, and definite-assignment checks.

[Paired enum-switch HGL/C++ examples](../developer-guide/enum-switch-cpp-mappings.md)
cover exhaustive and partial dispatch, a supplied default, and intentional
duplicate/type errors. No new source syntax or compiler implementation is
introduced by these checks.

## One construct in both function phases

Explicit `switch` must be supported in both node-style functions and graph
composition functions. These are the existing function phases, not new
declaration keywords. A switch does not by itself force a function to become a
runtime node.

| Context | Dispatch behaviour |
| --- | --- |
| Graph composition, wiring-time selector | Select and compose the matching case, or the default, during wiring. |
| Graph composition, temporal selector | Wire native `switch_` with generated branch callables and boundary signatures. |
| Node evaluation, including a `when` handler | Dispatch on the current readable selector value inside the node's evaluation. |

Case labels are source-expressible constant values. One matching branch is
selected; there is no implicit fallthrough from one case body into the next.
Failing to match any case is the no-match situation described below, not
sequential case execution.

## Validate the selector before lowering

Resolve the selector's type and phase before choosing a lowering. The selector
must supply a value suitable for comparison with the case labels, rather than
an arbitrary temporal structure or an untyped condition. It need not be a
Boolean condition as it is for `if`.

For a temporal graph switch, the selector must bind to the native `switch_`
scalar-key input, and the case values must be compatible with the selected key
contract. The native input is `TS<ScalarVar<K>>`; source compatibility and any
REF binding adaptation must use the existing type and wiring rules. Wiring
must not read the selector's current runtime payload to choose a branch.

In node evaluation, the selector must be readable under the existing validity
and access rules. SIGNAL has no selectable payload, and an opaque REF does not
permit reading the referenced value to make a case comparison. This does not
restrict the separately supported forwarding of REF values by a branch.

The exact set of admitted scalar key types, including imported native types,
is not fixed here. A backend's ability to emit a particular C++ statement is
not by itself evidence that every source type has a valid switch-key contract.

## Node-style lowering to C++

A node-style switch must lower to native C++ dispatch within the generated
node's evaluation code. It must not require Python execution or create a
graph-level `switch_` merely to implement local control flow.

The generated C++ may use a C++ `switch` where the admitted selector and case
types permit that statement. For other admitted types, equivalent native C++
comparison-based dispatch may be needed. This is a code-generation distinction,
not an agreement to admit additional selector types or new source syntax.

Branch statements operate on the current evaluation's local values and the
enclosing node's declared state and output capabilities. Changing the selected
case does not create or restart a child graph. State and lifecycle continue to
belong to the enclosing node. An explicit return has the existing node-return
meaning: produce the requested output and finish the current evaluation.

## Graph-style branch signatures and results

After validating the temporal selector, use the same analysis as
[temporal `if`/`else`](control-flow.md#branch-signatures), extended across all
cases and the optional default:

1. Determine return targets, branch continuations, lexical dependencies, and
   predeclared escaping variables. Branch-local declarations do not escape.
2. Separate wiring-time captures from temporal inputs. Form shared temporal
   input slots by source identity and remap each branch's captures onto them.
   Preserve REF forwarding intent and SIGNAL input restrictions. The selector
   is the switch key; a branch which also reads it has that lexical dependency.
3. Derive compatible result signatures, including used expression results and
   escaping assignments. Zero results use the native outputless switch path;
   one result is returned directly; multiple results use a generated bundle
   and remap to the enclosing bindings.
4. Apply definite assignment and early-return continuation rules to every path
   reaching a use. The default participates in the same analysis as every
   other branch; it is not a loosely typed fallback.

The exact common result-schema and per-field REF adaptation requirements from
[forwarding an existing binding](control-flow.md#forwarding-an-existing-binding)
also apply. Underlying type compatibility must not hide mismatched native
bundle fields. If the public native API cannot express a required adaptation,
the compiler must reject that lowering until the support exists.

Explicit branch arguments still obey the native callable signature rules;
forming a union of captures does not permit passing arbitrary arguments to a
branch whose explicit signature rejects them. Reuse the native capture and
boundary remapping machinery described in the conditional design.

The native switch owns branch activation, binding, and lifetime. Computations
wired outside the switch remain outside it. Only the selected child executes,
and selecting a previously stopped branch creates a fresh instance under the
existing native policy. This differs from local dispatch inside a node.

## Default and no-match failure

The agreed `default:` body catches selector values that match none of the
explicit cases. In graph composition it becomes the native `switch_` default
branch, with the same captures, result checks, and lifecycle as other branches.
In node-style code it becomes the native C++ dispatch fallback.

If no case matches and no default was supplied, execution must fail:

- For a wiring-time selector, fail when attempting that composition.
- For a temporal graph selector, use the native switch no-match failure.
- Inside a node, fail the current evaluation through the normal error path.

Do not silently continue, keep selecting the previous branch, or synthesize a
never-ticking result for this situation. This differs from the agreed omitted
`else` of a value-producing temporal `if`. The rule also applies to outputless
switches.

A no-match failure path does not reach a later use of an escaping variable;
definite assignment must check the branches that do reach that use. A supplied
default which reaches the use must provide the required binding, either by
assignment or by forwarding an existing incoming binding.

Default is a no-match branch, not an exception handler for failures inside a
selected case. Nor does this rule make an invalid or unreadable selector into
a valid unmatched key: existing selector validity and access rules still apply.

## Native references and remaining work

The native [higher-order operator contract](../../../include/hgraph/lib/std/operators/higher_order.h)
provides the switch key, cases, optional default, and outputless form.
[Switch execution](../../../src/hgraph/runtime/switch_node.cpp) owns dispatch
and lifecycle; [public-wiring tests](../../../tests/cpp/test_switch.cpp) cover
default selection, no-match failure, outputless branches, and fresh branch
instances on reselection.

The exact admitted selector types, native enum representation/import rules,
and any exposure of native reload policy remain to be discussed. Duplicate
resolved case rejection and declared-member exhaustiveness are agreed source
checks; their compiler implementation remains pending. The statement form is
illustrated in `language/stdlib/`; a switch expression-value surface is not added by these
examples. This record does not add parser, IR, backend, or runtime
implementation, and leaves the deferred `for` work untouched.
