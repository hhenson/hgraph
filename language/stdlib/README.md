# HGL standard-library design corpus

This folder will describe the core hgraph node and graph library in HGL as the
required language contracts are agreed. It starts with worked examples that
exercise those contracts; these example functions are not new public library
components. The component inventory and HGL declarations remain to be added.

Only agreed syntax belongs in the corpus. Open questions should be recorded
in the owning design document, without filling gaps with speculative
declarations or native-binding syntax.

## Conditional results

[conditional-result.hgl](../examples/conditional-result.hgl) is the executable
single-result case: one predeclared variable is assigned in both explicit
temporal branches, remapped from the native switch output, and used by later
composition. [conditional-results.hgl](../examples/conditional-results.hgl) is
the executable multiple-result case: the branch callables return one
compiler-generated structural TSB and later composition consumes its remapped
fields. The executable
[conditional-forwarding.hgl](../examples/conditional-forwarding.hgl) covers an
implicit false branch and per-field forwarding within a structural result. The
[conditional control-flow design](../docs/design/control-flow.md) explains
branch captures, output signatures, bundle remapping, and remaining decisions.

The executable
[conditional-mixed-results.hgl](../examples/conditional-mixed-results.hgl)
combines an `if` expression result with an escaping assignment. They share one
generated bundle output, then remap to the expression's receiving binding and
the predeclared variable.

The executable
[conditional-early-return.hgl](../examples/conditional-early-return.hgl)
covers an early return from one top-level temporal branch. The remaining
function body becomes the other branch's continuation, including its input
captures and child-graph lifetime. Nested temporal continuations remain in the
design corpus.

Outputless temporal conditionals have graduated into the executable
[conditional-sinks.hgl](../examples/conditional-sinks.hgl) compiler example.
`debug_print("enabled", value)` is wired through the native sink switch, while
`debug_print("always", value)` is always wired outside it. The example also
covers a discarded sink conditional inside a value-producing graph. The label
precedes the time-series argument.

[conditional-unassigned-result.hgl](examples/invalid/conditional-unassigned-result.hgl)
is intentionally invalid: the escaping variable has no incoming binding and
is assigned only on the true path before it is used. It records the agreed
compile-time definite-assignment error. The compiler now implements this
path-sensitive check. The single explicit-two-branch result form now has
backend lowering; the invalid example continues to guard the broader rule.

## Explicit switch

[Explicit switch dispatch](../docs/design/switch.md) records the agreed
node-style native C++ path and graph-style selector validation, captures,
results, and `default: ...` fallback. Unmatched values without a default must
fail. The agreed form is `switch selector { case value: ... default: ... }`;
case values must be expressible as source constants.

[switch-scenarios.hgl](examples/switch-scenarios.hgl) contains the node, graph,
result, early-return, sink, and state-lifetime examples. The
[paired HGL/C++ mappings](../docs/developer-guide/control-flow-cpp-mappings.md)
put the same source before its native mapping and expected behaviour.
[switch-temporal-case.hgl](examples/invalid/switch-temporal-case.hgl) records an
intentional error: a temporal parameter cannot be used as a case constant.
These fixtures await switch parser, checking, and lowering support; they are
not executable acceptance tests.

## Enum values

[enum-values.hgl](examples/enum-values.hgl) covers the agreed declaration and
`Mode::first` member-reference forms, explicit numbering with `= constant`,
automatic numbering from zero, and continuation after an explicit number.
`str(Mode::first)` returns the member name without a type prefix or number.
The [paired HGL/C++ mappings](../docs/developer-guide/enum-cpp-mappings.md)
show the resolved numbers and expected strings.

The same fixture uses `Mode(10)` and `Mode("first")` to produce `Mode::first`.
Construction checks assigned numbers or exact member names and rejects unknown
values. The [conversion mappings](../docs/developer-guide/enum-cpp-mappings.md#checked-conversion-into-an-enum)
show checked C++ lookups and the checking/wiring/evaluation failure boundary.
[enum-conversion-unknown-number.hgl](examples/invalid/enum-conversion-unknown-number.hgl)
and [enum-conversion-unknown-name.hgl](examples/invalid/enum-conversion-unknown-name.hgl)
are intentional constant-conversion errors, not implemented compiler tests.

[enum-duplicate-number.hgl](examples/invalid/enum-duplicate-number.hgl) and
[enum-implicit-duplicate-number.hgl](examples/invalid/enum-implicit-duplicate-number.hgl)
record the initial rejection of duplicate numbers, including an automatic
number that collides with an earlier explicit member.

[enum-number-range.hgl](examples/enum-number-range.hgl) covers negative
numbering, both signed `i64` endpoints, and an explicit reset after the
maximum. The [paired HGL/C++ range examples](../docs/developer-guide/enum-cpp-mappings.md#signed-range-and-overflow)
explain the compile-time, no-wrap rule. Intentional errors cover an explicit
number [above the maximum](examples/invalid/enum-number-above-range.hgl),
[below the minimum](examples/invalid/enum-number-below-range.hgl), and
[automatic successor overflow](examples/invalid/enum-number-overflow.hgl).

These are design fixtures awaiting compiler support. The
[remaining enum decisions](../docs/design/type-extensions.md#enum-types)
include unknown imported values and native mapping.
Enum identity and explicit integer conversion are agreed, as are enumeration
through `keys` (member-name strings), `values` (assigned integers), and
`elements` (enum instances). All three iterate in declaration order, regardless
of explicit numbers. [enum-enumeration-order.hgl](examples/enum-enumeration-order.hgl)
uses non-monotonic numbering, with [paired HGL/C++ expectations](../docs/developer-guide/enum-cpp-mappings.md#declaration-order-enumeration).
Their remaining source and result-shape details are recorded in the design
document; no speculative enumeration call fixtures are
added here.

## String conversion

[string-conversion.hgl](examples/string-conversion.hgl) uses the agreed
Python-style `str(value)` spelling in a node handler and in temporal graph
composition. The constant enum conversion is in `enum-values.hgl` above.
The [paired HGL/C++ mappings](../docs/developer-guide/enum-cpp-mappings.md#conversion-in-nodes-and-graphs)
show conversion within native node evaluation and wiring through native
`str_`, without making a graph read current payloads. These are design
fixtures, not passing compiler examples or a blanket Python formatting promise.

## Iteration

[elements-iteration.hgl](examples/elements-iteration.hgl) records the agreed
`elements` spelling for list and set traversal, with paired HGL/C++ examples
in the [iteration design](../docs/design/iteration.md). It covers fixed-list
graph wiring and a node counting added set members. This supersedes the
earlier no-`elements` rule but remains outside the executable corpus; the
compiler examples below still use `values`. Compatibility for that older
spelling remains undecided. Existing graph-loop restrictions are unchanged.

Fixed temporal-list traversal has graduated from this design-only corpus into
the executable compiler example
[fixed-list-iteration.hgl](../examples/fixed-list-iteration.hgl). Both compiler
backends wire one body per child connection under the agreed
[phase-dependent iteration model](../docs/design/iteration.md).

Independent dynamic map and unbounded-list traversal has also graduated into
the executable
[dynamic-collection-iteration.hgl](../examples/dynamic-collection-iteration.hgl)
example. Both backends lower one sink child graph per key or index and pass
shared temporal captures explicitly. Assignments to enclosing variables and
loop-carried reductions remain excluded.

The [deferred map/reduce option](../docs/design/iteration.md#deferred-option-map-plus-reduce)
records future unordered map reductions and the linear reduction option for
lists when index order matters. Neither reduction lowering is initially
supported by graph `for`; the example in that section is deliberately marked
unsupported, not added here as a supported loop contract.

[Graph-phase iterator predicates](../docs/design/iteration.md#deferred-graph-phase-predicates)
are also deferred. The proposed predicate-to-switch conversion is not an
agreed contract and has no corpus example; further loop design is paused.

## Compiler status

The smallest temporal graph conditional—an explicit two-branch expression with
one tail value and temporal captures—is implemented in both backends and the
backend-parity fixture. Outputless temporal conditionals with an optional
block `else` are also implemented through the native sink switch. One escaping
assignment that is assigned by both explicit branches is implemented in both
backends and the generated-code fixture. Several escaping assignments are also
implemented through a compiler-generated structural TSB. Mixed
expression/assignment results share that same lowering and are implemented in
both backends. Existing bindings can be forwarded by reference, independently
for each structural result field. A value-producing temporal conditional may
omit `else`; both backends supply a typed never-ticking false result.
Continuations remain design inputs. Typed declarations without initializers and
their definite-assignment checks are implemented. Files left here are not
runnable tests and remain deliberately outside `language/examples/`, whose
`.hgl` files are checked by CTest.
