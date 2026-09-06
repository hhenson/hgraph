# HGL standard-library design corpus

This folder will describe the core hgraph node and graph library in HGL as the
required language contracts are agreed. It starts with worked examples that
exercise those contracts; these example functions are not new public library
components. The component inventory and HGL declarations remain to be added.

Only agreed syntax belongs in the corpus. Open questions should be recorded
in the owning design document, without filling gaps with speculative
declarations or native-binding syntax.

## Conditional results

[conditional-results.hgl](examples/conditional-results.hgl) covers one and
multiple predeclared variables assigned by temporal `if` branches and used by
later statements. It also covers forwarding an initialized variable by
reference through a branch that leaves it unchanged. The
[conditional control-flow design](../docs/design/control-flow.md) explains
branch captures, output signatures, bundle remapping, and remaining decisions.

[conditional-mixed-results.hgl](examples/conditional-mixed-results.hgl) combines
an `if` expression result with an escaping assignment. They share one generated
bundle output, then remap to the expression's receiving binding and the
predeclared variable. This remains a design example awaiting compiler support.

[conditional-early-return.hgl](examples/conditional-early-return.hgl) covers an
early return from one temporal branch. The remaining function body becomes
the other branch's continuation, including its input captures and child-graph
lifetime. It remains a design example awaiting compiler support.

[conditional-sinks.hgl](examples/conditional-sinks.hgl) covers an outputless
conditional: `debug_print("enabled", value)` is wired through the switch,
while `debug_print("always", value)` is always wired outside it. The label
precedes the time-series argument. This remains a design example awaiting
compiler support.

[conditional-unassigned-result.hgl](examples/invalid/conditional-unassigned-result.hgl)
is intentionally invalid: the escaping variable has no incoming binding and
is assigned only on the true path before it is used. It records the agreed
compile-time definite-assignment error. The compiler now implements this
path-sensitive check, although the temporal conditional itself still awaits
backend lowering.

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

[enum-duplicate-number.hgl](examples/invalid/enum-duplicate-number.hgl) and
[enum-implicit-duplicate-number.hgl](examples/invalid/enum-implicit-duplicate-number.hgl)
record the initial rejection of duplicate numbers, including an automatic
number that collides with an earlier explicit member.

These are design fixtures awaiting compiler support. The
[remaining enum decisions](../docs/design/type-extensions.md#enum-types)
include integer range/overflow, unknown imported values, and type/native mapping.

## String conversion

[string-conversion.hgl](examples/string-conversion.hgl) uses the agreed
Python-style `str(value)` spelling in a node handler and in temporal graph
composition. The constant enum conversion is in `enum-values.hgl` above.
The [paired HGL/C++ mappings](../docs/developer-guide/enum-cpp-mappings.md#conversion-in-nodes-and-graphs)
show conversion within native node evaluation and wiring through native
`str_`, without making a graph read current payloads. These are design
fixtures, not passing compiler examples or a blanket Python formatting promise.

## Iteration

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
backend-parity fixture. The remaining corpus examples depend on broader
escaping-result, continuation, and sink work. Typed
declarations without initializers and their definite-assignment checks are
implemented. The files remain design inputs, not runnable tests, and are
deliberately outside `language/examples/`, whose `.hgl` files are checked by
CTest.
