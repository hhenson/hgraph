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
compile-time definite-assignment error, without prescribing diagnostic wording
or claiming that the compiler implements that check yet.

## Iteration

[fixed-list-iteration.hgl](examples/fixed-list-iteration.hgl) uses a graph-phase
`for` to wire one sink per fixed-list child connection. It records the agreed
[phase-dependent iteration model](../docs/design/iteration.md).

[dynamic-map-iteration.hgl](examples/dynamic-map-iteration.hgl) covers an
independent dynamic graph-loop body: one sink child graph per map key, with a
shared temporal capture. The initial dynamic-loop design excludes assignments
to enclosing variables and loop-carried reductions.

The [deferred map/reduce option](../docs/design/iteration.md#deferred-option-map-plus-reduce)
records future unordered map reductions and the linear reduction option for
lists when index order matters. Neither reduction lowering is initially
supported by graph `for`; the example in that section is deliberately marked
unsupported, not added here as a supported loop contract.

## Compiler status

These design examples depend on features requiring compiler work, including
temporal graph conditionals, graph-phase iteration, and typed declarations
without initializers. They are design inputs, not runnable tests, and are
deliberately outside `language/examples/`, whose `.hgl` files are checked by
CTest. No runtime or compiler implementation is added here.
