# HGL standard library and design corpus

This folder develops the core hgraph node and graph library in HGL as the
required language contracts are agreed. The compiled modules are under
[`hgl/hgraph`](hgl/hgraph); `standard.hgl` now provides the first real HGL
operator implementations, `len_` and `is_empty`, without replacing their
production C++ identities yet. The worked examples below exercise broader
contracts; those example functions are not new public library components. A
[recovered historical inventory](inventory.md) and the broader
[operator-family migration prototypes](hgl/README.md) are available as review
inputs. Their counts and provisional forms are not accepted current syntax and
must be refreshed or resolved before migration.

The [recovery manifest](recovery.md) accounts for every input from the earlier
combined prototype and records which pieces are now implemented, restored for
review, or intentionally superseded.

[`hgl/hgraph/operators.hgl`](hgl/hgraph/operators.hgl) now adds executable
contracts and native-delegating implementations for `add_`, `sub_`, `mul_`,
`div_`, `floordiv_`, `mod_`, the six comparisons, `and_`, `or_`, `neg_`, and
`not_`. It materializes numeric arithmetic (including mixed `i64`/`f64`),
string concatenation, supported primitive comparisons, Boolean logic, and
numeric negation. These `hgraph.operators.*` migration identities do not replace
the native identities used by symbols. Broader temporal, structural, and
downstream domains are not claimed as HGL implementation coverage.

The [operator design](../docs/design/operators.md) records the symbol mappings,
domain-bound algebraic properties, lifting/result signatures, numerical
exceptions, `//` floor division, and the `#` / `/* ... */` comment syntax. The
[paired HGL/C++ scenarios](../docs/developer-guide/operator-cpp-mappings.md)
show graph, node, constant, and native-kernel behavior.

Only agreed syntax belongs in the corpus. Open questions are recorded in the
owning design document or beside a compiled prototype with an explicit blocker,
without filling gaps with speculative declarations or native-binding syntax.

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
compile-time definite-assignment error. The compiler implements this
path-sensitive check, and CTest
(`hgraph_language_stdlib_invalid_conditional-unassigned-result`) runs
`hgl check` over the fixture and requires the diagnostic its `# expect:`
comment names. The single explicit-two-branch result form has backend
lowering; the invalid example continues to guard the broader rule.

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

[enum-switch.hgl](examples/enum-switch.hgl) covers exhaustive enum dispatch
in node and graph forms, partial coverage with no default, and a supplied
default. [Enum-switch HGL/C++ mappings](../docs/developer-guide/enum-switch-cpp-mappings.md)
show the local payload dispatch and graph branch structure. Invalid fixtures
cover [duplicate resolved cases](examples/invalid/enum-switch-duplicate-case.hgl),
an [integer label](examples/invalid/enum-switch-integer-case.hgl), a
[different enum's label](examples/invalid/enum-switch-other-enum-case.hgl), and
[an unassigned result despite full coverage](examples/invalid/enum-switch-unassigned-result.hgl).
Full coverage needs no default, but generated dispatch retains no-match
failure. Partial coverage is permitted; its unmatched path fails unless a
default handles it. These are design fixtures, not compiler tests.

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
Enum identity and explicit integer conversion are agreed. Calls on the type
use `keys(Mode)` (member-name strings), `values(Mode)` (assigned integers), and
`elements(Mode)` (enum instances). They return immutable fixed-size scalar
lists, sized by the member count. All three iterate in declaration order, regardless
of explicit numbers. [enum-enumeration-order.hgl](examples/enum-enumeration-order.hgl)
uses non-monotonic numbering, type-operand calls, indexing, and reuse, with
[paired HGL/C++ expectations](../docs/developer-guide/enum-cpp-mappings.md#declaration-order-enumeration).
The results are constant data rather than time series or borrowed iterators.
These remain design fixtures awaiting compiler support.

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
earlier no-`elements` rule. The compiler now keeps `values` for keyed/named
value projections and uses `elements` for list/set membership traversal; the
two spellings are deliberately not aliases. Existing graph-loop restrictions
are unchanged.

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

## Runtime handler defaults

[when-defaults.hgl](../examples/when-defaults.hgl) exercises the implemented
relationship between explicit, empty, and omitted handler selectors.
`modified()` means any temporal parameter was modified and `valid()` means
every temporal parameter is top-level valid. Omitting either selector supplies
that default, making `when { ... }` equivalent to
`when modified() && valid() { ... }`.

The example is checked, compiled as a scripted native module, and evaluated by
`hgl test`. Focused emitter tests also prove that selectors nested in residual
Boolean expressions do not suppress missing top-level defaults and that empty
selector calls outside a handler fail closed.

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
their definite-assignment checks are implemented. Files left here remain
deliberately outside `language/examples/`, whose `.hgl` files are checked by
CTest; their own test status is below.

## Fixture status

Every fixture under `examples/` carries a `module stdlib.examples.<name>`
line (`stdlib.examples.invalid.<name>` under `invalid/`), so `hgl check`
reaches the rule a fixture documents instead of stopping at the missing
module declaration. Each `invalid/` fixture also opens with a
`# expect: <substring>` comment naming the diagnostic it must produce;
`tests/stdlib/check_invalid_fixture.cmake` runs `hgl check` and passes only
when the check fails and its output contains every expectation.

| Fixture | Construct | Status |
| --- | --- | --- |
| `invalid/conditional-unassigned-result.hgl` | definite assignment | registered as `hgraph_language_stdlib_invalid_conditional-unassigned-result`; the expectation is the checker's wording |
| `invalid/switch-temporal-case.hgl`, `invalid/enum-switch-*.hgl` | `switch` | not registered: `switch` is not parsed yet |
| `invalid/enum-*.hgl` | `enum` | not registered: `enum` is not parsed yet |
| `switch-scenarios.hgl`, `enum-*.hgl`, `string-conversion.hgl` | `switch`, `enum`, `str(value)` | valid design fixtures; not checked until their construct parses |
| `elements-iteration.hgl` | `elements` | design fixture; spelling is implemented and covered by executable compiler examples |

An unregistered fixture's `# expect:` substring records the agreed rule in
the fixture's own words; it is aligned with the checker's diagnostic and the
fixture is added to `_hgl_stdlib_invalid_fixtures` in `tests/CMakeLists.txt`
the moment its construct lands. Adding the comment and module line does not
change what a fixture documents.
