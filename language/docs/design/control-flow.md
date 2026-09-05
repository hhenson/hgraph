# Conditional control flow

Status: agreed conditional strategy, 2026-09-05; compiler implementation is
separate. This record uses the existing `if`/`else` syntax. It does not settle
the other control-flow constructs or introduce new keywords.

## The three conditional contexts

| Context | Meaning of `if` |
| --- | --- |
| Graph composition with a wiring-time Boolean condition | Choose which branch to wire. |
| Graph composition with a temporal Boolean condition | Wire a native switch with branch callables selected by the condition. |
| Node evaluation, including a `when` handler | Execute an ordinary conditional using current readable values. |

A temporal condition in a graph function uses the switch strategy, following
the approach of the Arrow API. It does not classify the containing function
as a runtime node. Graph composition still runs during wiring; the native
switch owns the runtime selection and execution of its child graph.

## Temporal condition in graph composition

The following uses existing syntax with the newly agreed temporal-condition
semantics. It is a design example, not a currently executable compiler test:

```hgl
fn choose(condition: bool, value: f64) -> f64 {
    if condition {
        value + 1.0
    } else {
        value - 1.0
    }
}
```

The condition becomes the Boolean selector of a switch. Each branch's
composition becomes a child callable, with the required inputs connected
through the native child-graph boundary. The branches are not evaluated as
two eager outer argument expressions before selecting their results.

The runtime behavior is the existing native switch contract:

- Only the selected child graph runs.
- A changed condition stops the previous child and starts the selected child.
- Returning to a previously selected branch creates a fresh child instance;
  the switch does not keep inactive branch state suspended for later resumption.
- Under the native default policy, an unchanged Boolean value does not rebuild
  the child merely because the condition ticks again.
- Output compatibility, binding, and sampling follow the native switch rules.

The lifetime boundary covers computations composed inside the branch. A source
or computation wired outside the conditional and supplied to a branch remains
outside that lifetime boundary. Selecting a branch does not stop its external
producers.

The existing `if_then_else` library operator continues to describe selection
between already-wired outputs. It is distinct from the agreed temporal `if`
strategy, which controls child-graph execution.

## Omitted else

A value-producing temporal `if` without an `else` uses a typed, never-ticking
false branch, matching the Arrow approach. This is now an agreed HGL rule.
The false branch has the required result schema but produces no output tick;
it does not manufacture a default scalar value.

An assignment to an already-bound enclosing variable has a different false
branch: it forwards the incoming binding, as described below. That case does
not replace the existing connection with a never-ticking source.

A conditional early return is also distinct: its non-returning path continues
through the remaining function body, as described under
[Early returns](#early-returns-and-continuations).

## Results used after the conditional

A temporal conditional can supply bindings used by later statements; it is
not limited to a tail expression returned directly from the function:

```hgl
fn use_conditional_result(condition: bool, x: i64, y: i64) -> i64 {
    var r: i64
    if condition {
        r = x + 1
    } else {
        r = y - 1
    }

    return r * 2
}
```

This example includes the agreed typed declaration without an initializer,
`var r: i64`. It is design syntax awaiting compiler support, not an executable
example for the current language test corpus.

An escaping variable must be declared before the conditional in an enclosing
scope. Its branch-assigned binding is needed outside the conditional. A
variable declared inside a branch remains branch-local; naming it does not
make it visible after the conditional.

When the only result is one escaping variable, each generated branch callable
returns that variable's final binding directly. Here the true branch takes
`x` and returns `x + 1`; the false branch takes `y` and returns `y - 1`. The switch returns the
selected `i64` time-series connection, and the enclosing `r` is remapped to
that output. The multiplication by two is composed once outside the switch
and consumes this connection.

These are graph lexical bindings. Assignments inside the branch determine its
output connection; they do not create a persistent runtime variable shared
between the parent and child graphs. The uninitialized `r` is not an input
capture: neither branch reads an incoming binding for it.

### Multiple escaping variables

When multiple variables escape, each generated branch returns a bundle. The
switch produces that bundle, and its fields are remapped to the corresponding
enclosing variables:

```hgl
fn use_conditional_results(condition: bool, x: i64, y: i64) -> i64 {
    var r: i64
    var adjustment: i64
    if condition {
        r = x + 1
        adjustment = x
    } else {
        r = y - 1
        adjustment = y
    }

    return r * 2 + adjustment
}
```

| Escaping binding | True branch output | False branch output | Enclosing binding after the switch |
| --- | --- | --- | --- |
| `r` | `x + 1` | `y - 1` | The switch output's `r` field. |
| `adjustment` | `x` | `y` | The switch output's `adjustment` field. |

The bundle is a compiler-generated branch-output interface. The author does
not need to declare a bundle type, construct a bundle literal, or write a
remapping operation. The fields remain time-series connections with native
bundle/switch semantics; this does not introduce a scalar tuple snapshot or
an additional simultaneous-tick guarantee.

Both examples assign every escaping variable in both branches. An existing
incoming binding also allows a branch to leave an escaping variable unchanged.

### Forwarding an existing binding

If an escaping variable already has a binding before the conditional, a branch
that does not assign it forwards that incoming binding. This also supplies the
implicit false branch when there is no `else`:

```hgl
fn use_existing_conditional_result(condition: bool, x: i64) -> i64 {
    var r: i64 = x
    if condition {
        r = r + 1
    }

    return r * 2
}
```

The true branch processes the incoming `r` and returns the connection for
`r + 1`. It may consume an ordinary `i64` temporal input. The implicit false
branch only returns the incoming `r`, so its generated input/output contract
uses `ref<i64>` to forward the connection without observing or copying its
value ticks. Downstream consumers continue to receive ticks from the selected
source through that connection.

The incoming binding is `x` for both branches. Lowering resolves that binding
before remapping the enclosing `r` to the switch output; `r = r + 1` does not
create feedback from that output. When false is selected, the result follows
`x`, not a value remembered from a previous execution of the true branch.

Capture access is determined per branch. A pure forwarding branch takes the
incoming time series by reference. Where analysis establishes that the input
will be processed, an ordinary temporal input is sufficient; reference
capture is not required solely to avoid pass-through copying. Merely finding
a use on one path is not proof that the input is processed on every path.

This is the compiler specifying the intent of a generated component, not a
change to the author's `var r: i64` declaration or a general rule for rewriting
user-declared types. Explicit reference access restrictions still apply.
Branch input signatures can differ in REF qualification while remaining
compatible in underlying type. Their native input bindings must preserve the
forwarding connection and adapt it for value-consuming downstream nodes.
Deduplicating a captured source must not discard these per-branch contracts.

The same forwarding rule applies independently to each escaping field when
multiple results are bundled. A branch can supply a newly computed binding
for one field and forward another field's incoming binding by reference.
It does not imply an outer reference over the whole bundle. Lowering preserves
the escaped binding's resolved declared temporal schema as the common output
schema for each result slot. For an ordinary `T` result, a forwarding branch
retains its `ref<T>` capture and explicitly dereferences that connection at the
branch-output boundary before packing the field; a computed branch already
supplies the same ordinary temporal schema. For an explicitly declared
`ref<T>` result, the result slot remains `ref<T>` and a forwarding capture is
packed without dereferencing it. Every branch must produce that same declared
schema; a branch that cannot produce the required reference binding is rejected.
The branch bundles consequently have identical schemas at every field, not
merely compatible root types. If the public native API cannot express the
required per-field reference adaptation, HGL must reject the conditional until
the native support exists rather than send mismatched bundles to `switch_`.

The corresponding design-corpus source is
[conditional-results.hgl](../../stdlib/examples/conditional-results.hgl).

### Definite assignment

An escaping variable must have a binding on every path reaching its use. It
must either have an incoming binding before the conditional or be assigned
on every path reaching that use. A type annotation alone does not supply a
connection. Using a variable without definite assignment is a compile-time
error.

The following is intentionally invalid under the agreed design:

```hgl
fn unassigned_conditional_result(condition: bool, x: i64) -> i64 {
    var r: i64
    if condition {
        r = x + 1
    }

    return r * 2
}
```

On the false path, `r` has neither an assignment nor an incoming binding to
forward. Its later use must be rejected; lowering must not silently create
a typed never-ticking source for it. That fallback remains specific to the
agreed value-producing `if` expression without `else`.

Definite assignment checks the existence of a binding, not the runtime
validity of its time series. A bound source can be invalid or have produced
no tick without making its variable unassigned. For multiple escaping
variables, check each one independently before constructing the result bundle.

The negative design-corpus example is
[conditional-unassigned-result.hgl](../../stdlib/examples/invalid/conditional-unassigned-result.hgl).
It records the required rejection, not an implemented compiler test.

## Expression results and escaping assignments

A conditional may supply an expression result and also assign variables that
escape to the enclosing scope:

```hgl
fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var r: i64

    let result = if condition {
        r = x + 1
        x * 2
    } else {
        r = y - 1
        y * 3
    }

    return result + r
}
```

The final expression of each branch supplies the value of the `if`
expression. Assignments to the predeclared `r` supply a separate escaping
binding. Count the used expression result alongside the escaping variables
when determining the generated switch output.

Here both branches return a common compiler-generated bundle with two
time-series connections:

| Result | True branch | False branch | Receiver after the switch |
| --- | --- | --- | --- |
| Expression result | `x * 2` | `y * 3` | The initializer for `result`. |
| Escaping binding | `x + 1` | `y - 1` | The remapped enclosing `r`. |

The expression-result field is a compiler detail; it needs no source-level
field name or bundle declaration. `result` receives the value of the complete
`if` expression and is not an escaping variable. Only `r` needs the declaration
before the conditional, because it is assigned inside the branches and used
outside them.

The true branch captures `x`, the false branch captures `y`, and the switch
receives `condition`, `x`, and `y`. The final addition is composed outside the
switch after the two results have been remapped. Both results retain native
time-series bundle semantics, not scalar snapshot semantics.

The common output rule counts the used expression result, if any, plus the
escaping variables:

- Zero results: an outputless switch, preserving any conditional sink wiring.
- One result: return that connection directly.
- Multiple results: return a generated bundle and remap its fields to the
  expression consumer and enclosing variables as appropriate.

The existing definite-assignment and REF-forwarding rules continue to apply
to escaping variables. Combining the results adds no new source syntax and
does not expose branch-local declarations to the enclosing scope.

See [conditional-mixed-results.hgl](../../stdlib/examples/conditional-mixed-results.hgl)
for this agreed design example. Compiler support remains separate work.

## Early returns and continuations

An explicit `return` inside a temporal branch returns from the enclosing HGL
function, not merely from the branch lambda generated during lowering. A path
that returns skips the remaining function body. The non-returning path's
continuation therefore belongs inside that path's switch branch:

```hgl
fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    }

    let r = y - 1
    return r * 2
}
```

The generated true branch captures `x` and returns `x + 1`. The generated
false branch captures `y`, composes `r = y - 1`, and returns `r * 2`. Both
branches supply the enclosing function's `i64` result. The outer switch
receives `condition`, `x`, and `y` and provides that selected result.
The local `r` belongs to the false continuation; it is not an escaping
variable that must be declared before the original `if`.

This is not the never-ticking false branch of a value-producing `if`
expression without `else`: the source contains a continuation that supplies
the false path's result. Its inputs must be included when computing the
generated branch signature.

The lifetime consequences follow the native switch contract:

- The continuation is active only while its branch is selected. It must not
  remain independently active outside the switch.
- Stateful nodes and sinks composed in the continuation belong to that child
  graph and follow its stop/start and fresh-instance behavior.
- Computations composed before the `if` remain outside the child boundary.
  Capturing one does not move its producer into the continuation.
- A later condition change can select either branch. An early return does
  not permanently stop the graph or prevent future branch selection.

Graph composition still occurs at wiring time; the native switch owns runtime
branch activation. The return determines which path supplies the function's
output, rather than making the graph function execute on each tick.

Definite assignment considers only paths reaching the relevant use. A path
that returns earlier does not have to assign a variable used only in the
continuation, because that path never reaches the use. This does not permit
a read before assignment on a path that does reach it.

See [conditional-early-return.hgl](../../stdlib/examples/conditional-early-return.hgl)
for the design-corpus example. These semantics are agreed; compiler lowering
remains separate work.

## Outputless conditionals

A temporal conditional with no result or escaping variable uses the native
outputless switch path. It describes conditional sink wiring:

```hgl
fn observe(enabled: bool, value: f64) {
    if enabled {
        debug_print("enabled", value)
    }

    debug_print("always", value)
}
```

`debug_print` takes the label first and the time series second, matching the
[native operator contract](../../../include/hgraph/lib/std/operators/io.h).

When enabled, the `"enabled"` sink is wired in through the switch-selected
branch. The `"always"` sink is always wired in the enclosing graph, outside
the switch. When the false branch is selected, the conditional contributes no
sink; it does not affect the unconditional sink's connection.

The graph function describes these connections at wiring time. The sink nodes
perform printing during execution; the graph body does not become a per-tick
printing function. The switch owns the conditional child and its lifecycle
under the previously agreed native rules.

The true branch takes `value` as a temporal input, with `"enabled"` as its
fixed label. The selector is `enabled`. The false path has no conditional
work. This switch has no output: no returned time-series connection, bundle,
dummy value, or SIGNAL output is required. There are no escaping bindings to
remap. Native sink-switch behavior is covered in
[test_switch.cpp](../../../tests/cpp/test_switch.cpp).

Whether a conditional needs an output depends on its result and escaping
variables, not on the enclosing function's return annotation. An outputless
function can still compose a value-producing conditional and connect its
result to a sink.

The [conditional-sinks.hgl](../../stdlib/examples/conditional-sinks.hgl)
design example records this wiring model. HGL compiler support remains
separate work.

## Branch signatures

Lowering must compute the branch lambdas and their complete input signatures
in order to construct a valid switch signature. The branch bodies can refer
to different enclosing inputs, so the condition and the result type alone
are insufficient to describe the generated switch.

This requires identifying the external dependencies of each branch, retaining
their resolved types and wiring-time versus temporal roles, and describing how
the branch's inputs bind to the enclosing switch. The agreed REF boundaries
and SIGNAL input restrictions remain part of the relevant input contracts;
type compatibility must not erase those access semantics.

First determine the source return targets and the continuations reached by
each path. A generated lambda must not accidentally become the target of an
enclosing-function `return`. Capture analysis includes the continuation when
it belongs to that branch, not just the statements textually inside the
original `if` block.

The agreed derivation then has both input and output sides:

1. Resolve names by lexical binding and identify each branch's external
   dependencies, including dependencies used by nested expressions. Branch
   locals are not captures. An assignment target alone is not an incoming
   dependency; reading or forwarding its pre-conditional binding is. Include
   an implicit forwarding dependency when a branch leaves an already-bound
   escaping variable unchanged.
2. Separate wiring-time scalar captures from temporal input captures. Scalar
   captures specialize branch composition; they are not live switch input
   slots. Preserve resolved input types, REF boundaries, and SIGNAL contracts.
   Use reference capture for a generated pure forwarding branch; an ordinary
   temporal input is sufficient where processing is assured.
3. Form the shared temporal input slots from both branches, deduplicating by
   source identity, and map each branch's required inputs to those slots. Add
   the condition as the switch selector. A condition used inside a branch is
   also an explicit dependency of that branch; selecting it alone does not
   imply that the branch reads it.
4. Identify the used expression result and the predeclared variables whose
   branch-assigned bindings escape. Check definite assignment for their uses;
   reject a used variable when a path supplies neither an assignment nor an
   incoming binding. Count the expression result alongside the escaping
   variables: zero results require no output, one is returned directly, and
   several use a common generated bundle. For every result slot, preserve its
   resolved declared temporal schema as the common output schema. When that
   schema is non-`REF`, insert an explicit dereference at a forwarding branch's
   output boundary; when it is explicitly `ref<T>`, preserve the reference in
   the result. Never emit branch result bundles whose corresponding fields have
   different schemas.
5. For branches that rejoin, supply the expression result to its consumer and
   remap the enclosing variables from the switch output or its bundle fields,
   then continue composing the statements after the conditional. Both result
   forms may be present together. For an early-return conditional such as the
   example above, the non-returning branch includes the remaining function
   body and both branches supply the function's result.

For the single-result example, the true branch has temporal input `x` and
output `r: i64`; the false branch has temporal input `y` and the same output
contract. The outer switch receives `condition`, `x`, and `y`. Neither `r`
nor the literal constants are incoming temporal slots. For the two-result
example, the input mapping is unchanged and the common output is a bundle
with `r: i64` and `adjustment: i64` fields.

An input used only by the inactive branch must not prevent the selected
branch from running merely because that unused input is invalid. Likewise,
capturing an already-wired outer computation does not move its lifetime into
the conditional.

The existing native machinery distinguishes explicit callable arguments from
captured outer ports. In
[higher_order_impl.h](../../../include/hgraph/lib/std/operators/impl/higher_order_impl.h),
`compile_switch_branch` adds `CompiledSubGraph::captured_inputs` to shared outer
slots, deduplicates by source identity, and remaps child boundary inputs onto
those slots. The switch's complete temporal input schema is its selector plus
the resulting outer slots.

Explicit call arguments have a different constraint:
`bind_wired_fn_args` in
[wired_fn.h](../../../include/hgraph/types/wired_fn.h) validates each branch's
arity and parameter names. It does not silently discard arguments that one
branch does not accept. A language lowering must therefore provide consistent
explicit branch signatures or use the native capture-boundary mapping; it
cannot pass a union of arguments to differently shaped lambdas and assume the
binder filters them.

These lambdas and their boundary signatures are generated internally. This
agreement does not require new source-level closure syntax or implement
compiler lowering.

If the conditional exports no result or escaping variable, its generated
branch signatures are outputless. Preserve their sink wiring through the
native outputless switch; the absence of a result is not a reason to discard
the conditional's work or manufacture an output.

## Arrow precedent and native ownership

[Arrow control flow](../../../python/hgraph/arrow/_control_flow.py) constructs
true and false branch callables in `_IfThenOtherwise.__call__`, then calls
`hg.switch_`. The
[Arrow tests](../../../python/tests/ported/arrow/test_arrow.py) exercise
`if_then(...).otherwise(...)` and `if_(...).then(...).otherwise(...)`.

The lifetime contract is owned by the native switch, documented in
[Nested graphs](../../../docs/source/developer_guide/nested_graphs.rst)
and tested through public wiring in
[test_switch.cpp](../../../tests/cpp/test_switch.cpp). HGL should reuse that
contract rather than implement branch execution independently.

## Scope and next topics

The examples above establish predeclared escaping bindings, forwarding existing
bindings, definite assignment, early-return continuations, outputless
conditional wiring, mixed expression/assignment results, and subsequent
composition. [Iteration](iteration.md) records the subsequent agreement about
`for` in graph composition and node evaluation.
No new syntax or lifetime policy for `for`, explicit `switch`, `map`, `reduce`,
or `mesh` is introduced by these conditional agreements.

## Implementation status

At this change's baseline, both language backends reject a composition `if`
whose condition is a temporal port and direct authors to `if_then_else`.
The parser already accepts the conditional syntax. The new agreement changes
the target semantics; this documentation change does not remove that compiler
restriction. The parser also currently requires local declarations to have
initializers; supporting `var r: i64` is part of the newly agreed design. The
standard-library design corpus is kept outside the executable example glob
until the relevant compiler support exists.
