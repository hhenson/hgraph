# Iteration in graph composition and node evaluation

Status: phase-dependent iteration and the independent-body boundary for
dynamic graph loops agreed, 2026-09-05; compiler implementation is separate.
Map/reduce lowering of loop-carried accumulators is a documented future
extension, explicitly unsupported in the initial graph-loop implementation.

## Iteration follows the containing phase

The existing `for ... in ... { ... }` syntax and the `keys`, `values`, and
`items` operations follow the containing function's phase. Their presence
alone does not classify a function as a runtime node. Node-only constructs
such as `when` establish runtime evaluation; a body without node-only
constructs describes graph composition.

| Context | Iteration receives | Body does |
| --- | --- | --- |
| Graph, over a supported wiring-time iterable | Scalar values known during wiring. | Composes nodes using those values. |
| Graph, over a fixed temporal structure | Child time-series connections. | Composes nodes connected to each child. |
| Graph, over a dynamic map or list, with independent iterations | Per-member time-series connections in a generated child graph. | Describes one child graph per key or index through native mapping. |
| Node evaluation | Current child views or scalar elements. | Processes values within that evaluation. |

The iterator must be meaningful in that phase. This agreement does not make
runtime payloads available during wiring or make runtime-only borrowed views
persist across evaluations. It also does not supply an iteration protocol for
every imported atomic type; native operations remain a separate topic.

## Fixed temporal structure at wiring time

```hgl
fn observe(samples: list<f64, 3>) {
    for sample in values(samples) {
        debug_print("sample", sample)
    }
}
```

The list's three positions are known while wiring. The loop wires three sinks,
one connected to each child. `sample` is a time-series connection, not its
current payload. No sample value needs to exist during wiring. Source value
ticks subsequently reach those sinks through their fixed connections; they do
not cause the graph function to iterate again.

The source is recorded in
[fixed-list-iteration.hgl](../../stdlib/examples/fixed-list-iteration.hgl)
as a design example, outside the executable compiler example corpus.

## Node-time traversal

Inside a node's evaluation, including a `when` handler, a loop traverses the
current child views or scalar elements. Ordinary readable child expressions
observe their payloads; `valid`, `modified`, and `last_modified` retain their
endpoint-metadata meaning. The loop does not wire new nodes.

The existing runtime traversal and predicate rules remain in
[Collection views and iteration](../user-guide/types-and-expressions.md#collection-views-and-iteration).
In particular, runtime iterators are evaluation-local borrowed views, predicate
filters are pure, and the supported `modified`, `added`, and `removed` ranges
follow the collection's native delta semantics. None of those rules grants
node code access below an explicit REF boundary.

## Dynamic structures: independent bodies first

Maps and non-fixed-size lists do not provide a fixed set of child connections
that a graph loop can enumerate during wiring. For independent bodies, the
initial design lowers the loop through the native map machinery: one child
graph per map key or dynamic-list index, with membership controlling child
lifetime. The graph function does not traverse current payloads on each tick.

```hgl
fn observe(book: map<str, f64>, offset: f64) {
    for value in values(book) {
        debug_print("adjusted", value + offset)
    }
}
```

The generated child accepts the current member's time-series connection and
the shared `offset` connection. Lexical capture analysis separates wiring-time
scalars from temporal inputs, preserving their types and REF access boundaries,
as with generated conditional branches. Here the body is outputless, so the
lowering uses a sink map. The source is recorded in
[dynamic-map-iteration.hgl](../../stdlib/examples/dynamic-map-iteration.hgl)
as a design example, not a runnable compiler test.

Map keys determine child identity. Dynamic lists use index identity, not the
identity of a stored value. Updating an existing member does not recreate its
child; membership removal or list truncation stops the affected children using
the native lifetime protocol. Surviving children retain their own node state.
The native ownership, binding, and scheduling contracts remain authoritative;
see [Nested graphs](../../../docs/source/developer_guide/nested_graphs.rst) and
the public-wiring coverage in [test_map.cpp](../../../tests/cpp/test_map.cpp).

Independent bodies may capture surrounding inputs and compose stateful nodes
or sinks within each child. Independence does not mean that every node must be
stateless or pure. It means that one iteration cannot depend on a binding
assigned by another iteration. No sequential side-effect order between children
is promised. In the initial dynamic-loop subset, assignments to enclosing
variables and loop-result construction are unsupported; local bindings within
an individual body remain available.

## Deferred option: map plus reduce

A dynamic graph loop with independent contributions and a loop-carried
accumulator can potentially lower to a map followed by a reduction. This is an
intended extension, not support in the initial implementation. For example,
the following records a candidate shape that is initially unsupported:

```hgl
fn total(samples: map<str, f64>, factor: f64) -> f64 {
    var result: f64 = 0.0
    for value in values(samples) {
        result = result + value * factor
    }
    return result
}
```

The independent contribution is `value * factor`; `result` carries the
combination between iterations. Recognizing this dependency pattern is not by
itself permission to reassociate or reorder the selected addition operation.
In particular, floating-point addition is not exactly associative.

The collection determines the future reduction strategy:

- **Map:** reduction should be supported without imposing an order. HGL has no
  sorted-map contract, so a loop must not imply key-sorted or insertion-order
  folding. The resolved combiner must permit the reduction's regrouping and
  reordering; the compiler cannot infer that permission from an operator's
  spelling alone.
- **List:** indices provide an order. Where that order affects the result,
  use the native linear, ordered-left-fold option of `reduce`. An associative
  tree reduction is only an option when the operation's contract permits it.
  The native selector is `is_associative=false`; this is a reference to the
  runtime API, not a newly agreed HGL call or policy syntax.

The future analysis must resolve input and combiner types, identify captures,
and check that each mapped contribution is independent of the accumulator.
Observing intermediate accumulator bindings, including through sinks, can
require prefix or recurrence semantics rather than one final reduction. Such
patterns are not covered by this map-plus-reduce option.

The result would describe the current collection, not accumulate all ticks
over time. Membership changes must remove obsolete contributions. Validity,
tick behaviour, and mapped-child state and effects are part of the contract,
not merely the final scalar result. Their exact reduction rules must be agreed
before this extension is enabled.

The incoming accumulator binding must also be preserved. Native associative
`reduce` uses `zero` for an empty collection, combines it with a singleton, and
does not include it for two or more live values. It is therefore not a general
loop initializer: changing `result` above to start at `10.0` cannot simply
become `zero=10.0`. Ordered reduction provides a true initial accumulator.
See the [native reduce contract](../../../include/hgraph/lib/std/operators/higher_order.h),
[ordered dynamic-list tests](../../../tests/cpp/test_reduce.cpp), and
[zero-semantics tests](../../../python/tests/test_reduce_zero_semantics.py).

Initially, the compiler should diagnose a dynamic graph loop requiring a
loop-carried reduction as unsupported. It must not silently choose a reduction
order or change the containing graph into a runtime node. This restriction
does not prohibit ordinary evaluation-time accumulation inside a node, and
does not remove the existing native map or reduce APIs.

## Remaining decisions

Loop result construction, the precise recognition and operator contracts for
deferred reductions, other cross-iteration dependencies, graph-phase
predicates, and loop exits remain to be worked through. So does the
disambiguation of a function whose only phase-sensitive operation is runtime
collection traversal: once iteration follows its containing phase, such a body
contains no existing construct that selects runtime evaluation. Whether this
needs an explicit phase marker or another rule is deliberately unresolved. No
new source spelling for that distinction, explicit `map`, `reduce`, or their
policies is introduced here.

## Implementation status

Earlier language documentation treated collection iterators as inherently
runtime-only and as function-classification triggers. The agreed target now
makes iteration phase-dependent, with an initial independent-body subset for
dynamic graph loops and deferred map/reduce accumulation. This documentation
change does not implement graph iteration, its unsupported-pattern diagnostics,
or the compiler's classifier change. The graph examples are design inputs,
not passing compiler tests.
