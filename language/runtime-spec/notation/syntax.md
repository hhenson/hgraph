# Specification notation, revision 1

Status: proposed. `.hgspec` is reused as a file extension; this is a new
notation experiment, not the grammar proposed in #796. Nothing consumes these
files today. The definitions here are intended to make the examples precise
enough to review and implement a small prototype from them.

## Shape and names

Files begin with `spec <qualified-name> revision <integer>` and
`status proposed`. Declarations are `model`, `layout`, `scenario`, or `slice`.
Braces delimit blocks; a statement ends at a newline outside parentheses,
brackets, or braces. A `#` starts a line comment outside a quoted string.
Strings use double quotes with JSON escaping. Identifiers contain letters,
digits, underscores, and dots, and cannot start with a digit.

This outline describes only forms used by the examples:

```text
file        = header, status, { declaration }
declaration = model | layout | scenario | slice
model       = "model", name, "{", { model-item }, "}"
layout      = "layout", name, [ parameters ], "{", { layout-item }, "}"
scenario    = "scenario", name, "for", name, "{", { scenario-item }, "}"
slice       = "slice", name, "{", { slice-item }, "}"
```

Declarations are identified by file spec name and local declaration name.
Local names are unique across these example files; that is the lookup scope
for this revision. Ambiguous or unknown names are errors, not guessed imports.
Rule IDs are unique within the experiment and stay stable when wording changes.
Clause IDs may additionally contain hyphens; they occur in ID positions such
as `covers`, not as expression variable names.

Every declaration has `meaning "..."`. Models additionally state a
`scope "..."` and `contract proposed` or `contract established`. Established
labels apply to behavior, not to the maturity of this notation. Prose is part
of the review contract; it is never silently executed as host-language code.

## Values and expressions

The examples use booleans, signed decimal integers, strings, ordered lists,
and `none` / `some(value)`. Lists use brackets; `[]` is empty. Primitive types
are `bool`, `i64`, `u32`, `tick`, `text`, `option<T>`, and `list<T>`.
`tick` is an integer in `[0, 2^63 - 1]` **in these examples**; it does not
establish the engine's epoch or clock units. `i64` and `u32` have their usual
signed-64 and unsigned-32 value ranges, independently of storage representation.

Expressions use names, literals, field selection, `+`, `-`, comparisons,
`and`, `or`, `not`, and `if condition then a else b`. Precedence from strongest
to weakest is selection/calls, unary `not`, addition/subtraction, comparisons,
`and`, `or`, conditional. Comparisons cannot be chained. `and` and `or`
short-circuit; a conditional evaluates only its selected branch. Arithmetic
is mathematical integer arithmetic followed by the destination range check.
An overflowing model assignment is an invalid model transition, not wrapped
C++ arithmetic.

Equality is exact for the admitted primitives and structural for options and
ordered lists. `none` acquires its option type from context. `unwrap(x)` is
legal only for `some(v)` and returns `v`; an unguarded unwrap is a specification
error. There are no implicit conversions between tick, payload, and layout
units in a target implementation merely because the notation prints integers.
`tick(integer)` is an explicit checked conversion into the example tick domain.
`storage(n,a)` is defined in the layout section, not a model payload conversion.

`all(xs)` is true exactly when every boolean in `xs` is true; `all([])` is
true. Other helper functions must be defined in the model or below. There is
no arbitrary C++/Python expression escape.

## Models: stored facts, observations, and transitions

```text
state last: option<tick> = none
derive valid = last != none
invariant ATOM.COHERENCE: (current == none) == (last == none)

on publish(value: i64) {
    require now != none
    require last != now
    next current = some(value)
    next last = now
}
```

`state` declares an abstract fact and its initial value. It does not request a
physical member. `derive` defines an observation as a side-effect-free
expression, with no stored copy implied. Derived dependencies must be acyclic.
An `invariant ID: expression` must hold initially and after every completed
action, including failure post-states. `rule ID "..."` names an additional prose obligation, such as
the ownership meaning of an emitted event; it needs explicit validation evidence
and cannot be counted as checked by evaluating only the expressions.

`on` defines one action. All `require` statements restrict the admitted domain:
a scenario violating one is ill-formed and establishes no engine error policy.
This distinction lets a small example honestly exclude unrelated behavior.

`reject if expression with ErrorName` specifies an actual error in the
admitted domain. Rejection guards are checked in written order against prior
state; the first true guard wins. A rejected action returns that error, leaves
all state unchanged, and emits no events. Rejection guards precede all `next`
and `emit` statements. Error names are local to the model.

`fail ErrorName` at the end of an action instead returns an error **after**
the declared state changes and cleanup events. It is used for a constructor
failure with rollback, whose resulting state differs from its starting state.
This is distinct from pre-effect `reject`. Both forms require an explicit
expected error in a scenario.

`next field = expression` defines post-state, with expressions evaluated against
prior state and action arguments. Assignments are simultaneous, not imperative.
Unmentioned fields remain unchanged; each field can be assigned at most once.
`emit [Event, ...]` specifies an ordered observable log for a non-rejected
action, including cleanup on `fail`. It is not an implementation program. Its parameters
also use prior state. At most one `emit` occurs per action; omission means `[]`.
No callbacks or internal observation points occur between these abstract
assignments unless the action's event contract explicitly provides them.

`inspect` is a built-in action that changes nothing and emits `[]`. Every
admitted action must complete in finitely many implementation steps under its
stated resource and callback assumptions. Unspecified transitions are outside
the model's declared domain, not arbitrary permitted runtime behavior.

## Scenarios: explicit action and observation points

```text
scenario Retention for AtomicI64 {
    meaning "An idle cycle retains the prior publication."
    covers [HG-ATOM-002, HG-ATOM-003]
    observe [current, last, valid, modified]
    initial expect [none, none, false, false]
    step begin(10) expect [none, none, false, false]
    step publish(7) expect [some(7), some(10), true, true]
    step begin(20) expect [some(7), some(10), true, false]
}
```

Each scenario starts with a fresh model instance. `observe` fixes the ordered
projection used by every expected list. All listed observations are required;
an omitted observation is unasserted, not implicitly zero or unchanged.
`initial` observes the initial state before an action. `step` invokes one action
and observes its post-state before the next step. Action arguments are positional.

`step release() error BorrowLive expect [...]` requires that error and the
listed projected state. A rejection preserves state; `fail` has the declared
cleanup post-state. A step without `error` requires success. Where a
model emits events, every step includes `events [...]`; matching is exact in
both content and order. An empty list forbids any observed event at that step.
`covers` references clause/rule IDs; it is a coverage claim to audit, not proof
that those clauses are satisfied in every case.

Observation must not normalize away modifications, removals, error categories,
or order specified by the scenario. A future driver must distinguish admitted
publication from a setter that might suppress or batch writes. Driver mappings
are named evidence; they must not repair a nonconforming implementation.

## Layouts: physical placement with an explicit boundary

```text
layout AtomicStorage(payload: storage) {
    meaning "One payload followed by its publication stamp."
    target "C++ object storage; 8-bit bytes; 64-bit size_t"
    field value use payload at 0
    field stamp use storage(8, 8) at align_up(end(value), 8)
    alignment max(payload.align, 8)
    size align_up(end(stamp), alignment)
}
```

A `storage(size, align)` argument supplies **target-verified** positive byte
size and alignment. Alignment must be a power of two; size must be a multiple
of alignment, as required for a complete C++ object type in this example.
The C++ proof must assert these traits; a mismatching target rejects this
profile instead of pretending to share its layout.

Every `field` has an object-storage description and a byte offset. `end(f)`
is its offset plus its size. A layout can supply storage to another layout;
the nested layout's size/alignment apply. Field dependencies must be acyclic.
Declaration order is physical order in this revision: overlapping or reversed
field regions are rejected. `size` covers every field and tail padding;
`alignment` meets every field alignment. The allocation base must satisfy it.

All computations use mathematical integers first, then check against
`MAX_SIZE = 2^64 - 1` for the declared target. `align_up(n,a)` is the least
multiple of positive power-of-two `a` that is at least `n`. `max` returns the
greater argument. Invalid traits reject with `InvalidStorage`; an offset, end,
or total size above `MAX_SIZE` rejects with `LayoutOverflow`. These planning
errors occur before allocation or object construction, with no partial plan.
Other field overlap/alignment violations reject with `InvalidLayout`.

`case Name arguments [...] expect {field: offset, ..., size: n, alignment: a}`
checks a layout instance. `error ErrorName` instead of `expect` checks planning
rejection. Cases without arguments instantiate a parameterless layout.

`bind Model using PayloadType { ... }` relates a layout to abstract state for
that payload type; inner `require` statements constrain the supplied storage
traits. Matching byte size alone does not establish payload type identity. `decode`
equations say how stored bytes/objects represent model facts; they do not grant
permission to read a non-live C++ object. `external` names a model fact supplied
outside the region. A binding names construction and mutation obligations as
rules. A layout without such a binding specifies placement only, not behavior.

Physical obligations can include object construction/destruction order,
allocation ownership, borrowing, address stability, and allocation budgets.
Use `rule` for each with an ID and a concrete scope. Padding has no semantic
value. These examples are in-process layouts, not serialization or ABI promises.
Raw byte copying is not an implied move or copy operation for a live object.

## Slices: instructions that fit one implementation change

A `slice` records `status`, `meaning`, `requires`, `delivers`, `preserves`,
`touches`, `excludes`, `checks`, and `open`. These fields are lists of names or
quoted obligations, except `status` and `meaning`. It is a reviewable work
description, not executable build configuration. Paths in `touches` are relative
to the repository; example future paths need not exist yet.

The implementation must identify which rule each change satisfies and attach
the result of every listed check. `open` lists decisions it must not silently
make. A missing capability or conflict is reported at the affected slice,
not resolved by redesigning a neighboring subsystem. Each follow-on change
records dependencies and preserves the previous layer's reviewable diff.

Keep design status, implementation status, and verification status distinct.
The examples are all unimplemented. A proposed physical layout is not an
established runtime guarantee, and a passing finite trace is not proof of all
the named prose rules or of full hgraph conformance.
