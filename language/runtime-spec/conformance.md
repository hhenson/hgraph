# Contracts and conformance

Status: proposed method and worked contract. The examples are not executable
fixtures, and no implementation is certified by this document.

## Writing a contract that can be implemented

Start with the meaning in ordinary language, then give the exact cases. A
requirement must identify its scope rather than letting readers infer it from
an example. Use this structure for each completed contract:

| Field | Required content |
| --- | --- |
| Identity and status | Stable clause ID; proposed or accepted; semantic version when accepted. |
| Domain | Types, capabilities, phases, and feature profile to which it applies. |
| Inputs and prior state | Including initial, empty, invalid, and boundary states. |
| Preconditions | What makes an operation legal, and who checks it. |
| Transition | Resulting state, emitted changes/effects, and what remains unchanged. |
| Observations and ordering | Who may observe the result and at which point. |
| Failure | Failure identity, phase, post-failure state, and cleanup obligations. |
| Examples | Ordinary, boundary, and forbidden outcomes linked to the rule. |
| Evidence and gaps | Supporting design/tests and any decision preventing completeness. |

The prose and precise rule must describe the same behavior. If they disagree,
the draft has a defect; neither an example nor pseudocode silently overrides
the other. Do not use implementation code as the only definition of a rule.

## Worked contract: an atomic publication and its retained value

An atomic series retains its most recently published value. A later cycle with
no publication preserves that value and its validity, while `modified` becomes
false. Publication is an event; an equal payload need not imply the absence of
an event.

This deliberately small contract illustrates the required precision. It is not
the full `TS` contract or a definition of every public setter.

### Domain and notation

Use one owned output `e` of shape `TS[Integer]`. For this example `Integer`
has the finite value domain `{2, 7, 9}` and ordinary exact equality. Logical
times are integers in this example, strictly increasing between cycles. They
do not define the engine's clock representation, resolution, or epoch.

The legal actions are `BeginCycle(t)`, `Publish(v)`, and `Observe`. The first
action must be `BeginCycle`. A cycle admits at most one publication. `Observe`
can occur any number of times after the first `BeginCycle`. The owner is live
throughout; there is no invalidation, binding adaptation, failure, concurrency,
or write coalescing in this slice.

`Publish(v)` means an **admitted publication** of `v`, not an arbitrary call to
a native output setter. Whether a setter suppresses an equal write or batches
several writes is a separate contract still to be specified. No backend
adapter may suppress a `Publish` action supplied by this example's driver.

Use tagged alternatives, not host-language `None` or a magic timestamp:

```text
State(e) = (valid, current, modified, last, change)
current  = Absent | Present(Integer)
last     = Never | At(logical_time)
change   = NoChange | Publication(Integer)
initial  = (false, Absent, false, Never, NoChange)
```

`change` is the trace's event observation. It does not prescribe what an engine
returns from a raw scalar `delta_value` accessor in an unmodified cycle. That
accessor's legal domain and behavior must be specified separately.

### Transition rules

**HG-ATOM-001 — Initial state.** Before the first publication, the endpoint
MUST have the initial state above. It MUST NOT expose an invented default
integer as a valid temporal value. Native storage may already be constructed.

**HG-ATOM-002 — Begin a cycle.** `BeginCycle(t)` MUST set the observation time
to `t`, set `modified = false` and `change = NoChange`, and preserve `valid`,
`current`, and `last`. The action is legal only when `t` exceeds every earlier
cycle time. It resets the per-cycle publication allowance.

**HG-ATOM-003 — Publish.** In a begun cycle with no prior publication,
`Publish(v)` for `v` in the domain MUST produce:

```text
(true, Present(v), true, At(t), Publication(v))
```

This rule applies even if `current` was already `Present(v)`. It consumes the
cycle's publication allowance and changes no other endpoint.

**HG-ATOM-004 — Observe.** `Observe` MUST return the current state without
mutating it or consuming `change`. Observing the same endpoint twice without
an intervening action MUST return the same observation.

These are safety rules over all finite legal action sequences in the declared
domain. Each action must also finish in finitely many internal steps, assuming
ordinary computation and storage operations finish. An implementation that
never returns from a legal observation does not satisfy the contract.

Illegal actions, such as a second publication in the same cycle or a value
outside this domain, are outside this worked slice. They are **unspecified here**,
not declared legal and not required to throw a guessed exception. The complete
atomic contract must close those cases or assign them to a declared profile.

### Example trace HG-TRACE-001

Each row shows `Observe` immediately after its action. Rows 2 and 3 share a
cycle; rows 7 and 8 share a cycle. All other `BeginCycle` actions begin a new
cycle. `Never` is an abstract observation, not a native clock sentinel.

| Step | Action | valid | current | modified | last | change |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `BeginCycle(10)` | false | Absent | false | Never | NoChange |
| 2 | `Publish(7)` | true | Present(7) | true | At(10) | Publication(7) |
| 3 | `Observe` again | true | Present(7) | true | At(10) | Publication(7) |
| 4 | `BeginCycle(20)` | true | Present(7) | false | At(10) | NoChange |
| 5 | `BeginCycle(30)` | true | Present(7) | false | At(10) | NoChange |
| 6 | `Publish(7)` | true | Present(7) | true | At(30) | Publication(7) |
| 7 | `BeginCycle(40)` | true | Present(7) | false | At(30) | NoChange |
| 8 | `Publish(9)` | true | Present(9) | true | At(40) | Publication(9) |

Forbidden outcomes include clearing the value at step 4, marking step 4 as
modified, suppressing the admitted equal publication at step 6, and consuming
the publication when it is first observed. This trace is derived from the
clauses above; it is not a claim about an uninspected public setter.

## What an implementation must preserve

A specification defines allowed observable traces for a program, its initial
conditions, and an external event history. A trace can include observations
inside an evaluating node, not only output snapshots after a cycle finishes.
That matters for ordered handlers, incremental writes, and effects.

For a semantic version and declared feature profile:

```text
implementation's observable traces(program, inputs)
    are permitted by
specification's observable traces(program, inputs)
```

For deterministic cases the permitted result is unique. For nondeterministic
cases the specification must identify the allowed choices and their constraints.
An implementation can choose an allowed ordering only if the contract permits
that choice. Nondeterminism is not a substitute for an unanswered question.

Trace permission alone is insufficient: an implementation must also accept all
required well-formed programs in its profile, reject prohibited ones at the
specified boundary, and satisfy progress and termination obligations. An empty
trace from an engine that never runs must not pass by being a valid prefix.
Resource assumptions, cancellation, and nonterminating source streams need
explicit treatment when specifying those features.

Observations to cover include:

- logical times, values, validity, modifications, deltas, and binding changes;
- required effects and their ordering, such as sink actions and diagnostics;
- lifecycle events where admitted hooks can observe them;
- type identity, candidate selection, and specified rejection categories;
- specified recovery and post-failure behavior.

Addresses, allocation counts, internal container order, exact diagnostic prose,
and independent-node scheduling order are not automatically observations of
the core contract. A debugging, resource, ordering, or ABI profile can make
specific properties observable. Canonicalizing test output must never erase
an ordering or distinction the applicable contract exposes.

## Turning clauses into validation

Each fixture should carry its clause IDs, profile, logical type declarations,
graph/callable description, initial state, driven actions, observation points,
expected observations, and any permitted ordering alternatives. A language
adapter translates those semantic actions into an engine's public APIs. It
must report unsupported behavior rather than repair the engine's output.

Use four complementary kinds of evidence:

1. Finite examples and counterexamples covering each rule and boundary.
2. Property cases over explicitly defined domains, with reproducible seeds and
   reduction to a minimal failing trace.
3. Differential execution against current C++ and Python surfaces, to locate
   disagreements without assuming either result defines the intended contract.
4. An independent implementation of a completed slice, to expose assumptions
   that shared code and shared fixtures may conceal.

Finite testing does not prove universal conformance. A useful report names the
version, profile, clauses, cases, adapter, implementation revision, failures,
and untested requirements. Passing the scalar example cannot be reported as
"hgraph conformant". The future fixture format and runner remain open; this
document supplies their semantic requirements rather than claiming they exist.
