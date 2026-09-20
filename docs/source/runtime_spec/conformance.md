# Conformance

Status: proposed. The cases are written expectations; no runtime runner exists yet.

An idle cycle keeps a value but has no delta. Publishing the same value later
can still be a tick. Conformance compares the **trace**, not just the final
value: what a node sees, what runs, and what effects occur, in order.

## A testable rule

Give each rule an ID. Say where it applies, its starting state, what changes,
what stays, when the result can be read, and what happens on failure. Include
empty, invalid and boundary cases. Prose, diagrams and cases must agree.

A precondition limits a case; it does not invent an error outside it. A
rejection has a specified error and post-state. Failure after partial work
also needs cleanup and surviving effects. Omitted observations are untested.

Compare values, validity, modification, deltas, times, bindings, admission,
lifecycle, errors and effects. Include type identity and wiring errors where
specified. Preserve observable order. Addresses, layout and cost belong to a
separate physical contract. Abstract state need not be stored field for field.

An implementation must accept the required programs, reject prohibited ones,
and complete admitted actions under the stated resource assumptions. Where
several outcomes are allowed, list them. An unanswered question is not a
licence for arbitrary behaviour.

## Cases

- [Atomic](cases_atomic.md): first tick, idle cycles, equal publications and repeated reads.
- [Collections](cases_collections.md): validity, membership, deltas and per-level time.
- [Lifecycle](cases_lifecycle.md): activation, construction failure and teardown.
- [Representations](representations.md) and [layout](layout_example.md): physical contracts.

Each case names its rules, limits, initial state, actions and observation
points. Times are microsecond offsets from a legal run start unless stated
otherwise; cycles advance strictly. Nil, `never` and zero are distinct.

An **admitted publication** is an event the producer has chosen to publish.
An operator may suppress equal results before that point. The test adapter
must identify the public path that produces the event, drive real endpoints,
and report mismatches without repairing observations.

Use examples, properties over stated domains, differential tests and an
independent implementation. Keep failing seeds and traces. Agreement between
two implementations is evidence, not proof.

Record revisions, rules, cases, adapter, commands, results and untested work.
Keep eligibility, behaviour, physical checks and measured performance separate.
Document checks do not establish runtime conformance.

## An implementation card

Name dependencies, deliverables, preserved rules, files, exclusions, checks
and open decisions. For the atomic proof: an isolated owner and action driver;
shared cases; layout and lifetime checks; sanitizers; measured cell costs.
Core integration then needs public C++/Python coverage and the full acceptance
gates. The [layout example](layout_example.md) remains a proposal.
