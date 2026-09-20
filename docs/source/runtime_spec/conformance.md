# Contracts and conformance

Status: proposed method; the cases are written expectations, not an executable
runner or a certification of either runtime.

An idle cycle keeps an atomic series' value but has no new delta. Publishing
the same value in a later cycle can still be an event. A test that compares
only final values cannot distinguish those histories. The unit of comparison
is therefore an **observable trace**, including observations inside a node's
evaluation where the contract makes them visible.

## Completing a chapter rule

Keep the concept, relationships, state, behaviour and numbered rules in the
chapter. Add a small case file when the transitions need more room. Each rule
that is ready to implement needs the following, in prose or a table:

| Item | What the reader must know |
|---|---|
| Identity and status | Stable rule ID; intended behaviour, proposal, or unresolved question |
| Domain | Types, capabilities, phases and feature limits |
| Prior state | Initial, empty, invalid and boundary cases |
| Preconditions | What is legal and who checks it |
| Transition | Changed state, retained state, notifications and effects |
| Observation | Who can observe it, and at what point |
| Failure | Error category, phase, surviving state/effects, cleanup and continuation |
| Progress | What must finish, under which resource and callback assumptions |
| Evidence | Source revision, relevant tests, what was actually run, and gaps |

A precondition excludes an action from a bounded example. It does **not**
specify an exception for that action in the runtime. A specified rejection,
by contrast, is a legal request with an expected error and post-state. An
error before effects and a failure after partial effects are different cases.
Unmentioned state remains unchanged only where the case says so; omitted
observations are unasserted, not implicitly empty.

Prose, diagrams, rules and examples must agree. A disagreement is a document
defect; an example does not silently override a rule. Abstract state does not
require one physical member per fact. Logical `never` and nil must remain
distinct from real timestamps and payload values such as zero.

## What to compare

For a named specification revision and feature profile, every implementation
trace must be allowed by the contract. Deterministic cases have one expected
result. An allowed choice must state its alternatives and constraints;
"nondeterministic" cannot conceal a missing decision.

Compare values, validity, modification, deltas, last-modified times, binding
changes, node admission, lifecycle, errors and sink effects. Also compare type
identity and wiring rejections when the profile includes wiring. Preserve
observable ordering. A graph with explicitly ordered nodes is not free to
reorder its sinks just because their values are independent.

Addresses, padding, allocation counts and diagnostic wording are outside the
behavioural contract unless a named physical, resource or diagnostic profile
requires them. A live view and a retained copy have different lifetimes even
when their contents initially match.

Trace comparison alone is insufficient: required programs must be accepted,
prohibited programs rejected at the stated boundary, and admitted operations
must make progress. An engine that never executes cannot pass because its
empty trace is a prefix of the expected one. Infinite sources, cancellation
and resource exhaustion need their own bounded obligations.

## Driving a case

A case records its rule IDs, domain, initial state, driven actions, exact
observation points and expected outcomes. Times in the cases are offsets in
microseconds from a legal run start unless explicitly described as an abstract
layout exercise. A new cycle always has a later time.

An **admitted publication** is an event the producer has chosen to publish.
It is not synonymous with every setter call: a library operator may suppress
equal results before publication. An adapter must identify the public path
that produces the specified event. It must report unsupported behaviour or a
mismatch, never manufacture timestamps, discard removals, or repair results.
Collection cases must drive real temporal children rather than assign expected
validity flags into implementation internals.

Use examples and forbidden outcomes, properties over explicit finite domains,
differential execution, and an independently implemented slice. Differential
agreement is evidence; a shared bug is still possible. Save the seed and full
failing trace, then reduce it without losing the mismatch.

## Cases and evidence

| Cases | Purpose |
|---|---|
| [Atomic](cases_atomic.md) | Initial state, idle retention, equal publication, repeated reads, zero payload/time |
| [Collections](cases_collections.md) | Immediate-child validity, set changes, dictionary membership and per-level time |
| [Lifecycle](cases_lifecycle.md) | Ordered activation, partial construction, borrow rejection and teardown |
| [Representation profiles](representations.md) and [layout example](layout_example.md) | Separate logical behaviour from eligibility, placement and ownership |

Report the spec and implementation revisions, profile, case IDs, adapter,
commands, results and untested clauses. Keep four claims separate: schema
eligibility, behavioural evidence, physical checks, and measured performance.
Finite examples do not prove universal conformance. These Markdown cases have
no runtime adapters yet; document consistency checks must not be reported as
executing hgraph.

## A bounded implementation handoff

A work card can replace the old `slice` declaration. Name the problem,
dependencies, deliverables, preserved rules, permitted files, exclusions,
validation and decisions still open. The [atomic layout](layout_example.md)
is an example proposal, not authorization to implement it.

For such a proof, deliver an isolated owner, immutable layout description and
public action driver; run the shared atomic cases, layout boundaries, lifetime
checks and sanitizers. Measure only the declared cell region. Before core
integration, name the real C++ and Python surfaces, expand the domain to their
semantics and run the repository acceptance gates. A passing isolated proof
does not select a production representation.
