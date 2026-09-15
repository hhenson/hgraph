# Extraction and decisions

Status: design input and outstanding decisions, not an accepted full contract.

## Sources and authority

The original exploration is
[PR #796: prototype backend-neutral runtime contracts](https://github.com/hhenson/hgraph/pull/796),
reviewed at head `72d9ace16472ccfe43dcc45b2ad1657626469249`. It was an open
draft at extraction time. Its `.hgspec` files explicitly describe provisional
syntax with no parser or generator.

Current HGL documents and selected native source/tests were also inspected at
base `22668c5d4`. These references record provenance, not a declaration that
every statement in those documents is current or every test was run. The new
foundation must distinguish intended semantics, observed implementation
behavior, and unresolved disagreement.

## What to retain from #796

| Original material | Concept to retain | Change for the fresh specification |
| --- | --- | --- |
| `values.hgspec` | Values, schemas, nominal/structural identity, operations, capability requirements | Define logical types separately from realization flags and storage plans; enumerate the actual type relations. |
| `time-series.hgspec` | Temporal shapes, validity, modification, last modification, deltas, observation lifetimes | Define transitions per shape; distinguish invalid, absent, null, and unchanged; separate input access contracts. |
| `execution.hgspec` | Nodes, graphs, input policies, scheduling, lifecycle, capabilities | Describe evaluation and publication ordering, failure paths, and dynamic graph behavior; distinguish default and empty policies. |
| `operators.hgspec` | Nominal callable contracts, candidate substitution/ranking, ambiguity, transactional registration | Specify matching and resolution rules completely enough to implement independently; a shared C++ service is not the specification. |
| Provider lifecycle | Logical removal distinct from physical unloading | Retain the safety obligation; define dependency and retention behavior without requiring one lease representation. |
| Ownership vocabulary | Ownership, borrowing, mutation, sharing, and lifetime boundaries | Specify aliasing and escape rules in prose and examples before choosing generated language types. |
| `conformance.hgspec` | Reusable laws and finite observable traces | Give every action, observation point, and allowed outcome an exact meaning; link fixtures to clauses. |
| `backends.md` | Several representations can realize the same semantics | Keep mappings and binary interoperability in explicit profiles. |

The original split between runtime meaning and HGL source remains useful.
Its characterization of HGL as only a graph DSL is superseded in current HGL
design by temporal programming with value functions, state, and native helpers.

## What not to carry over as a commitment

The [notation experiment](notation/README.md) now proposes a new source form
and explicit C++ layout contracts. The distinctions below explain what is not
inherited automatically from #796; they do not rule out making a scoped,
reviewed representation choice for a C++ simplification.

[Notation revision 2](notation/representations.md) makes each realization an
independent relationship between a behavioral model and a storage profile. Its
TSD map/pivot examples distinguish storage eligibility from behavioral evidence
and preserve current, delta, and temporal observations. They are proposed finite
fixtures, not an extracted claim that either representation is implemented or
that the full TSD behavior has now been specified.

No `.hgspec` grammar, generated façade, parser, backend language pair, fixed
integer discriminant, universal ops table, or storage-plan API is selected by
this restart. These may become useful artifacts after their semantic purpose
is established. In particular:

- `canonical(shape, capabilities)` conceals the identity algorithm and mixes
  capabilities such as equality with target traits such as trivial copying.
- `more_specific` and `rank` name unresolved operator rules rather than define
  them. Declaration-order independence alone is insufficient.
- A law named `modified_implies_time_advance` only checked nondecreasing
  `last_modified`; it did not define cycles, first modification, or time advance.
- `next_cycle` did not define elapsed logical time, reset, or publication phases.
- The activation trace required `[a, b, sum, observe]` without establishing why
  independent sources `a` and `b` must have that particular order.
- Treating `signal` as a general output/delta constructor is not the current
  HGL input-only observation contract.
- `hot` must not automatically mean "no allocation" for every engine and
  operation. Resource guarantees need explicit scope and measurable bounds.
- A lifecycle enum does not specify failed construction, failed start, repeated
  calls, evaluation errors, cleanup errors, or partial effects.

## Current evidence worth preserving

| Evidence | Implication for the specification |
| --- | --- |
| [Type registry tests](../../tests/cpp/test_type_registry.cpp), especially nominal bundles and `a storage category takes no part in type resolution` | Logical identity and representation must remain distinguishable. |
| [Time-series guide](../../docs/source/user_guide/concepts/time_series_types.rst) | A retained value and a current-cycle change are different observations. |
| [Static-node tests](../../tests/cpp/test_static_node.cpp), especially nested-list and TSD `all-valid` cases | Exercise the established single-level `all_valid` contract. |
| [Node evaluation](../../src/hgraph/runtime/node.cpp), `evaluate_impl` | Scheduling supplies activation; evaluation checks readiness and guards rather than universally polling input modification. |
| [Scheduler tests](../../tests/cpp/test_node_scheduler.cpp) | Current node-scheduler rules distinguish startup from evaluation, tagged replacement from accumulation, and past-time requests from future events. |
| [Reference tests](../../tests/cpp/test_time_series_reference.cpp) and [linking contract](../../docs/source/developer_guide/data_structures/linking_strategies.rst) | Scalar/fixed unbind can be silent; keyed structural unbind can reconcile membership. One universal "unbind ticks" rule would lose behavior. |
| [HGL language model](../docs/design/language-model.md), canonical temporal types and runtime functions | Temporalization, ordered handlers, default policies, and output writes need explicit runtime clause ownership. |
| [HGL type extensions](../docs/design/type-extensions.md) | REF type compatibility, opaque node access, and metadata-only signal observation are different contracts. |
| [ADR 0008](../docs/design/decisions/0008-temporal-contracts-and-target-mappings.md) | Value functions, reconstructible cache, semantic state, and target realization are separate concerns. |

The scheduler evidence is deliberately scoped: ignoring a past request in the
node-scheduler API does not imply every internal graph scheduling API ignores
past time. Similarly, a reference becoming empty and the resulting observation
at a dereferenced consumer are separate events.

## Established validity contract

The single-level meaning of `all_valid` was already settled and documented in
the [time-series guide](../../docs/source/user_guide/concepts/time_series_types.rst).
For TSL/TSB it checks endpoint validity and immediate children's `valid`; it
does not ask those children for `all_valid`. For TS/TSD/TSS it is the same as
`valid`. Nested-list and TSD native tests preserve that behavior.

Descriptions of this operation as recursive in HGL and native authoring
documents are stale wording, not an unresolved semantic choice. The experiment
records the correction here while keeping those guides outside its change
scope. The extracted contract is
**HG-VALID-001** in the [core model](core-model.md).

## Open decisions

Each entry needs a clause, examples, and an explicit compatibility assessment.
The table records missing specifications, not approval requests or instructions
to change implementation behavior immediately.

| ID | Question to settle | Why it matters |
| --- | --- | --- |
| OPEN-01 | What is the complete logical type algebra: primitive domains, nominal keys, aliases, recursion, variance, nullability, conversions, constraints, and portable identity encoding? | An alternative type checker cannot rely on C++ metadata addresses or native names. |
| OPEN-03 | What are initial validity/last-modified observations, publication boundaries, equal-write policies, multiple-write coalescing, and delta access outside a modified cycle? | The worked atomic slice distinguishes publication from setters and does not settle the whole API. |
| OPEN-04 | How are cycles, logical timestamps, source admission, dependency order, timer ties, re-evaluation, readiness failure, feedback, and termination related? | "Run the graph in order" does not determine a trace or guarantee progress. |
| OPEN-05 | What are the exact formation, compatibility, adaptation, and binding/unbinding rules for REF and metadata-only inputs? | Binding change, target change, and child removal can produce different observations. |
| OPEN-06 | What are the delta and identity rules for collections/windows, including same-cycle remove/reinsert, stable children, eviction boundaries, and scheduled expiry? | Equal final snapshots can hide different histories, activation, or resource lifetime. |
| OPEN-07 | What is the full operator resolution procedure, including role eligibility, defaults, constraints, substitutions, ranking, ambiguity, and diagnostics? | Another implementation must compute a result without calling the current engine's resolver. |
| OPEN-08 | What happens on failures in construction, start, evaluation, stop, and external admission? What effects/state survive? | Correct results on successful ticks do not establish lifecycle or failure compatibility. |
| OPEN-09 | What exactly is recorded/restored: node state, timers, connections, child graphs, source positions, and effects? | Restoring values alone does not necessarily restore future behavior; no implicit exactly-once guarantee is justified. |
| OPEN-10 | How do map, switch, reduce, mesh, services, and feedback own child membership, scheduling, cancellation, and teardown? | Higher-order computations are part of runtime semantics, not merely HGL lowering details. |
| OPEN-11 | What are the numerical/string/library contracts, including overflow, NaN, ordering, Unicode, and algebraic laws? | Host-language defaults can disagree even when topology and types match. |
| OPEN-12 | What are the profile boundaries for native values, provider lifetime, resource guarantees, concurrency/backpressure, serialization, and ABI? | Semantic equivalence and binary compatibility are different claims. |

Some decisions already have partial answers in implementation, RFCs, or HGL
design. Resolve those from evidence first; do not treat this list as permission
to discard established behavior. Where evidence conflicts, record the chosen
behavior and migration consequences explicitly.

## Order of work

The first complete slice should specify an atomic series and a small graph
that consumes it: scalar type/identity, initial state, publication/no-publication,
active/passive inputs, readiness, source-to-consumer ordering, and sink observation.
That reaches a useful executable behavior without requiring a whole new DSL.

Then extend in dependency order:

1. Structural values and temporal collections, with validity and delta rules.
2. Reference binding and observation, including removal and rebinding.
3. Wiring-time resolution, generics, operator identity, and failure diagnostics.
4. State, timers, complete lifecycle/failure behavior, and record/replay.
5. Dynamic graphs, external events, services, and provider interoperability.
6. Standard-library contracts, HGL mappings, and target/profile completeness.

Each slice should add explanation, complete rules, counterexamples, and
portable conformance cases together. Runtime and HGL clauses must be reconciled
as each slice lands; the final step completes that coverage rather than deferring
the language connection until the end. A second implementation of the first
slice can validate whether the specification stands on its own, before a
generator or a larger implementation is attempted.
