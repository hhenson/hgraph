# Dynamic-case validation

Status: completed 2026-09-21, with accepted variations and one user ruling.
The owner reviewed the variation reports and accepted all proposed choices
on 2026-09-21. These are the implementation baseline; the deviations remain
recorded for corrections to the reference implementations.
This validates expectations, not conformance of the whole Rust prototype.

Fourteen bounded cases ran three times each on Python hgraph 0.5.41 and the
current installed C++ runtime. Every result was repeatable: 84 isolated runs,
including the Python expiry error. Of 243 asserted observations, 198 match
both runtimes, 44 match reasoning and one runtime, and one required the user's
expiry ruling. [Evidence and replay](validation/README.md) retain the inputs,
expectations, observations, adapters, revisions and artifact hashes.

## Coverage

| Cases | What was validated |
|---|---|
| `membership` | Invalid children, publication, invalidation, removal, key-set ticks, immediate-child all_valid |
| `quote` | Per-field deltas and times, admitted equal publication, repeated reads, removed-child contents, fresh reinsertion |
| `restore` | Same-cycle removal and restoration, retained value and time, cancelled membership delta |
| `scalar_ref` | Empty REF, sampling, target ticks versus designation ticks, passive following, unbinding |
| `dictionary_ref` | Overlapping-key rebind, full sampled child deltas, keyed withdrawal |
| `timer`, `cancel_timer`, `nested_timer` | Child-only deadlines, cancellation on removal, another owner level |
| `mapped_ref`, `nested_mapped_ref` | Keyed child creation, reference routing, instance state, replaced timers and teardown together |
| `parallel_children` | Independent sibling deadlines; input and timer in one evaluation |
| `child_failure`, `child_start_failure` | Failure propagation and exactly-once stop of started children |
| `expired_ref` | Saved reference after removal and new insertion; contract ruled, both implementations vary |

[Reference cases](cases_references.md) and [nested cases](cases_nested.md)
give the derivations. The machine-readable assertion paths identify precisely
what each case checks. An unasserted observation is retained evidence, not a
validated expectation.

## Variations for review

R is the reasoned expectation. C++ below means the Python-facing C++ runtime
unless native confirmation is stated. These are review reports; no runtime
fix or issue publication is included.

| ID | Case and observation | Accepted expectation | Variation |
|---|---|---|---|
| DV-01 | membership, added keys at 1 and 2; TS-19 | R + Python: X is added on insertion, not first child publication | C++ reports no added key at 1 and X at 2. Native endpoint probe confirms |
| DV-02 | membership/quote, key-set modified on value changes; TS-19 | R + Python: only membership changes tick the key set | C++ Python accessor reports key-set modified on child writes/invalidation. Native output key-set tracking matches R, so inspect the projection/bridge |
| DV-03 | membership, all_valid at 1 and 3; TS-9 | R + current C++: false with an invalid live child | Python returns true. An older cached C++ wheel did too; it was excluded from final evidence |
| DV-04 | membership, child invalidation at 3; TS-7 | R + C++: the parent reads modified | Python parent remains unmodified with its old time |
| DV-05 | dictionary_ref, rebind at 2 and 4; TS-14 | R + Python: full new child values as delta; sampled child times | C++ reports modified on the parent but nil delta, unsampled child times and no retained removed item. Native rebind probe also has no removed item |
| DV-06 | dictionary_ref, empty REF at 3; TS-15 | R + Python: delta removes Y and Z | C++ reports the removed keys but returns nil delta and no removed items |
| DV-07 | dictionary_ref, validity at 3; TS-1/TS-15 | R + C++: invalid with an input-side withdrawal event | Python remains valid during withdrawal. Its last time is nevertheless `never` and all_valid is false |
| DV-08 | scalar_ref, last time after unbind at 5; TS-1 | R + Python: `never` | C++ retains the old last time, although valid and modified are false |
| DV-09 | restore, same-cycle restoration at 1; TS-11 | R + C++: old value 7 and child time 0, no added key | Python returns an invalid replacement child and reports X added |
| DV-10 | expired_ref, saved reference at 3; TS-23 | User ruling: old child is absent from the next cycle and never retargets new X | Python raises `AttributeError`; C++ still reads 7 at 3 and expires at the next insertion at 4 |

The supplementary native explicit-unbind probe also reports no withdrawal
modification or removed keys. That is a distinct entry point from REF
withdrawal and needs its own C++ review; it does not override DV-06's expected
graph-level removal delta.

Acceptance is per observation. For example, Python supports the withdrawal
delta while C++ supports invalidity; neither complete withdrawal trace passes.
Python's stronger testing history guides ambiguous interpretations, but does
not erase the revised rules or the user's explicit ruling.

## Specification sufficiency

The review added sampled input timestamps and child sampling, passive-input
observations, withdrawal deltas while invalid, same-cycle restored value/time,
and next-cycle reference expiry to the Time-series chapter. The new cases
state initial state, actions, observation phase, exact ticks and failure hooks.
The conformance chapter now records the three-way acceptance procedure.

Remaining limits are explicit: whole-graph output retention after stop,
allocator slot reuse, bounded memory/subscriptions under long churn, rank-invalid
references, compound REF values, multi-child partial-start rollback and captured
nested errors still need focused cases. The current runs do not establish
those properties. State/cache recovery, performance and native-only graph
parity are also outside this evidence; the native supplement checks endpoints.
