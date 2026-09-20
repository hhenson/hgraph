# Representations

Status: proposed; neither storage candidate is implemented here.

A dictionary may own child objects or store fixed child paths in columns.
Both must give the same values, deltas and times at every level. The
[quote history](cases_collections.md) is one shared test of that promise.

Behaviour says what happens. A **representation** says how it is stored; a
**layout** fixes byte positions. A **realization** connects the two: where
each logical fact comes from, and how each action preserves the rules.
The behaviour and its tests do not depend on a storage choice.

## Candidates

**NodeMap** admits `TSD<str,V>`, where V is a finite tree of `TS<i64>`,
nonempty TSBs, or nested `TSD<str,V>`. Key indexes own children. Each temporal
level has its own time; removal records outlive current membership.

**FlatPivot** admits the same shapes except nested TSDs. Outer keys select
rows; fixed paths have time columns and leaves have payload columns. Root
time is stored once. Removed keys are recorded independently of row slots.

Both reject all other shapes as UnsupportedShape. Neither promises stable
raw addresses, concurrent mutation or allocation-failure handling.

| Child shape under outer TSD | NodeMap | FlatPivot |
|---|---|---|
| bid/ask TSB of i64 series | eligible | eligible |
| fixed nested TSB of i64 series | eligible | eligible |
| nested dynamic TSD of i64 series | eligible | unsupported |
| fixed TSL of i64 series | unsupported | unsupported |
| REF of i64 series | unsupported | unsupported |

## The quote realization

The relation holds initially and after every action, including inspection.

| Logical fact | NodeMap source | FlatPivot source |
|---|---|---|
| observation time | external evaluation context | external evaluation context |
| X present | X in live key index | X maps to an occupied row |
| current bid | live X and published bid leaf, otherwise nil | occupied X row and published bid time, otherwise nil |
| current ask | live X and published ask leaf, otherwise nil | occupied X row and published ask time, otherwise nil |
| root last time | root tracking | root tracking |
| row last time | live X bundle tracking, otherwise absent | occupied X bundle-time column, otherwise absent |
| bid last time | live bid tracking, otherwise absent | occupied X bid-time column, otherwise absent |
| ask last time | live ask tracking, otherwise absent | occupied X ask-time column, otherwise absent |
| X removed this cycle | removal record for X and this time | removal record for logical X and this time, independent of slot reuse |
| removed child: identity, bid/ask values and row/leaf times | retained child outside the live key index, keyed by X and removal cycle | retired row or saved row image, including identity, payloads and time columns, keyed by X and removal cycle |

Initially there are no members or changes; times are `never`. Begin advances
the external clock. Publication creates unpublished children as needed, writes
the leaf, and stamps leaf, row and root while retaining sibling time. Removal
retains the child before removing live membership, records the key and stamps
the root. The removed view reads the retained child, not the live index.
Reclamation or row reuse must preserve it until the cycle ends (TS-11);
reinsertion in that cycle restores the same child. Inspection changes nothing.

Updates finish before observation. Flags follow the behavioural rules,
including TS-9. Moving a row preserves its values and times; reuse initializes
every child time. One row timestamp cannot replace the leaf timestamps.

## Selection and evidence

Planning checks the whole temporal schema, operations, lifetimes and resource
requirements. Record the chosen realization and child choices in the immutable
plan; reject if none fits. Eligibility alone proves neither behaviour nor cost.

Composition needs rules for child identity, parent changes, coherent reads,
removal and reuse. Live migration needs its own protocol.

| Realization | Model and cases | Storage | Evidence |
|---|---|---|---|
| AtomicCell | ATOMIC-RETENTION, ATOMIC-ZERO | [AtomicStorage](layout_example.md) | proposed; no native implementation |
| MapQuote | QUOTE-HISTORY | NodeMap | proposed; no native implementation |
| PivotQuote | QUOTE-HISTORY | FlatPivot | proposed; no native implementation |

Record model/storage revisions, rules, layout arguments, target traits,
implementation, adapter and results. A behaviour change invalidates affected
evidence. A storage change reruns unchanged behavioural cases and physical
checks; a relation change rechecks both. Broader support needs new cases and
composition rules. Measure performance within an explicit boundary.
