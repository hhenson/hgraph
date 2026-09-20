# One behaviour, several representations

Status: proposed design profiles extracted from #937. No map or column
replacement, byte cost or speed result is selected by this document.

A dictionary can store each child as a separate object or transpose fixed
child paths into columns. Both must preserve the same values, deltas and
temporal state at every level. Equal snapshots alone do not establish equal
behaviour. The [quote history](cases_collections.md) exposes the differences.

## Separate responsibilities

| Document part | Specifies |
|---|---|
| Behavioural chapter | Domain, state, observations, transitions, failures and progress |
| Case | Actions and exact observations at stated boundaries |
| Representation profile | Storage organization, eligible schemas, ownership and resource limits |
| Layout profile | Byte placement and object lifetime in a precisely bounded region |
| Realization record | How one representation implements every fact and action of one model |

The behavioural chapter never depends on a storage choice. A realization
depends on both, and runs the same cases without changing their expectations.
Current value, current-cycle delta and per-level timestamps are three logical
responsibilities; they do not require three allocations. A timestamp may
derive validity and modification without storing two extra flags.

## Two deliberately limited candidates

**NodeMap** admits exactly an outer `TSD<str,V>` whose V is a finite tree of
`TS<i64>`, nonempty TSBs of admitted children, or nested `TSD<str,V>` children.
Its key index owns child objects; each temporal level retains its own time.
Removal records retain logical keys independently of current membership.

**FlatPivot** admits exactly an outer `TSD<str,V>` whose V is a finite tree
of `TS<i64>` and nonempty TSBs of admitted children. Only the outer dictionary
has dynamic keys. A key index selects an occupied row; each fixed temporal
path has its own time column and each leaf its payload column. The root time
is stored once. Removed-key/cycle records survive row removal.

Both predicates reject every other type with the profile's UnsupportedShape
category. These are finite type trees, not recursive scalar declarations.

| Child shape under outer TSD | NodeMap | FlatPivot |
|---|---|---|
| bid/ask TSB of i64 series | eligible | eligible |
| fixed nested TSB of i64 series | eligible | eligible |
| nested dynamic TSD of i64 series | eligible | unsupported |
| fixed TSL of i64 series | unsupported | unsupported |
| REF of i64 series | unsupported | unsupported |

These cover the eight original eligibility checks plus the explicit map/TSL
boundary. Eligibility is not a conformance claim for all admitted trees.
Neither profile promises stable raw addresses, concurrent mutation or
allocation-failure behaviour. A caller requiring those needs a stronger
profile, even if its schema is eligible. A different column strategy could
support dynamic descendants; this candidate does not.

## Complete relation for the quote case

The relation must hold initially and after every admitted action, including
inspection. `never` means logically unpublished; a stale payload in storage
must not become observable merely because a slot is reused.

| Logical fact | NodeMap source | FlatPivot source |
|---|---|---|
| observation time | external evaluation context | external evaluation context |
| X present | X in live key index | X maps to an occupied row |
| bid | live X and published bid leaf, otherwise nil | occupied X row and published bid time, otherwise nil |
| ask | live X and published ask leaf, otherwise nil | occupied X row and published ask time, otherwise nil |
| root last time | root tracking | root tracking |
| row last time | live X bundle tracking, otherwise absent | occupied X bundle-time column, otherwise absent |
| bid last time | live bid tracking, otherwise absent | occupied X bid-time column, otherwise absent |
| ask last time | live ask tracking, otherwise absent | occupied X ask-time column, otherwise absent |
| X removed this cycle | removal record for X and this time | removal record for logical X and this time, independent of slot reuse |

Initial membership and changes are empty and all times are never. Begin
advances the external context; no sweep is required. Publication creates fresh
unpublished children if needed, writes the selected payload, and stamps leaf,
row and root, preserving sibling time. Removal records X, removes it from live
membership and stamps the root. Retired children follow TS-11's separate
observation lifetime. Inspection changes nothing. Updates are complete before
the case's observation points, with finite progress under its assumptions.

All other observations derive from these facts using the behavioural rules,
including TS-9's immediate-child validity. A whole-row timestamp cannot replace
leaf timestamps. Moving rows preserves all live observations; reusing one
initializes every child time before publication and cannot resurrect ask.

## Selection, composition and evidence

Check the full temporal schema, operations, ownership, lifetime and resource
requirements at planning time. Reject an unsuitable candidate before
construction; another eligible candidate can be selected only if its physical
properties satisfy the requested constraints. Record the chosen realization
and child choices in the immutable plan. Never discard temporal behaviour to
make a snapshot shape fit.

A hybrid map with packed children needs its own composition contract for
child identity, parent changes, coherent observations, removal and reuse.
Conforming children alone do not prove a conforming parent. Live migration
needs a separate protocol; changing a plan is not such a protocol.

| Realization | Model and cases | Storage | Evidence |
|---|---|---|---|
| AtomicCell | ATOMIC-RETENTION, ATOMIC-ZERO | [AtomicStorage](layout_example.md) | proposed; no native implementation |
| MapQuote | QUOTE-HISTORY | NodeMap | proposed; no native implementation |
| PivotQuote | QUOTE-HISTORY | FlatPivot | proposed; no native implementation |

An evidence record pins both document revisions, model/rule IDs, layout
arguments, realization, target traits/compiler, implementation revision,
adapter, cases and results. A behaviour change invalidates affected realization
evidence. A storage change reruns unchanged behavioural cases plus physical
checks. A relation change rechecks both sides. Broader eligibility needs new
models, composition rules and cases, not just a wider predicate. Performance
requires measurement with a stated boundary; it never follows from eligibility.
