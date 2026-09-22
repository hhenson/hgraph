Fixed collections: validation cases
==================================

Status: 36 cases compared with Python and C++; three decisions remain.
See the [comparison report](validation/fixed/README.md).
The prototype slice covers fixed-size TSL and TSB. Growing TSL is separate.

A field or position is a time-series, including when it is a collection.
Peering belongs to each input: an assembled parent can contain a peered child
and an assembled child. A whole-output REF can change to child references;
the input must follow that change without changing its declared shape.

The initial expectations are in [reasoned.json](validation/fixed/reasoned.json).
Each case observes value, delta, validity, modification and time at every
level, plus collection peering. Times are engine cycles starting at zero.
An idle delta is nil; an invalid scalar reads nil. Reading twice must agree.

| Cases | Required observation |
|---|---|
| TSL / TSB, owned / assembled | Staggered child ticks, idle cycles, one child invalidated, all children invalidated, revalidation, whole invalidation |
| All four two-level TSL/TSB combinations, owned / assembled | A grandchild tick reaches both ancestors; root `all_valid` can be true while both children's `all_valid` are false |
| TSL of TSB / TSB of TSL, mixed | Assembled root with one peered child and one assembled child |
| TSL / TSB, whole REF | Empty, bind A, repeat A, A ticks, bind B, B ticks, empty, bind A; sampling leaves producer times unchanged |
| TSL / TSB, child REF | Whole A, then A.left + B.right, then empty + B.right, then whole A; unchanged child bindings do not resample |
| TSL / TSB, passive | Child ticks between polls cause no evaluation; later polls read retained state with no stale delta |

An assembled input derives its observed time from its current children.
Notification alone is not a tick. The original notification-time expectation
and its correction remain separate in the evidence.

The additional cases cover nested REF targets, heterogeneous bundle fields,
fixed collections inside TSD and TSD inside fixed collections, and aggregate
inputs across switch/map boundaries with timers and fresh child state.

Whole invalidation, changing whole bindings to child bindings, and sampling
invalid nested targets require the decisions in the report before implementation.
