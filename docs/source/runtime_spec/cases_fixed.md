Fixed collections: validation cases
==================================

Status: 36 cases compared with Python and C++; user rulings recorded.
See the [comparison report](validation/fixed/README.md).
The prototype slice covers fixed-size TSL and TSB. Growing TSL is separate.

A field or position is a time-series, including when it is a collection.
Peering belongs to each input: an assembled parent can contain a peered child
and an assembled child. A whole-output REF can change to child references;
the input must follow that change without changing its declared shape.

The initial expectations are in [reasoned.json](validation/fixed/reasoned.json).
Snapshot cases observe value, delta, validity, modification and time through
the collection levels. Fixed input snapshots include peering; TSD-owned and
retired subtrees omit it. The four graph-boundary cases record scalar output
ticks and lifecycle events. Duplicate-read checks cover the ordinary collection
observers, not the flat REF or dictionary observers. Times are engine cycles
starting at zero. An idle delta is nil; an invalid scalar reads nil.

| Cases | Required observation |
|---|---|
| TSL / TSB, owned / assembled | Staggered child ticks, idle cycles, one child invalidated, all children invalidated, revalidation, whole invalidation |
| All four two-level TSL/TSB combinations, owned / assembled | A grandchild tick reaches both ancestors; root `all_valid` can be true while both children's `all_valid` are false |
| TSL of TSB / TSB of TSL, mixed | Assembled root with one peered child and one assembled child |
| TSL / TSB, whole REF | Empty, bind A, repeat A, A ticks, bind B, B ticks, empty, bind A; sampling leaves producer times unchanged |
| TSL / TSB, child REF | Whole A, then A.left + B.right, then empty + B.right, then whole A; unchanged child bindings do not resample |
| TSL / TSB, passive | Child ticks between polls cause no evaluation; later polls read retained state with no stale delta |

An assembled input may cache state driven by child events; it need not scan
children on each read. It replaces an assembly node, usually for one consumer.
A child invalidation records the current time while the structure stays valid;
losing its last valid child resets every local observation time to *never*.

The additional cases cover nested REF targets, heterogeneous bundle fields,
fixed collections inside TSD and TSD inside fixed collections, and aggregate
inputs across switch/map boundaries with timers and fresh child state.

Whole invalidation clears every level (TS-26). Whole-to-child rebinding preserves
unchanged targets (TS-25). Invalid targets contribute no sample time or delta
(TS-14). Valid TSB values retain all fields, including nil children (TS-24);
equal REF designations cause no additional tick (TS-16).
