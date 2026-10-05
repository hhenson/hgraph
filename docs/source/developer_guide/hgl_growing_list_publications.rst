HGL growing-list publications
=============================

``list<S>`` and ``list<S, unbounded>`` preserve sparse child publications and
complete tail removals. The delta uses ``items`` and ``remove`` with constant
nonnegative indices. The native removed/modified bundle representation and
its owning HGL wrapper remain distinct from a fixed-list delta and a complete
``atomic<list<T>>`` snapshot.

Eval preflights each input trace from length zero. New positions must append
contiguously, removals must cover a complete live tail, and a removal cannot
reintroduce positions in the same publication. Recursive child state survives
updates and is discarded on removal. A later append receives fresh child
state. Empty structural publications remain outside the admitted profile;
removing the complete live tail is a present publication with empty held data.

Both backends construct the same owning delta, and generated pass-through
applies it to its own output. Replay and recording preserve removals and child
deltas across growth, shrink, capture, and teardown. The normative contract is
``growing-list-publications.md`` in the pinned specification.
