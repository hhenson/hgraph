# ADR 0011: `cache` declarations

Status: accepted. Scalar cache declarations and aggregation are implemented.
Mixed recordable state/cache storage and scheduler checkpoint recovery remain
implementation gaps, not exceptions to the recovery contract.

## Decision

`cache name[: T] = init` declares reconstructible, function-level runtime data.
The initializer runs on every start. Initialization and any rebuilding must
finish before evaluation uses the cache. Given restored inputs and recordable
state, rebuilding must preserve values, validity, ticks, deltas and effects
([ADR 0008](0008-temporal-contracts-and-target-mappings.md)). A historical counter
or pending event cannot be made a cache merely to match existing native storage.

A single scalar cache lowers to `hgraph::State<T>`. Multiple scalar cache
variables lower to fields of a generated C++ struct in one `State<Struct>`.
Reads use the corresponding field and writes mutate it in place. The native
one-`State<>` constraint is a storage-slot constraint, not a one-variable limit.
All cache fields are constructed before `start` and initialized on every start.
The current native restriction against combining `State<>` and
`RecordableState<>` is separate; shared graph-IR validation diagnoses that
unsupported combination before backend dispatch. Non-scalar caches and generic
recordable state without an initializer remain future work.

## Scheduler recovery contract

Pending scheduled tasks must be recoverable. Authoritative recordable state
holds the raw schedule records: deadlines and their clock domain, stable task
identity and tie order, cancellation/replacement status, and progress needed to
preserve a finite schedule's remaining work. A heap, lookup table or other
ordering/index structure may be a cache of indices into that state. Process
pointers, native handles and container iterators are not durable records.

Recovery restores the records quietly, rebuilds derived indices, and re-arms the
native scheduler before any dependent evaluation. It must not schedule duplicate
tasks, reset finite progress, emit extra startup ticks, resurrect cancellations,
or change same-deadline ordering. Wall-clock and overdue-task behavior must follow
the native recovery contract rather than invent a second scheduler in HGL.

Required trace: after a three-tick schedule has emitted once, restoring with an
empty cache must leave exactly two emissions at the same pending deadlines as an
uninterrupted run. Also exercise cancelled/replaced tasks, equal deadlines,
repeated checkpoints, and wall-clock deadlines. Restarting a fresh graph twice
is not checkpoint-recovery evidence.

The current native and HGL `schedule` implementations keep their counter in
non-recordable storage, so full checkpoint recovery for these schedule
implementations is not implemented. Their ordinary-run parity tests establish
only that supported execution slice. This
is a correctness gap to repair through the native scheduler/state contract
before production migration or claims of recovery equivalence. It is not an
accepted exception allowing semantic history in a cache.
