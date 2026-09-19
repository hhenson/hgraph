# ADR 0011: `cache` declarations

Status: accepted. Scalar cache declarations and aggregation are implemented.
Native pending scheduler recovery is implemented independently of user state.
Mixed recordable state/cache lowering and schedule-operator progress remain
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

Pending native `NodeScheduler` events have a dedicated checkpoint element,
independent of `State<>`, `RecordableState<>`, and HGL `cache` declarations.
At a completed checkpoint cut it stores the pending deadlines and tags. Restore
replaces bootstrap scheduler data after the normal node `start` hook, rebuilds
the tag lookup, and notifies the graph through its ordinary scheduling path.
An empty saved schedule clears bootstrap events too. Events at the restart time
are delivered in that cycle; restarting after a saved deadline is refused.
Component recovery currently supports simulation only.

`SingleShotScheduler` is best effort. Its schedules are neither stored nor
recovered; its normal `start` behaviour is unchanged.

Native and Python persistence tests cover pending deadlines, equal deadlines,
cancelled/replaced tags, empty schedules, and repeated checkpoints. Operator
progress is a separate contract: a finite schedule's emitted-tick counter is
semantic history and needs recordable state. Recovering its native alarm does
not recover that counter. The eventual operator test must show that a three-tick
schedule checkpointed after one emission resumes with exactly two remaining.

The current native and HGL `schedule` implementations keep their counter in
non-recordable storage, so full checkpoint recovery for these schedule
implementations is not implemented. Their ordinary-run parity tests establish
only that supported execution slice. This
is a correctness gap to repair through the operator recordable-state contract
before production migration or claims of recovery equivalence. It is not an
accepted exception allowing semantic history in a cache.
