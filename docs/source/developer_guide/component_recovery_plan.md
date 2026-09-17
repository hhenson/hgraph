# Component recovery implementation plan

This change implements a deliberately restricted first checkpoint capability
under RFC 0023, using the ownership boundary in RFC 0025. It targets deterministic
day-by-day simulation components whose external inputs are explicit ports.

## Contract

- A component checkpoint contains source baselines and boundary alias clocks, ordinary outputs,
  hidden recordable-state and error endpoints, supported dynamic children,
  internal reference locators, and synthetic adapter recipes and clocks.
- Each external input owns a dedicated direct pull-source endpoint baseline.
  Restore imports this baseline before source start so collection deltas retain
  their meaning. The caller supplies future-only events; source cursor and
  scheduling state remain outside the image. Projected/computed ingress and
  sources shared with other consumers are refused. Boundary schemas contain
  values only, recursively; references neither enter nor escape the component.
- Internal references identify live component endpoints by graph/node/endpoint
  ordinals and structural integer paths. They retain empty, bound-invalid,
  peered, and non-peered distinctions. No process address is persisted.
- Restore constructs a fresh graph. Endpoint import is quiet and preserves
  validity and modification times. Recordable state is available before start.
  Restored input values are not new events and must not be evaluated again.
- Map membership and child state are recovered together. New and reinserted
  keys start fresh. Child identity is structural and never a diagnostic path.
- Sets, dictionaries, and mapped children retain integer slots, capacity, and
  free-slot allocation order, including the effect of pending erasures. Restore
  must not rebuild keyed state by unordered insertion or compact empty slots.
- Capture occurs after successful evaluation and deferred notifications, before
  teardown. Publication occurs only after normal completion and successful stop.
  An exception or an early stop cannot publish a completed day.
- One immutable checkpoint object is the publication unit. Applications name
  the completed day and its predecessor explicitly; this version makes no
  concurrent-writer, mutable-head, broker transaction, or exactly-once claim.
- Static graph identity, endpoint schemas, and execution bounds are validated
  before static import; each dynamic child is validated before its own import.
  Changed strategy code must use an explicit application
  revision; arbitrary function bodies cannot be identified by the runtime.
- Unsupported endpoint representations, semantic local State, scheduler-driven
  nodes, sources/effects inside the component, and unsupported dynamic owners
  fail closed. Stateless compute `schedule_on_start` bootstraps are allowed and
  discarded on resume. General scheduler state, references outside the boundary,
  and input-tail journals remain later RFC 0023 stages, not implicit fallbacks.

RecordableState already is the hidden output endpoint; there is no duplicate
internal state to synchronize through an additional observer.

## Architecture and lifecycle

`TSCheckpointOps` lives beside the representation's value/delta operations.
Each representation captures an owned image and validates a quiet import into
fresh storage. Keyed representations import keys at recorded integer slots and
restore the free-slot complement; map uses the same identities to reconstruct
live child graphs. Images contain no graph or node addresses.

REF operations require an explicit cold-path `TSCheckpointContext`. Capture
converts the runtime reference into an owned tree; import queues a fixup instead
of resolving it while topology is incomplete. The context is passed through
representation recursion, with no process-global or thread-local restore state.
A locator names nested graph ancestry as owning-node/child-slot pairs, then a
node ordinal, endpoint role, and integer child path. Custom endpoint ordinals
come from their semantic owner. A synthetic binding appends requested-schema
and projection steps; each saved adapter also has its own clock image. Empty
references retain their declared schema, and an explicitly adapted declaration
is kept separately from the target endpoint's actual schema.

Wiring assigns component-local identities and a canonical contract signature,
including scalar parameters, endpoint schemas, input policies, connections,
and child templates even when a map has no members. Unsupported ownership or
state is rejected while wiring where possible, with runtime validation before
import as a second guard. The application revision covers semantic code changes
that structural signatures cannot detect.

The executor owns a `ComponentRecoverySession`. Recovery uses these phases:

1. Load the explicitly selected predecessor and validate version, component,
   revision, static graph contracts, and interval bounds.
2. Prepare supported dynamic owners and their children at the saved slots,
   importing owned endpoint values and source baselines without starting graphs.
   Queue reference fixups while restoring their original endpoint clocks.
3. Allocate the recorded synthetic adapters without publishing or following
   unresolved references; every owning endpoint now has a stable address.
4. Resolve internal reference locators, then restore adapter bindings and clocks.
5. Finalize owner-specific aliases and derived topology without evaluating
   historical inputs.
6. Run ordinary start hooks and start the prepared nested graphs in owner order.
   Restore each node's saved input activity and discard historical bootstrap
   schedules, while preserving freshly admitted source events.

Component input boundaries forward the original endpoint rather than copying
deltas: even an uninterrupted configured run must retain the source integer-slot
allocation and free-list order. Copying a removal delta can change the next
insertion order and thus an order-sensitive reduction. The existing lifecycle
observer seam coordinates preparation and start; it does not add a parallel
runtime lifecycle implemented in Python.

The boundary binds even an initially invalid value before consumers start, so
an internal reference keeps following the endpoint when its first value arrives.
If preparation or startup fails, the session detaches restored subscriptions
while all prepared graph storage is still alive, before ordinary rollback can
destroy children. Successful root startup releases that preparation inventory;
later dynamic-child failures cannot traverse retired graph allocations.

The image records value/structural input observation paths. Restore reinstates
these after each node's start hook so a passivated input stays passive. Static
input projections are supported, including Python argument bundles; per-element
activity inside a peered collection is explicitly refused.

At a completed root cycle, capture runs after deferred notifications and before
stop destroys nested graphs. Commit runs only after normal stop succeeds. Error
capture inside managed nodes is rejected so a swallowed exception cannot turn
a partial evaluation into a completed day. The extension checks that the entire
encoded image round-trips before publishing its immutable object.

Core exposes owned images and load/commit callbacks through `GlobalState`; it
has no dependency on the persistence extension. The extension owns the codec,
storage configuration, predecessor selection, and publication. Python adapts
user callables and configuration to these native paths.

Capture/restore and storage are proportional to the complete retained image,
including reserved keyed capacity. Windows instead store one typed sequence
of live samples and their chronological timestamps, without ring spare capacity,
per-sample endpoint images, or transient removed/reset deltas. Their restore is
linear in live samples and does not replay pushes or expire retained samples.
This implementation trades keyed snapshot
size for exact slot reuse and straightforward validation. Checkpoint operations
do not run per tick; selected component input wrappers forward the source
endpoint and preserve its keyed storage.
Periodic snapshots with an input journal can reuse these image contracts once
source cursors, replay ordering, and effect suppression have explicit contracts.

## Delivery steps

1. Add representation-owned exact endpoint images and quiet import, with
   explicit eligibility and native round-trip tests.
2. Add component ownership/identity during wiring, automatic endpoint capture,
   restore before start, and successful-run capture/publication coordination.
3. Add map membership/child capture and restore without historical bootstrap
   evaluations. Reject dynamic owners without checkpoint operations.
4. Add an immutable durable checkpoint store in hgraph-persistence, native and
   Python configuration, and a process-restart example.
5. Prove uninterrupted-versus-restarted behavior, partial failure and changed
   schema rejection through public native and Python wiring.
6. Run fresh full native and Python 3.14 compatibility gates on macOS and Linux,
   installed-SDK consumers, appropriate sanitizer coverage, and Windows
   validation when a suitable host is available.

Steps 1–5 are implemented for the declared component subset. Acceptance includes
native and Python public-wiring restart tests, real process restart through the
durable example, malformed image refusal, and no publication after evaluation,
stop, or encoding failure. Full-graph, general scheduler state, references
outside the component boundary, and input-tail replay support remain outside
this implementation. Python component `recordable_id` and per-node
`__recordable_id__` remain optional; omitted IDs use the existing function-name
and structural defaults. The application revision remains an explicit semantic
compatibility contract.

The scenario expansion adds the following owner-specific recovery contracts:

- Dynamic list maps retain live child indices and index creation clocks. Shrunk
  children are retired; regrowth creates fresh state.
- Reductions retain dense leaf order, source slots, tree capacity, combiner
  state, and hidden publication endpoints. Ordered reductions preserve order.
- Owned-output meshes retain instance slots, dependencies, ranks, key clocks,
  and pending removals. Private sibling/key-set subscriptions rebind quietly.
- Forwarding endpoints use owner-selected sources and clock-only images.
  The first release includes ordinal locators for internal REF endpoints and
  records synthetic adapter recipes and clocks, including fixed-list ordered-reduce
  selection. Referring outside the closed component remains unsupported.
- Count and duration TSW retain values, timestamps, warmup and invalidation.
  Duration expiry keeps its existing incoming-sample semantics.

Validation compares uninterrupted traces with every individual split and repeated
single-cycle restarts, including empty/invalid state, quiet intervals, partial
structures, churn, nested maps, reductions, recursive meshes, and window resets.
Malformed images and failure publication remain separate negative tests.

The first-release implementation includes 414 durable Python scenarios and
native coverage for its supported runtime paths. Reference scenarios include
retargeting, empty and bound-invalid targets, non-peered structures with partial
child validity, recordable-state references, mapped membership churn, moving recursive-mesh subscriptions, and
malformed locators and adapter inventories. Capture excludes cached adapters
whose source slots have retired; a saved reference to a retired endpoint is
still refused.

| Implementation acceptance gate | Result |
| --- | --- |
| Fresh native acceptance builds and final complete suites | 1,900 passed on each of macOS, Linux, and Windows (MSVC 19.51) |
| Python 3.12 stable-ABI wheel, fresh Python 3.14 non-WIP suite | 3,480 passed, 10 skipped on each of macOS, Linux, and Windows |
| Persistence Python suite, including separate-process example | macOS and Windows: 520 passed, 1 skipped; Linux: 521 passed |
| Installed core and persistence C++ SDK consumers | Passed on all three platforms, including durable window and internal-reference restarts |
| Linux AddressSanitizer | 1,795 core cases plus 14 checkpoint-store cases passed; leak detection disabled according to the documented retained-cache test convention |
| Documentation | Sphinx dummy build with warnings treated as errors passed |

The native durable integration uses the public component and evaluation APIs,
reopens a local store between runs, verifies quiet restore and continuation,
and confirms that a failed subsequent day leaves its predecessor intact.
Installed SDK consumers additionally compile and run window and internal-reference component restarts
through the installed C++ boundary and persistence APIs. The reproducible TSW
benchmark and measurement definitions are in
`extensions/persistence/benchmarks/README.md`; image size follows live samples,
not configured count capacity.

The first release also refuses keyed interior adapters (for example, a `TSD` of
REF values observed as an ordinary `TSD`) and REF values inside custom hidden-owner
endpoint images whose owner does not supply reference-aware checkpointing.
Ordinary node recordable-state endpoints do receive the reference context.
Owner-specific forwarding-terminal restrictions still apply. A full graph
image, pending semantic schedules, online snapshot/suspend, and input journal
replay remain future work. The durable envelope and endpoint/component image
versions are 1 for the first release. Unreleased development snapshots are not
a compatibility contract; unsupported versions are rejected rather than migrated.
