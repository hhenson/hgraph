# Component recovery implementation plan

This change implements a deliberately restricted first checkpoint capability
under RFC 0023, using the ownership boundary in RFC 0025. It targets deterministic
day-by-day simulation components whose external inputs are explicit ports.

## Contract

- A component checkpoint contains boundary input images, ordinary outputs,
  hidden recordable-state and error endpoints, and supported dynamic children.
- Each external input owns a dedicated direct pull-source endpoint baseline.
  Restore imports this baseline before source start so collection deltas retain
  their meaning. The caller supplies future-only events; source cursor and
  scheduling state remain outside the image. Projected/computed ingress and
  sources shared with other consumers are refused.
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
- Unsupported endpoint representations, semantic local State, schedules,
  sources/effects inside the component, and unsupported dynamic owners fail
  closed. General reduce/mesh/window recovery and input-tail journals remain
  later RFC 0023 stages, not implicit fallbacks.

RecordableState already is the hidden output endpoint; there is no duplicate
internal state to synchronize through an additional observer.

## Architecture and lifecycle

`TSCheckpointOps` lives beside the representation's value/delta operations.
Each representation captures an owned image and validates a quiet import into
fresh storage. Keyed representations import keys at recorded integer slots and
restore the free-slot complement; map uses the same identities to reconstruct
live child graphs. Images contain no graph or node addresses.

Wiring assigns component-local identities and a canonical contract signature,
including scalar parameters, endpoint schemas, input policies, connections,
and child templates even when a map has no members. Unsupported ownership or
state is rejected while wiring where possible, with runtime validation before
import as a second guard. The application revision covers semantic code changes
that structural signatures cannot detect.

The executor owns a `ComponentRecoverySession`. Before root start it loads and
validates the selected predecessor and restores static endpoints and ingress
baselines. A post-start observer reconstructs supported dynamic owners after
their storage exists, importing child endpoints before each child's start.
Historical bootstrap schedules are discarded; external source admission and
future event scheduling remain intact. This uses the existing lifecycle
observer seam rather than adding a second independent lifecycle to every node.
The image also records value/structural input observation paths. Restore
reinstates these after each node's start hook so a passivated input stays
passive. Static input projections are supported, including Python argument
bundles; per-element activity inside a peered collection is explicitly refused.

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
including reserved keyed capacity. This first implementation trades snapshot
size for exact slot reuse and straightforward validation. Checkpoint operations
do not run per tick; selected component input wrappers forward normal deltas.
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
stop, or encoding failure. Full-graph, scheduler, reference, reduce/mesh, and
input-tail replay support remain outside this implementation.

Step 6 is complete for this change:

| Acceptance gate | Result |
| --- | --- |
| Fresh native acceptance builds and final complete suites | 1,839 tests passed on each of macOS, Linux, and Windows |
| Python 3.12 stable-ABI wheel, fresh Python 3.14 non-WIP suite | 3,480 passed, 10 skipped on each platform |
| Persistence Python suite, including separate-process example | macOS and Windows: 106 passed, 1 skipped; Linux: 107 passed |
| Installed core and persistence C++ SDK consumers | Passed on all three platforms |
| Linux AddressSanitizer | 1,734 core cases plus 8 checkpoint-store cases passed; leak detection disabled according to the documented retained-cache test convention |
| Documentation | Sphinx dummy build with warnings treated as errors passed |

The native durable integration uses the public component and evaluation APIs,
reopens a local store between runs, verifies quiet restore and continuation,
and confirms that a failed subsequent day leaves its predecessor intact.
