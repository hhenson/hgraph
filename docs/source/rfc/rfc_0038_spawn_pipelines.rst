RFC 0038: Parent-Timed Spawned Sink Graphs and Pipelines
========================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-17
:Target: Native runtime, C++ graph wiring, and Python graph authoring
:Related: RFC 0017 (binary value codec), RFC 0023 (checkpoint recovery),
          RFC 0026 (versioned dataflow fabric), RFC 0027 (bounded queues),
          RFC 0036 (reference transparency), RFC 0037 (distributed maps)

Summary
-------

Introduce ``spawn_`` for a persistent, independently executing **sink graph**
whose logical time is controlled by its owning graph. A child may run behind
its owner, but never ahead. There is no time-series feedback into the owner.

Introduce ``pipeline_`` to compose an ordered list of persistent stages ending
in a sink, and ``bind_`` to bind named external time-series inputs and scalar
configuration independently of the pipeline's main flow. The composition is a
wiring-time plan consumed by ``spawn_``. Each listed stage is an execution
boundary; ordinary graph composition inside a stage continues to flatten.

A child requests permission to proceed when its input queue is empty and it
has pending scheduled work. The owner grants time it has already completed,
or arranges a notification when it reaches the requested time. Permission
never overtakes input changes. An idle upstream stage must also answer a
downstream demand for progress, even when it has no output tick to publish.

Motivation and ownership
------------------------

A signal-producing graph should be able to feed a slower portfolio or order
construction graph without waiting for that computation every cycle. The
consumer needs the original evaluation timestamps, consistent same-time
inputs, historical sources and its own timers. It does not need to send a
result back into the producer's current evaluation.

``dmap_`` is a different contract: it distributes mapped computation and
collects results behind a per-cycle barrier. Ordinary ``map_``, ``reduce`` and
``mesh_`` are graph composition within one execution owner. The versioned
fabric in RFC 0026 joins independently timed executions through durable data.
This RFC supplies parent-timed asynchronous execution without a per-cycle
result barrier or a durable broker.

Scheduling, execution ownership, admission, boundary transfer and lifecycle
belong in C++. Python adapts signatures, callables and values to the same
prepared native plan. There is no Python queue or independent Python scheduler
implementing these semantics. This is a generally useful runtime boundary,
not a domain-specific downstream policy promoted into core.

Public authoring contract
-------------------------

Python
~~~~~~

A direct spawn accepts a sink signature and has no output port::

    @graph
    def portfolio_sink(signal: TS[Signal], prices: TSD[str, TS[float]]) -> None:
        ...

    spawn_(portfolio_sink, signal=signals, prices=prices)

``spawn_`` wires an owned execution boundary. It does not start threads or
perform evaluation while the graph body is being wired. It returns ``None``;
normal enclosing-graph lifecycle starts and stops its child. A callable with a
time-series return type is rejected, rather than silently discarding results.
An outputless child with no time-series arguments is valid; its internal
scheduled sources can request progress from the owner.

A pipeline separates its main flow from additional bindings::

    spawn_(
        pipeline_([
            bind_(construct_portfolio, prices=prices, config=portfolio_config),
            bind_(generate_orders, limits=limits),
            publish_orders,
        ]),
        signal=signals,
    )

``bind_`` is a wiring-time partial application. Named bindings may be scalar
configuration or time-series ports. The plan retains typed wiring descriptors,
not borrowed runtime views. Binding does not implicitly make an input passive
or sample it only when the main flow ticks. Existing graph input activation
rules continue to apply.

The binding and composition rules are:

* The first entry's unbound arguments become the pipeline's public arguments.
  They are supplied to ``spawn_`` using ordinary positional/named argument
  resolution, scalar defaults and type resolution.
* Every later entry has exactly one remaining unbound time-series input. The
  preceding entry's output binds that input. Other required arguments must be
  supplied by ``bind_`` or their declared defaults.
* Every nonterminal entry produces one time-series output. A structured output
  such as a bundle is one connection; it is not implicitly unpacked into named
  arguments. An adapter graph expresses any desired reshaping.
* A plan supplied to ``spawn_`` must end in a sink. Intermediate sinks,
  incompatible schemas, ambiguous flow inputs, duplicate bindings and missing
  required bindings are wiring errors.
* Nested ``pipeline_`` descriptions flatten into one ordered stage list without
  adding execution boundaries. A reusable fragment may have an output; only
  the complete spawned plan must terminate in a sink. An empty pipeline is
  rejected. An explicit pass-through graph is a valid intermediate stage.
* Plans are immutable descriptions and may be reused. Each ``spawn_`` call
  creates a distinct execution instance and lifecycle; it must not be deduped
  merely because its inputs and plan compare equal.

Ordinary graph arguments remain ordinary graph arguments. Execution settings
use a dedicated spawn configuration descriptor, so buffer limits and hosting
policy cannot consume or shadow a child's application arguments. Concrete
configuration spelling and defaults must be documented with the implementation.

Native C++
~~~~~~~~~~

Native graph structs retain ``compose(Wiring &, ...)`` and typed ``Port`` and
``Scalar`` parameters. A public typed spawn facade must accept a sink graph,
perform the same argument resolution and return no ``Port``. A public native
pipeline/binding facade must represent heterogeneous graph signatures without
routing through Python or requiring user-written serialization code.

Both facades lower to one prepared native execution plan containing:

* the ordered, independently built stage graphs;
* resolved boundary schemas and input-slot bindings;
* immutable scalar configuration and stage identities;
* prepared boundary capture/application operations;
* selected execution/admission policy and bounded resource limits.

The public native facade uses ``SpawnStage`` (callable, bindings and optional
callable owner) and ``SpawnPipeline``. ``spawn_fn<G>(named_scalar_args...)``
creates a native stage with scalar configuration. ``bind_`` accepts a stage or
``WiredFn`` plus named ``WiringPortRef`` bindings, and ``pipeline_`` accepts an
ordered vector of stages. ``wire_spawn(Wiring &, SpawnPipeline,
span<WiringArg>, SpawnConfig)`` wires the sink boundary. Compiled public wiring
and installed-SDK examples must verify these overloads. Python-only support
does not satisfy this RFC. Runtime correctness must not depend on C++ template
instantiation; erased callers receive equivalent plan validation.


Logical time and protocol
-------------------------

Terms
~~~~~

An owner's **completed frontier** ``F`` means it has finished all evaluation
work and published all boundary input changes at times through ``F``. Merely
entering a cycle at ``F`` does not establish that frontier.

A child's **grant** ``G`` permits evaluation through ``G``. A child may have
consumed only an earlier prefix. Its **completed frontier** advances only after
it has consumed all applicable inputs and finished every scheduled evaluation
through that point. A clock frontier is not a time-series tick.

Every channel has a run identity and monotonically increasing sequence. One
owned input frame contains one completed source cycle's changed input slots
and original evaluation time. Multiple bound inputs from the same source
cycle are atomic with respect to child evaluation.

The control protocol has these logical messages:

``InputFrame(sequence, time, deltas)``
   Ordered, owned input changes. It must be possible to distinguish no tick,
   invalidation, membership removal and valid empty values.

``RequestProceed(request_id, time)``
   The child's earliest pending scheduled time, or time demanded by a
   downstream stage that the child cannot yet authorize. At most one request
   is outstanding per child; replacement requests have a newer identifier.

``Grant(request_id, frontier, through_sequence)``
   All source input frames through ``frontier`` have been published. The
   receiver must consume through ``through_sequence`` before using the grant.
   A completed input frame may carry this authorization, avoiding a redundant
   control frame for an ordinary data tick.

``Seal(final_frontier, through_sequence)``
   The channel will publish no more input; drain its finite authorized prefix.
   Sealing is not permission to run future scheduled work indefinitely.

``Complete`` / ``Failure``
   Lifecycle outcomes, including sufficient stage identity to attribute an
   error. Admission, graph processing and durable external effects are
   distinct events.

These names describe protocol meaning, not a requirement for one heap object
or transport packet per message. The process transport encodes input and
cycle requests. Coalescing grants is valid
only when it preserves their sequence fence. Input frames are never silently
conflated or dropped.

Owner transitions
~~~~~~~~~~~~~~~~~

On ``RequestProceed(id, T)``, the owning graph's execution thread:

1. ignores a stale request identifier or a request from a stopped generation;
2. grants through ``T`` immediately when ``T <= F``;
3. otherwise registers a notification for ``T`` and grants only after that
   evaluation cycle and its input publication have completed.

The child sends the request through a runtime-owned control mailbox. It does
not retain or manipulate the owner's graph, node scheduler or executor from
its execution thread. A notification is a wake-up; the mailbox owns the
request until the owner consumes it. Schedule registration is owner-thread
work. Real-time wake and simulation control pumping are explicit executor
integrations, not a simulated push source.

Simulation must not declare the owner quiescent while a live child can still
report a pending schedule. The runtime establishes a stable idle/request state
before selecting its next time or completing the run. It services control
messages while waiting for child capacity or quiescence. An absent future
input is not evidence that no future child work exists.

Child transitions
~~~~~~~~~~~~~~~~~

The child has one evaluation owner. That owner:

1. processes queued input in increasing timestamp/sequence order;
2. before staging input at ``T``, evaluates all due child work strictly before
   ``T`` that is authorized by the relevant grants;
3. stages all input changes at ``T`` atomically, then evaluates input-triggered
   and scheduled work at ``T`` together;
4. continues scheduled work through the authorized frontier;
5. when the input queue is empty and further scheduled work requires a later
   time, sends one proceed request and waits.

Installing the waiting state and checking for new queue/control work is an
atomic protocol operation. A producer cannot insert a frame between the final
empty check and sleeping without waking the child. A stale grant may advance
an already valid monotonic frontier but must not resurrect a stopped child.

No child is stepped over its next scheduled evaluation. No future input is
staged early while earlier timers run. Progress-only authorization does not
mark an input modified, establish validity, or force unscheduled nodes to run.
A child with no pending work remains idle unless a downstream stage requests
progress or new input arrives.

Pipeline alignment and external bindings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A later stage can receive both its flow from the preceding stage and additional
ports bound from the enclosing graph. Each source has an ordered input prefix
and a completed frontier. The stage's effective grant is the minimum of its
required source frontiers. Scalars impose no run-time frontier dependency.

The stage merges frames by timestamp. It waits until every source has closed
the timestamp before evaluating it, then applies all changes at that time in
one child cycle. An unchanged source closes time through its grant; it does
not need to manufacture an empty input tick. Bound inputs have the value
valid at that logical time, never the owner's latest value at the wall-clock
instant the child happens to run.

A downstream request propagates demand to any predecessor whose frontier is
insufficient. An idle predecessor requests owner permission if necessary,
finishes its own due work, publishes any output and then advances its frontier.
The parent coordinator represents the owner's completed frontier as a
monotonic watermark, published at cycle end. Each worker replies with its next
scheduled time after completing an authorized evaluation. The coordinator
advances that stage's frontier only after forwarding its output. Reading a
watermark still requires the input-sequence fence described above; it is not
permission to race pending boundary capture. Idle progress is tracked by the
coordinator, without evaluating the worker or sending an empty input tick.
Demand travels to the owner through its execution activity integration.

This works transitively through a chain. Without demand propagation, an idle
stage with no output ticks would deadlock a downstream timer.

The first version supports linear pipelines and additional bindings from the
same enclosing graph. Arbitrary multi-parent joins, feedback, independently
clocked live inputs, nested spawn boundaries and cyclic execution dependencies
are rejected. Fan-out
can be a subsequent extension with explicit consumer retention rules.

Observable traces
~~~~~~~~~~~~~~~~~

**Same-time data and timer.** The child is complete through 10, has a timer at
12, and requests 12. At 12 the owner changes two bound inputs. The owner
publishes one frame containing both changes and then the grant. The child
observes both new values in its evaluation at 12, including its scheduled
work. Evaluating its timer first against the old inputs violates the contract.

**Lag and intervening work.** The owner is complete through 20. The child has
an input frame at 15 and an earlier timer at 13. The child executes 13 with
its previous inputs, applies the frame and executes 15, then processes later
authorized work. Staging the input from 15 before evaluating 13 is incorrect.

**No data.** A child completed through 10 has a timer at 12. If the owner has
completed 14, permission for 12 is immediate. If the owner has completed 11,
it schedules notification at 12. The unchanged boundary inputs do not tick.

**External binding.** The owner updates ``limits`` at 12 and is already at 20.
The preceding portfolio stage is only complete through 11. The orders stage
buffers ``limits``. When portfolio closes 12, any portfolio change and the
limits change are applied together. It cannot use the limits from 20.

**Idle intermediate.** Stage C requests 15; stage B has no scheduled work and
is complete through 12. B requests progress from A, closes its own input and
work through 15, and grants C through 15 even if B never emitted a data tick.

Boundary types and state
------------------------

Reuse the schema-prepared boundary transfer contract from distributed maps:
``TS``, ``SIGNAL``, ``TSS``, ``TSD``, fixed and dynamic ``TSL``, ``TSB`` and
``TSW``, recursively, with supported scalar payload encodings. Preserve
validity, invalid members, removals, dynamic list extent, bundle field deltas
and original window sample timestamps. Unsupported scalar handles fail during
wiring, not on the first live tick.

References are materialized at every execution boundary. Internal references
within one stage remain local. A child must not retain a reference into the
owning graph, a sibling stage or a producer's transient view. New/rebound
subtrees carry their current state once; ordinary cycles transfer incremental
changes. In particular, ``TSW`` warm-up/full transfer retains its history, while
steady-state append, clear and scheduled eviction do not copy the complete
window on every tick.

``map_``, ``reduce`` and ``mesh_`` may run inside a stage under their ordinary
contracts. Their local slots and references remain owned by that stage. This
RFC does not make asynchronous boundaries checkpointable, nor extend
checkpoint recovery across in-flight channels. Durable restart requires a
consistent frontier fence, graph checkpoints and channel cursor recovery; it
is a separate capability. External effects are not automatically exactly-once.

RFC 0039 adds the one case that needs none of that machinery: recovery at a
*completed-day* boundary. There the executor has already settled every stage,
so the frontier fence holds and every channel is empty; the owner asserts that
and saves one image per stage. What those images hold is a ``component`` wired
inside a stage -- the recoverable unit is the usual one -- and everything else
in the stage, the terminal sink first of all, is processed rather than
recovered. A checkpoint taken *during* a run -- with
frames in flight -- still needs a real fence and channel cursors, and is still
out of scope.

Historical and scheduled pull sources inside a stage run against its authorized
logical time. Independent live push sources have no parent-clock closure
contract and are rejected in this first version. Captured outer ports,
services or contexts must not create hidden cross-execution edges; unsupported
captures fail wiring. Graph-local configuration belongs in the run's
``GlobalState``, never an unrelated process global.

Admission, memory and lifecycle
-------------------------------

Channels are lossless and bounded in both frames and accounted payload bytes.
A frame larger than its configured maximum fails with a diagnostic rather than
waiting forever for impossible capacity. The selected admission policy pauses
upstream at a safe cycle boundary when capacity is exhausted. A fail-on-full
policy may be offered explicitly. Dropping and conflation are outside v1.

Flow and external-binding channels have separate bounded admission. A future
external frame must not fill a shared queue and prevent arrival of the earlier
predecessor output needed to release that frame. The coordinator must also
continue progress/control handling while either data channel is full.

Each channel's admission limits include queued and in-flight frames until
processing and forwarding complete. In addition, each producer may hold one
bounded frame under construction per destination channel, before admission;
that frame is subject to the same per-frame byte maximum. Decoded graph state
and transient codec allocations are separate from these payload-byte limits.
This accounts for storage outside the queue container explicitly. Data
saturation does not consume control capacity: workers can publish completion
and failure while their producer waits for admission. Every already admitted
frame has permission to execute, so releasing capacity never depends on a
future producer cycle. Waiting Python paths release the GIL. Arbitrary node callbacks must not block on a child whose
progress requires that same callback or owner cycle to finish.

Each stage runs in a separate worker process with an isolated native executor
and owned binary boundary payloads. Each process owns its input state,
scheduler, resources and, for Python stages, interpreter and GIL. Parent-side
transport threads retain only owned messages, progress cursors and channels;
they never construct or evaluate child graphs. There is no thread-hosted graph
mode or fallback.

The existing distributed-worker launcher supplies cross-platform process
creation, framed IPC, finite deadlines, termination and reaping. A native
worker reconstructs its graph from a registered factory and immutable bootstrap
configuration. A Python worker imports a module-level callable and decodes its
scalar bindings; closures and ``__main__`` callables fail at wiring time.
Executable code and live pointers never cross the boundary. Both sides verify
canonical input and output schema identities before reporting readiness.
Process workers execute trusted code; process separation is not a sandbox.

Start acquires resources with rollback and reports readiness before accepting
input. Partial start failure stops and joins already-started stages. Normal
shutdown seals input at the owner's final completed frontier, drains accepted
work within that frontier in upstream-to-downstream order, then stops each
stage and joins all workers. Pending requests beyond the final frontier are
cancelled explicitly. A self-rescheduling child cannot extend the run beyond
the enclosing run's end-time policy, including the real-time executor's
bounded immediate-cycle drain. A boundary created dynamically starts at its
activation time; it cannot replay timers from before it existed.

An early child ``request_stop`` is unsupported: it fails the enclosing run
with a lifecycle error rather than silently discarding already accepted input.
Normal completion is driven by the owner's final seal.

A child failure fails the owned pipeline and is reported to the enclosing run.
Idle workers are checked for process exit at intervals of at most 100 ms
(shortened for smaller worker timeouts), so failure wakes an idle owner without
waiting for another input or the run end time.
Failure/cancellation wakes all capacity and progress waiters. Cleanup is safe
after partial construction and does not await data that can no longer arrive.
There is no detached background work after the enclosing run returns.
``SpawnConfig.worker_timeout`` and Python ``__worker_timeout__`` default to
60 seconds and bound bootstrap, each complete evaluation exchange and shutdown.
Timeouts cover partial writes and reads as well as blocked callbacks. Failure
terminates and reaps unresponsive worker processes. A timeout does not imply
that an external sink effect was rolled back, and workers are not restarted.

Performance model
-----------------

Let ``D`` be the bytes of changed boundary data, ``I`` the number of participating
input channels, ``S`` the number of stages and ``Q`` the configured in-flight
capacity. Prepared schema/binding plans are built once. A normal boundary
capture/application costs O(D) plus work proportional to touched members;
initial state or rebound subtrees cost O(the transferred state). Sparse updates
must not scan the full historical population or full TSW history.

FIFO admission/dequeue is O(1) per frame excluding payload work. Ordered
multi-input selection is O(log I) per frame with a heap, or O(I) with a clearly
bounded small-channel scan; it must not repeatedly scan queued history. Linear
pipeline compilation is O(S + number of bindings). Flattening nested plans
must append to a single destination rather than repeatedly concatenate prefixes.
Do not erase the front of a vector for each consumed frame or search all prior
sequences for each grant. Frontier state is O(I); channel storage is bounded
by Q and payload limits, plus explicitly accounted graph state and fixed
transport overhead.

Benchmark throughput, peak retained bytes and parent pause time for 1, 2 and
several stages; growing payload and collection cardinalities; TSW warm-up and
steady state; sparse dynamic-membership changes; and slow consumers. Report
native computation separately from Python/GIL overhead. Establish scaling
rather than claiming speedup from one elapsed-time measurement.

Compatibility, ABI and alternatives
-----------------------------------

The operators are opt-in and do not change ordinary graph composition,
``component``, ``map_`` or ``dmap_`` behavior. No new persistence format is
introduced. An encoded worker protocol needs explicit version/identity checks
and bounded decoding before live state mutation. Existing distributed protocol
messages must not silently acquire incompatible meanings.

New public native headers and exported out-of-line symbols are installed with
the SDK. A separately built consumer must compile, link and execute a spawned
sink and bound pipeline without Python. Python bindings remain behind their
existing optional build boundary. Configuration and diagnostics must use
portable values, not machine-local handles or private process addresses.

Alternatives considered:

* ``stream_`` describes data delivery but is less explicit about creating an
  owned execution. ``spawn_`` names the boundary; ``pipeline_`` names composition.
* Returning an ordinary output port from ``spawn_`` would imply a same-cycle
  dependency or invent late feedback semantics. Sink-only signatures avoid it.
* A barrier on every parent cycle prevents the desired overlap. Conversely,
  free-running child clocks violate parent timing and historical consistency.
* Sending an empty frame every cycle is correct only with a complete ordering
  contract, but needlessly wakes idle children. Demand-driven progress supplies
  timers without fake ticks.
* A global ring buffer can optimize transport later. The first implementation
  needs the ownership, ordering and bounded-retention contract; its public API
  should not commit to a specific queue or shared-memory representation.

Acceptance criteria and implementation sequence
-----------------------------------------------

Implement in separable, reviewed steps while preserving this contract:

1. Prepared native plans and strict sink/binding/pipeline validation, with typed
   C++ authoring and Python adaptation.
2. Single-stage execution, bounded owned channels, ordered grants and
   owner-thread request handling in real-time and simulation modes.
3. Linear stage composition, same-time external-binding alignment, transitive
   demand and normal/error shutdown.
4. Boundary type/lifecycle coverage, installed-SDK consumers, scaling evidence
   and cross-platform acceptance.

Completion requires public wiring tests in C++ and Python, using synchronous
composition as an observable oracle where applicable. The acceptance matrix is:

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Area
     - Required scenarios
   * - Signatures
     - Direct sink, no-input sink, positional/named arguments, defaults,
       scalar bindings, generic resolution, nested reusable pipeline,
       pass-through stage and all invalid/ambiguous composition cases.
   * - Logical time
     - Behind/at/ahead requests, timers before/at/after inputs, no-input
       timers, no-output progress, arbitrary stage delays, real-time idle
       wake and simulation quiescence, end-time boundaries.
   * - Input alignment
     - Same-time main/external ticks, active and passive bindings, multiple
       external sources from the owner, invalid-to-valid transitions and
       late wall-clock arrival with original logical timestamps.
   * - Time-series types
     - TS/SIGNAL, TSS add/remove, TSD invalid members/removal/re-add,
       fixed/dynamic sparse TSL, partial/nested TSB, count/duration TSW
       history/append/clear/eviction, plain and REF boundary equivalence.
   * - Nested graphs
     - map, reduce and mesh inside stages; changing keyed membership,
       stable local slot semantics, scheduled nested work and teardown.
   * - Lifecycle/failure
     - Partial start failure, full queue, impossible oversized frame,
       stop while waiting for capacity or permission, stale notifications,
       child/transport failure, finite drain, no retained tasks after return.
   * - Complexity
     - Increasing collection size with fixed sparse deltas, growing window
       capacity with steady appends, many frames/stages and bounded-memory
       slow-consumer runs; no quadratic cumulative scans or copies.
   * - Packaging/platforms
     - Native installed consumer, stable-ABI Python wheel and full Python
       compatibility suite; macOS/Linux and available Windows host; memory
       and concurrency sanitizers where supported.

Implementation status
---------------------

The implementation provides native ``wire_spawn``, ``spawn_fn``, ``bind_`` and
``pipeline_`` and Python ``spawn_``, ``bind_`` and ``pipeline_``. Each stage runs
in an owned worker process with a private externally driven C++ executor.
``ExecutorActivity`` and its owned wake capability let a root simulation or
real-time executor authorize child requests without manufacturing input ticks.

Public-wiring tests compare synchronous and asynchronous traces for scalar,
signal, collection, bundle, window and reference boundaries, external bindings,
and local map/reduce/mesh graphs. Dedicated tests cover no-input and downstream
timers, exclusive end times, lifecycle failures, early child stop and failed
owner cycles. The installed-SDK consumer builds and executes a native pipeline.

Nested spawn, independent live child sources, arbitrary joins, feedback,
mid-run restart and asynchronous checkpoint recovery remain outside v1 (completed-day recovery is RFC 0039's). Separate Python
interpreters allow parallel Python bytecode execution. Normal completion drains
authorized work and reaps all workers. Tests assert distinct process identities
and cover worker crashes, blocked callbacks, deadline failure and reaping.

References
----------

* :doc:`rfc_0037_distributed_map` supplies existing externally driven child
  execution and schema-prepared owned boundary transfer.
* :doc:`rfc_0027_bounded_push_source_queues` establishes bounded admission and
  lifetime rules; its real-time push-source timing is not reused for simulation.
* :doc:`rfc_0026_versioned_dataflow_fabric` covers independently versioned,
  durable graph boundaries, outside this parent-clocked scope.
* :doc:`../developer_guide/graph_wiring` and
  :doc:`../developer_guide/real_time_adaptors` define graph composition and
  approved external-thread ownership boundaries.
