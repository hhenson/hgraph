RFC 0008: Prepared Node Inputs
==============================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-29
:Target: Node invocation, input routing, and endpoint notification

Summary
-------

Move stable node-invocation work out of the per-tick path.  Native static nodes
prepare their input root, immutable scalar bundle, and output handle once
during start.  Each callback then creates one lightweight invocation frame
which borrows those prepared objects for the current evaluation time and
projects each input slot once.  Input views resolve a forwarding route when the
evaluation snapshot is created, not every time ``valid``, ``modified``,
``value``, or ``delta_value`` reads that snapshot.

The design relies on the existing event-driven binding contract.  Target links
and active-path observers update their stable target handles when a route is
bound, unbound, invalidated, or repointed.  A new evaluation snapshot reads that
current handle; ordinary value ticks neither relink nor revalidate the route.
Dynamic keyed children remain governed by their slot-store and slot-observer
lifecycle.

Motivation
----------

Profiling the fixed per-node cost shows more time reconstructing stable access
structures than performing the scalar operation:

* input root, bundle conversion, child projection, and size checks: about
  15.4 percent;
* forwarding-chain resolution: about 5.7 percent;
* terminal endpoint notification: about 4.3 percent;
* repeated ``NodeTypeRef::ops_ref`` lookup: about 2.3 percent; and
* the scalar add itself: about 1.9 percent.

The graph has already validated node schemas, endpoint shapes, input slot
indices, and scalar slots before execution.  Bind and invalidation operations
already notify active target paths.  Repeating those checks and reconstructing
the same root views on every property access adds cost without strengthening a
runtime invariant.

Ownership boundary
------------------

This is a core runtime facility.  It applies to the C++ node invocation path,
the optional Python bridge, system-node child graphs, and independently built
extensions participating in the same runtime.

The C++ runtime remains the source of truth.  Python may cache wrappers and
adapt values around the prepared C++ views, but it does not implement a second
route cache or independently decide when a binding is stale.

Runtime contract
----------------

Prepared static invocation state
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A static node with at least one reusable invocation component receives one
planned storage field.  Its exact representation is private, but it may contain
only components present in that node's declared signature:

* one input-root view for a node with ``In`` selectors;
* one immutable scalar-bundle view for a node with ``Scalar`` selectors;
* one output handle for a node with an ``Out`` selector; and
* one recordable-state handle when required by a later implementation stage.

The storage is constructed in place with the node and is prepared after input
activation but before the implementation's start hook.  It is destroyed before
the input and output storage it borrows.  It adds no heap allocation and does
not add a word to ``AnyPtr``, ``TSDataStorageRef``, ``TSInputView``, or
``NodeView``.

Each start, evaluate, or stop callback creates one stack-local invocation
frame.  The frame:

* borrows the prepared input root at the callback's evaluation time;
* projects input slots directly by their already-validated canonical index;
* reads scalar slots from the prepared immutable bundle;
* reconstructs output views from stable handles and the current time; and
* falls back to the existing ``NodeView`` operations for genuinely dynamic
  facilities such as replaceable state or scheduling.

Preparation belongs to lifecycle, not graph construction, because endpoint
storage has its final address and active route observers exist at start.
Preparation is idempotent so a failed start can be retried safely.

Evaluation snapshots
~~~~~~~~~~~~~~~~~~~~

``TSInputView`` is a transient evaluation snapshot.  Construction or
``borrowed_ref(evaluation_time)`` captures the route current for that
evaluation.  Repeated reads of the same view use the captured ``TSDataView``;
they do not resolve the target chain again.

Calling ``bind_output``, ``bind_output_sampled``, or ``unbind_output`` through
that same view updates its snapshot immediately.  A route changed through a
different view becomes visible when the consumer creates its next evaluation
snapshot.  Runtime callbacks already receive fresh or explicitly reborrowed
views per evaluation, so this does not delay graph-observable changes.

Retaining an input view across evaluations without calling
``borrowed_ref(new_time)`` is unsupported.  The evaluation time was already
part of the view's delta and modification semantics; this RFC makes the route
part of the same snapshot boundary.

Route invalidation
~~~~~~~~~~~~~~~~~~

The existing target-link state remains the route owner.  The following events
may change its current target and active observed handles:

* initial bind and explicit rebind;
* unbind or source invalidation;
* forwarding or ``REF`` repoint;
* sampled binding;
* dynamic slot replacement, erasure, or reuse; and
* nested-graph teardown.

Ordinary value modification does not dirty topology.  It schedules active
consumers through the existing notification path but leaves the route handle
unchanged.

A prepared static input stores only the stable non-peered input root.  Its
per-evaluation child projection reads the target link's current handle, so
repointing does not require patching every node cache.  Target paths below a
peered structural root continue to use the active target trie, whose observed
handles are replaced by the existing resubscription logic.

Readiness checks
~~~~~~~~~~~~~~~~

The common readiness gate creates one root input view and, for a TSB root, one
bundle projection.  It checks every required slot through that shared
projection.  It must not reconstruct the root and bundle independently for
each validity policy.

Endpoint notification
~~~~~~~~~~~~~~~~~~~~~

Nested TSData continues to bubble modification state to its owning root.  Once
that root reaches a node-owned input or output endpoint, the terminal
``record_child_modified`` operation is a no-op: the root tracking was already
updated and subscribers were already notified.  The hot path may therefore
stop at the node endpoint instead of resolving the node type and redispatching
to a no-op.

This does not remove child notification within TSB, TSL, TSD, TSS, TSW, proxy,
or forwarding storage.  Those layers own real delta, membership, or tracking
state and remain unchanged.

Dynamic and nested graphs
-------------------------

Map, mesh, switch, reduce, and other nested graph nodes retain their existing
slot-store ownership protocols.  A prepared view must not keep a dynamic child
alive, bypass an erase, or retain a retired-object side container.

Static child nodes inside a nested graph prepare their own stable root after
their node storage is attached.  Their input links may repoint while keys churn;
the next invocation projects the current slot target.  Paused graphs retain
their prepared node roots because the node allocation remains live.  Stop
deactivates links before graph storage is erased, and prepared views are
destroyed before the endpoints they borrow.

C++ and Python contract
-----------------------

The first-class C++ path is the static-node invocation frame described above.
The public selector types and wiring syntax do not change.

The Python bridge continues to receive C++ ``TSInputView`` snapshots.  Its fast
compute cache must call ``borrowed_ref(now)`` for a view retained across
callbacks.  Python wrapper reuse is independent of route validity: replacing a
wrapper's C++ view replaces its complete evaluation snapshot.

No new Python-only semantic or configuration switch is introduced.

Compatibility and ABI
---------------------

The node, TSData, input-view, and pointer ABIs do not gain fields.  Static node
storage plans gain a private component and therefore static bytes, but node
storage is planned and constructed by the same runtime version; downstream
code does not embed that layout.

The evaluation-snapshot rule is a clarification of the existing transient-view
and explicit-``borrowed_ref`` API.  Code which retained a view across ticks and
expected its route and evaluation time to update implicitly must reborrow it.

There is no serialization or wire-format change.  Prepared views and route
handles are derived runtime state and are never recorded.

Performance and memory goals
----------------------------

* Construct the static input root, scalar bundle, and output handle once per
  node start, with no heap allocation.
* Construct at most one input-root borrow per static callback.
* Perform no bundle kind or size validation for a canonical input slot after
  the node type and endpoint have been validated.
* Resolve a target route at most once per evaluation snapshot; repeated
  property reads perform zero route rebuilds.
* Preserve the existing active-route handle on ordinary value ticks.
* Add planned storage only to static nodes that have reusable components.
* Keep dynamic keyed collection and nested-graph lifecycle allocation
  behaviour unchanged.

Implementation stages
---------------------

1. Share one root and bundle in the common readiness gate.
2. Add private planned invocation state and a stack-local frame for native
   static node callbacks.
3. Make input views capture a resolved route when constructed or reborrowed,
   and reuse it for all reads during that evaluation.
4. Stop terminal node-endpoint notification after root tracking is complete.
5. Apply the same prepared-root contract to Python and system-node paths only
   where profiling shows remaining reconstruction.
6. Consider promoting a stable input-slot handle to the public extension
   bridge in a separate RFC amendment after at least one installed SDK consumer
   proves the private representation.

Alternatives considered
-----------------------

Caching a ``TSInputView`` for every input slot was rejected because it
multiplies per-node memory and makes dynamic-child lifetime harder to audit.
The prepared non-peered root is sufficient for static nodes and leaves target
ownership in the link.

Adding cached ops pointers to ``NodeView`` or another word to the common
type-erased pointers was rejected because it taxes every cold and hot use.  A
stack-local invocation frame can cache the same information only while needed.

Polling a route generation on every property read was rejected.  It replaces
one repeated validation with another and ignores the existing notification and
active-observer design.

Acceptance criteria
-------------------

The implementation requires:

* native public-wiring tests for one and multiple inputs, passive and unchecked
  inputs, TSB/TSL projections, scalar configuration, and lifecycle-only
  selectors;
* route tests for bind, rebind, sampled rebind, unbind, forwarding/REF paths,
  and repeated reads from one snapshot;
* map and mesh key erase/reuse tests proving prepared views do not retain a
  child slot;
* Python parity tests for direct and paired fast-compute inputs, retained
  wrapper expiry, and reference repointing;
* the complete C++ and non-WIP Python 3.14 suites on macOS and Linux, with ASan
  for the prepared-view lifetime change; and
* five-sample before/after full benchmark packs on the same macOS and Linux
  hosts, plus focused native microbenchmarks where the full graph scenarios do
  not isolate the changed cost.

The benchmark report must record absolute medians for each host and the
``hg_cpp / legacy C++`` throughput ratio.  A regression larger than five
percent in an unaffected workload requires investigation before merge.

Implementation status
---------------------

The pre-change full benchmark uses revision ``16fac510b6b4`` with five fresh
process samples per scenario:

* macOS 26.5, Apple M4 Max, Apple Clang 21, Python 3.14.6;
* Linux x86_64, Intel Core Ultra 7 155H, GCC 15.2, Python 3.14.4.

Post-change results and any implementation deviations will be added before the
RFC is accepted.
