RFC 0008: Prepared Node Invocation
==================================

:Status: Accepted (stages 1–4 implemented; stage 5 specified by the
         2026-07-29 amendment below)
:Author: Howard Henson
:Created: 2026-07-29
:Target: Node invocation, input routing, and endpoint notification

Summary
-------

Stop reconstructing the same root projections independently for every
parameter of a node callback.  Each native static-node callback creates one
lightweight invocation frame.  On first use, the frame materializes one input
root and one immutable scalar bundle for that invocation, then projects
already-validated canonical slots from those shared roots.

The frame is stack-local and retains nothing across callbacks.  Binding,
repointing, invalidation, and dynamic-child lifetime therefore keep their
existing semantics.  The design also shares one root in the common readiness
gate and removes a terminal node-endpoint notification whose dispatched
operation is already a no-op.

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
indices, and scalar slots before execution.  Reconstructing the root and its
bundle wrapper independently for each callback argument adds cost without
strengthening a runtime invariant.

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

Evaluation-local invocation preparation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each start, evaluate, or stop callback creates one stack-local invocation
frame.  The frame:

* creates the input root at the callback's evaluation time at most once;
* projects input slots directly by their already-validated canonical index;
* creates the immutable scalar bundle at most once and reads scalar slots from
  it;
* creates the output view directly for the single permitted output selector;
  and
* falls back to the existing ``NodeView`` operations for genuinely dynamic
  facilities such as replaceable state or scheduling.

The frame is lazy, so a hook which does not request an input or scalar root does
not create it.  It adds no planned node storage, heap allocation, or persistent
borrow.  A failed start, paused evaluation, stopped nested graph, or reused
dynamic slot cannot leave invocation state behind.

Input-view semantics
~~~~~~~~~~~~~~~~~~~~

This RFC does not change ``TSInputView`` binding semantics.  A retained view
continues to observe bind, rebind, source invalidation, and forwarding changes
through its target link.  Each callback receives views with the current
evaluation time, exactly as before.

Target links already keep an observed output handle for active paths and
replace that handle when topology changes.  Eliminating the residual
``resolved_target_at_path`` dispatch is a possible later optimization, but it
requires a stable-handle contract and topology-aware invalidation.  It is not
part of this RFC's first implementation.

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

An invocation frame projects from the current input root.  Target paths below
a peered structural root continue to use the active target trie, whose observed
handles are replaced by the existing resubscription logic.

Follow-up stable-handle design
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If profiling after this change still identifies target resolution as a
material fixed cost, the next stage should reuse the route object which the
runtime already maintains rather than cache a copied ``TSDataView``.

For an active value input, each node in the target-link active trie owns a
stable ``TSOutputHandle observed`` object.  Bind, rebind, invalidation,
forwarding changes, and dynamic-slot changes already replace or reset that
object in place.  A prepared slot can therefore hold a non-owning pointer to
the ``observed`` object and create a time-stamped input view directly from its
current contents.  Ordinary value ticks do not touch the pointer or rebuild
the route.

That pointer is valid only while its active-trie node is retained.  The
prepared-slot lifecycle must therefore be:

* acquire only after input activation has created the trie node;
* clear before input deactivation may prune that node;
* never own a target output or dynamic child;
* fall back to normal route resolution for passive or structural selectors
  until they have an equivalent stable route-owner contract; and
* update the existing ``observed`` object in place on every topology event,
  rather than adding a per-read generation poll.

The first implementation should remain private to static nodes.  Promotion to
the installed extension SDK requires the RFC amendment, performance evidence,
and installed-consumer proof required by the extension policy.

Readiness checks
~~~~~~~~~~~~~~~~

The common readiness gate creates one root input view.  For a TSB root, it
checks every required slot directly through that shared root.  It must not
reconstruct the root independently for each validity policy.

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
slot-store ownership protocols.  A callback-local view must not keep a dynamic
child alive, bypass an erase, or retain a retired-object side container.

Static child nodes inside a nested graph create their invocation frame only
while a callback is executing.  Their input links may repoint while keys churn;
the next invocation projects the current slot target.  No invocation frame
survives a pause, stop, erase, or slot reuse.

C++ and Python contract
-----------------------

The first-class C++ path is the static-node invocation frame described above.
The public selector types and wiring syntax do not change.

The Python bridge keeps its existing C++ ``TSInputView`` handling.  Python
wrapper reuse and retained-view expiry are unchanged by the native static-node
invocation frame.

No new Python-only semantic or configuration switch is introduced.

Compatibility and ABI
---------------------

The node, TSData, input-view, pointer, and static-node storage ABIs do not gain
fields.  Public selector and wiring APIs do not change.  Existing retained-view
and binding behavior is preserved.

There is no serialization or wire-format change.  Prepared views and route
handles are derived runtime state and are never recorded.

Performance and memory goals
----------------------------

* Construct at most one input root and scalar bundle per static callback.
* Project canonical input slots directly from the shared root rather than
  rebuilding a shape-specific bundle wrapper for each selector.
* Preserve the existing active-route handle on ordinary value ticks.
* Add no planned storage or persistent borrowed state.
* Keep dynamic keyed collection and nested-graph lifecycle allocation
  behaviour unchanged.

Implementation stages
---------------------

1. Share one root in the common readiness gate.
2. Add a stack-local frame for native static-node callbacks.
3. Stop terminal node-endpoint notification after root tracking is complete.
4. Apply the same invocation-local root sharing to Python and system-node
   paths only where profiling shows remaining reconstruction.
5. Design topology-aware stable input handles in a separate RFC amendment,
   after profiling the remaining path and proving bind, invalidation, map,
   mesh, and installed-SDK behavior.  *Done: the stage 5 amendment at the end
   of this document.*

Alternatives considered
-----------------------

Caching a ``TSInputView`` for every input slot, or retaining a root view in
planned node storage, was rejected for this stage.  It adds per-node memory and
makes dynamic-child and source-invalidation lifetime harder to audit.  The
invocation-local frame captures most of the repeated construction without
altering that ownership boundary.

Adding cached ops pointers to ``NodeView`` or another word to the common
type-erased pointers was rejected because it taxes every cold and hot use.  A
stack-local invocation frame can cache the same information only while needed.

Polling a route generation on every property read was rejected.  It replaces
one repeated validation with another and ignores the existing notification and
active-observer design.  A later route optimization should expose the handle
which that observer protocol already updates, not add a parallel validity
protocol.

Acceptance criteria
-------------------

The implementation requires:

* native public-wiring tests for one and multiple inputs, passive and unchecked
  inputs, TSB/TSL projections, scalar configuration, and lifecycle-only
  selectors;
* existing route tests for bind, rebind, sampled rebind, unbind, and
  forwarding/REF paths;
* map and mesh key erase/reuse tests proving callback-local views do not alter
  child-slot lifetime;
* Python parity tests for direct and paired fast-compute inputs, retained
  wrapper expiry, and reference repointing;
* the complete C++ and non-WIP Python 3.14 suites on macOS and Linux; and
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

Both hosts ran the same command, reusing the unchanged released-hgraph C++
baseline:

.. code-block:: console

   uv run python benchmarks/orchestrate.py \
       --suite core --suite diagnostic \
       --mode upstream-cpp --mode hg-cpp --samples 5

The following representative full-pack medians are in milliseconds.  Positive
change means the post-change implementation is faster.  The final column is
the ``hg_cpp / legacy C++`` throughput ratio before and after the change.

macOS
~~~~~

.. list-table::
   :header-rows: 1

   * - Workload
     - Before
     - After
     - Change
     - Legacy-relative throughput
   * - Native feedback loop
     - 32.815
     - 31.809
     - +3.2%
     - 2.09x -> 2.16x
   * - Python compute chain
     - 23.343
     - 23.835
     - -2.1%
     - 1.24x -> 1.22x
   * - Scheduler fan-out
     - 106.875
     - 101.636
     - +5.2%
     - 1.79x -> 1.88x
   * - Scheduler fan-in
     - 142.992
     - 143.039
     - 0.0%
     - 1.80x -> 1.80x
   * - Python generator boundary
     - 9.155
     - 8.250
     - +11.0%
     - 1.70x -> 1.89x
   * - Python sink boundary
     - 13.186
     - 12.230
     - +7.8%
     - 1.17x -> 1.26x
   * - Integer arithmetic
     - 16.046
     - 15.114
     - +6.2%
     - 1.89x -> 2.01x
   * - Partial TSB updates
     - 37.535
     - 36.284
     - +3.4%
     - 1.76x -> 1.82x
   * - Dense TSD map
     - 85.673
     - 86.451
     - -0.9%
     - 1.88x -> 1.87x
   * - Ordered fixed-TSL reduce
     - 23.813
     - 23.101
     - +3.1%
     - 2.03x -> 2.10x
   * - Mesh with key churn
     - 23.528
     - 23.240
     - +1.2%
     - 4.02x -> 4.07x
   * - Native reference service
     - 13.035
     - 11.363
     - +14.7%
     - 1.75x -> 2.00x

Linux
~~~~~

.. list-table::
   :header-rows: 1

   * - Workload
     - Before
     - After
     - Change
     - Legacy-relative throughput
   * - Native feedback loop
     - 64.229
     - 59.961
     - +7.1%
     - 1.50x -> 1.61x
   * - Python compute chain
     - 45.248
     - 41.822
     - +8.2%
     - 0.89x -> 0.97x
   * - Scheduler fan-out
     - 213.362
     - 197.764
     - +7.9%
     - 1.18x -> 1.28x
   * - Scheduler fan-in
     - 276.405
     - 255.661
     - +8.1%
     - 1.19x -> 1.28x
   * - Python generator boundary
     - 16.016
     - 14.420
     - +11.1%
     - 1.39x -> 1.54x
   * - Python sink boundary
     - 23.628
     - 21.630
     - +9.2%
     - 0.94x -> 1.03x
   * - Integer arithmetic
     - 29.197
     - 25.828
     - +13.0%
     - 1.44x -> 1.63x
   * - Partial TSB updates
     - 64.819
     - 60.900
     - +6.4%
     - 1.40x -> 1.49x
   * - Dense TSD map
     - 161.162
     - 157.114
     - +2.6%
     - 1.25x -> 1.28x
   * - Ordered fixed-TSL reduce
     - 43.291
     - 39.299
     - +10.2%
     - 1.50x -> 1.65x
   * - Mesh with key churn
     - 34.734
     - 34.693
     - +0.1%
     - 4.51x -> 4.52x
   * - Native reference service
     - 24.000
     - 19.966
     - +20.2%
     - 1.30x -> 1.56x

All 56 comparable scenarios completed on both hosts.  No unaffected workload
had a repeatable regression above five percent.  The macOS sparse-reduce cell
initially measured -7.1 percent in the full five-sample pack, but its samples
contained a high outlier; a 15-sample rerun measured 13.387 ms versus the
13.137 ms pre-change median (-1.9 percent).

The implementation deliberately stops at callback-local preparation.  It does
not add persistent input handles or change ``TSInputView`` behavior.  The
stable observed-handle design above remains a separately measured follow-up.

Stage 5 amendment: stable prepared slots (2026-07-29)
-----------------------------------------------------

Trigger evidence
~~~~~~~~~~~~~~~~

The condition this RFC set for stage 5 — "if profiling after this change
still identifies target resolution as a material fixed cost" — is met.  A
profile of merged main (revision ``0e161e5a``, after stages 1–4, the lifted
single-projection change, and the observer-notify inline) on the Linux host
shows the per-tick input substrate as the top of the pure-native profile:
``input_child_projection`` 6.0 percent, ``resolved_target_at_path`` 2.3
percent, ``target_link_resolve`` 0.9 percent, and the TSB projection wrappers
(``at`` / ``size`` / ``as_bundle`` / ``indexed_child_at``) about 5.3 percent
combined.  The cost is structural: ``input_at`` resolves the slot's target
once and discards the resolution, then every subsequent property read on the
returned view re-resolves the route (``resolved_value_data`` reassigns its
cached target view unconditionally at a target position), so a typical
``modified()``-then-``value()`` tick performs the full resolution two to
three times.

Relaxations
~~~~~~~~~~~

Two stage 1–4 constraints cannot hold for a slot that keeps a pointer across
ticks, and are relaxed **for static nodes only**:

* "Add no planned storage or persistent borrowed state" now binds stages 1–4
  alone.  Stage 5 adds one planned static-node storage field: a fixed-size
  prepared-slot array sized by the implementation's canonical input-slot
  count.
* "The … static-node storage ABIs do not gain fields" is likewise scoped to
  stages 1–4.  The prepared-slot field is private to the static-node path
  (``NodeStorageField``, with the array offset captured by the node's own
  callbacks); it is not part of the installed extension SDK, whose promotion
  rules from the original text still apply.  The input-view cursor also gains
  one non-owning route pointer (``TSInputView`` grows from eight to nine
  words; the pointer is null everywhere outside the prepared path), which is
  what lets ordinary property reads resolve through the stable handle instead
  of re-running route resolution per read.

Prepared-slot contract
~~~~~~~~~~~~~~~~~~~~~~

A prepared slot exists per canonical **actively observed value input** of a
node whose front-end plans the route array — the static-node builder and
the lifted-kernel builder, which share the machinery in
``runtime/prepared_input_routes.h``.  Per-tick consumers (issue #203 extended
the original three): the static-node invocation frame, the lifted-kernel
child projection, the common readiness gate, the python fast-compute cache
(one route per ts-arg below the args root, acquired at node start), and the
retained capture views of the service and shared-output nodes
(``TSInputView::routed_child_at`` — a retained view carrying its route
resolves reads through the trie handle).  All other node kinds keep the
existing projection path unchanged.  Each slot holds exactly two non-owning words of route
state:

* a pointer to the slot's active-trie root node
  (``TSInputTargetActiveNode``), whose ``observed`` output handle the
  runtime already replaces in place on every topology event; and
* the slot's target-link storage reference, from which the input cursor is
  reconstructed so that sampled-rebind blending, structural-transition
  checks, and link-local tracking reads keep their existing semantics.  The
  prepared slot removes the per-callback projection walk and the per-read
  route re-resolution; it must not bypass the cursor.

Lifecycle:

* **Acquire** in a framework-supplied ``start`` callback.  Input activation
  (``activate_input_slots``) runs before the user ``start`` hook, so the trie
  node exists; static nodes therefore register start/stop callbacks
  unconditionally, not only when the implementation declares them.
* **Clear** in the framework-supplied ``stop`` callback.  Deactivation runs
  after the user ``stop`` hook, so the trie node is still live at clear time.
  Nested static children acquire and clear on every child start/stop, which
  keeps dynamic-slot reuse correct without new bookkeeping.
* **Validity per use** re-checks the full trust condition that
  ``resolved_target_at_path`` requires — locally active, value-kind
  observation, bound.  Source invalidation and ``make_passive`` clear
  ``observed`` in place while the trie node stays alive; a runtime
  ``make_structural_active`` swaps in a *bound structural* handle on the
  same node, which the kind check rejects (a bound-only check would expose
  structural storage as the value route).  Any failed condition falls back
  to full resolution.  No generation counter is added (the alternative
  already rejected above).

Non-pruning becomes a contract.  The active-trie pruning helpers
(``try_prune_child`` / ``try_prune_active_root``) are dead code today; stage
5 removes them and records the invariant they contradicted: an active-trie
node, once created, lives until its owning target-link storage is destroyed
or assigned over.  Storage *move construction* preserves trie-node addresses
(the tree is repointed in place); copy-assignment and move-assignment into a
populated link destroy the tree, and both happen only during build-time
embedding, before any acquire.

Fallbacks (unchanged behaviour)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The stable path applies only where the runtime already trusts the observed
handle: locally active, value-kind observation, bound.  Everything else keeps
the existing resolution path, routed at compile time from the selector's
policy pack (``InputActivity``) with no runtime test:

* passive inputs (never activate, no observed handle);
* structural selectors (their observed handle is a derived structural view,
  not the value position);
* slots whose projection carries no target link (owned storage); and
* from-REF alternatives and mid-transition reads, which the reconstructed
  cursor already handles because the prepared slot retains the link storage
  reference.

Acceptance
~~~~~~~~~~

The original acceptance criteria apply unchanged, with emphasis on: existing
bind/rebind/sampled/unbind/forwarding route tests; map and mesh key
erase/reuse (child start/stop must re-acquire cleanly); source-invalidation
tests proving the bound-state fallback; and five-sample before/after packs on
both hosts with the ``hg_cpp / legacy C++`` ratio recorded.

Focused pinned Linux medians (Core Ultra 7 155H, seven samples per side,
against main ``a5d1a9b5``): scheduler fan-out +21.0 percent, partial-TSB
+8.2, dense TSD map +6.7, integer arithmetic +6.5, native tick +5.4;
python-boundary shapes neutral.  Nodes outside the planned-route front-ends
pay the one-word-wider input view without gaining the cache — measured
-1.7 percent on the native request-reply service scenario, inside the five
percent investigation bar; extending route planning to the erased node
front-ends is the natural follow-up if that cost matters.
