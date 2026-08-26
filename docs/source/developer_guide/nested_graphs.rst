Nested Graphs
=============

This page is the design record for **non-flattening nested graphs** — the
substrate and operators (``nested_``, then ``switch_`` / ``map_`` / ``reduce``)
where a node owns and drives a child graph at runtime instead of inlining it at
wiring time. It builds on the runtime nested-graph node described in *Graph
Wiring > Nested graphs* and supersedes the prior C++ attempt's design where the
two conflict (see *Reconciliation with the 2603 RFC* below).

Scope of this page: the wiring-side sub-graph compilation and the runtime
design decisions for the nested operators implemented so far. Sections are added
in the same change as the code they describe.

``nested_<G>`` is a C++ authoring primitive, not a generic Python API. Python
constructs that require nested execution, such as error handling and
higher-order operators, lower to registered C++ operators that compile and own
their child graphs. The bridge exposes those public constructs rather than a
second, user-facing ``nested_graph(...)`` mechanism.


The model
---------

Three artifacts, one per phase:

- **Sub-graph definition** (authoring): an ordinary graph struct — a static
  ``compose(Wiring &, Port..., Scalar...)`` body. The same definition can be
  inlined by ``wire<G>`` (flattening) or compiled by ``compile_subgraph<G>``
  (non-flattening); nothing in the definition chooses.
- **``CompiledSubGraph``** (compile-time template;
  ``include/hgraph/types/subgraph_wiring.h``): the ranked child ``GraphBuilder``
  plus the **boundary binding specs** (``NestedGraphInputBinding`` /
  ``NestedGraphOutputBinding``) and boundary schemas. Produced once at wiring
  time; owned by the nested node's (interned, program-lifetime) context. This is
  the C++ realisation of the RFC's *child-graph template*.
- **Child graph instance** (runtime): a ``GraphValue`` created in caller-owned
  memory by the external-storage overload of ``make_nested_graph``. ``nested_``
  owns exactly one planned region; ``switch_`` owns two fixed regions (one
  active and one stopped previous instance); keyed operators own stable dynamic
  slots. There is no separate instance class — per-instance state is the
  operator's own storage struct.


Storage sizing and lifetime
---------------------------

``GraphBuilder::nested_storage_layout()`` returns the exact size and alignment
of one child graph, including its runtime header, nodes, node state, inputs and
outputs. Nested operators use that layout rather than allowing ``GraphValue``
to allocate its payload independently:

- ``nested_`` adds one raw child region to the node's ``StoragePlan``;
- ``switch_`` adds an array of two raw regions, each using the maximum size and
  alignment of all possible branches;
- ``map_`` and ``mesh_`` compute one stable slot layout containing the entry
  header followed by an aligned child region. Capacity growth appends blocks,
  preserving every existing entry and graph address;
- dynamic ``reduce`` uses two such positional banks. Capacity growth builds the
  reshaped tree in the inactive bank while the stopped old bank remains alive
  through the engine cycle.

The fixed cases therefore contribute their full child memory to the parent
graph's up-front static storage plan. A keyed operator cannot know its eventual
key count at wiring time, but it knows the exact cost of each instance and
allocates capacity in blocks rather than once per entry and once per graph.
Vectors and hash indexes still grow dynamically; the pre-allocation guarantee
covers graph/node payloads and stable key slots, not an unbounded key set's
bookkeeping.

Logical removal and physical erase are deliberately separate. Logical removal
clears forwarding/output bindings and stops the child while all producers are
alive. The slot remains constructed for the rest of the engine cycle. A later
slot ``erase`` invokes the entry destructor, whose ``GraphValue`` destroys the
in-place child before the raw slot can be reused. ``switch_`` expresses the
same rule with its active/previous A/B slots; ``reduce`` expresses it with its
active/previous banks.

Map normally mirrors the authoritative key-set slots directly. If the bound
key-set handle itself is replaced, the replacement may reuse the same slot ids
before the old handle can emit erase callbacks. Map therefore alternates two
``InPlaceGraphSlotStore`` banks for that case: it stops and retires the old
bank, builds the replacement in the inactive bank, and destroys the retired
bank only on a later evaluation time. This is the dynamic counterpart of the
switch A/B protocol and does not introduce per-entry ownership allocations.

Steady-state nested evaluation avoids temporary pointer bookkeeping. Boundary
paths are traversed as spans, forwarding-chain cycle detection uses constant
storage, mesh reuses its rank-order vector after capacity growth, and the
currently evaluated mesh key is a borrowed ``ValuePtr``. Event-driven key,
dependency, and capacity growth can still allocate; scanning an unchanged
nested graph does not.


Boundary compilation: placeholders, not stubs
---------------------------------------------

A sub-graph's time-series parameters are compiled as **boundary placeholder
sources** — a ``WiringPortRef`` source kind (``BoundarySource{arg_index, path}``)
alongside peered/structural/null. ``compile_subgraph<G>`` runs ``G::compose``
against a fresh ``Wiring`` whose ``Port`` parameters carry boundary sources;
``Wiring::finish_subgraph`` then runs the ordinary rank pass and converts every
boundary-sourced input edge into a ``NestedGraphInputBinding``
(``source_path = {arg_index} + path`` from the outer node's input root,
``target`` = the child node input endpoint). The returned output port becomes the
``NestedGraphOutputBinding`` (root forwarding).

**No stub nodes exist at any layer.** The 2603 RFC's Option B (keep wiring-stub
nodes, compile them away) assumed Python-side stub wiring nodes; since the C++
wiring core owns boundaries, we adopt the RFC's own "cleanest end state"
(Option C) directly: boundaries are wiring-only values, compiled into binding
specs at ``finish_subgraph``.

A boundary source composes with the rest of wiring uniformly: it can feed any
child node input directly or appear as a child of a structural source (the
binding target path is extended into the structural slot).

**Pass-through outputs (``alias_parent_input``).** A sub-graph ``compose`` may
return a boundary input directly. This compiles to the ``ParentInput`` output
binding kind: at runtime the outer node's forwarding output aliases whatever
upstream output the outer *input* is bound to (re-resolved each cycle, cleared
while the upstream is unbound). This is the RFC's ``alias_parent_input`` mode
and is what identity branches of ``switch_`` / ``map_`` and component-style
wrappers rely on.

**Forwarding links are transparent parents.** Views projected THROUGH a
forwarding link (a nested map's output operating on its element, a child
reached by ``at_slot`` on a link-backed dictionary) stamp the accessing
link view as the child's ``TSParentLink``. A child-modification
notification arriving at a link therefore delegates to the TARGET: the
target's per-slot delta bits, its own tracking record, and its parent
chain all fire exactly as if the child had been reached through the
target directly, while the generic notify path separately records the
link's own tracking and continues the link's chain (node scheduling).
Without this delegation a nested container's writes join the outer
delta window structurally but never mark the inner modified surface -
the nested map-in-map case (TSD elements that are themselves TSDs,
enabled by the storage-stability ruling) depends on it.

**Outer-port capture (auto-import).** A sub-graph compose body - most
commonly a Python lambda handed to ``map_``/``switch_`` - may reference
ports of the ENCLOSING wiring (closure capture: ``map_(lambda x: x + c,
ts)`` where ``c`` is an outer port). During ``finish_subgraph``, a
peered source whose producing node does not belong to the sub-graph's
wiring is a CAPTURE: it converts to a fresh boundary argument appended
after the declared inputs (deduplicated by source identity), and the
compile result reports the captured outer ports in
``CompiledSubGraph::captured_inputs`` (parallel to the appended
boundary indices). The CALLER binds them: ``nested_`` / ``try_except_`` append
ordinary inputs, branch operators retarget them onto shared switch/dispatch
slots, and ``map_`` / ``mesh_`` append pass-through (broadcast, never
multiplexed) inputs. Captures are deduplicated against existing outer slots.
Named contexts use this same mechanism when a child lookup resolves a port
published by an enclosing ``Wiring``. Fixed structural captures preserve their
shape and cross leaf by leaf. A plain ``finish`` (root graphs) reports a
foreign source as an error - captures only make sense across a sub-graph boundary.
Captured producers contribute no rank edge inside the child (they rank
in the OUTER graph; the child sees them as boundary inputs).

**Structural boundary args.** An outer structural initializer
(``nested_<G>(w, {a, b})``, or named TSB form) is mirrored into the child
compile as a structural source whose **leaves are boundary refs**. The child
consumer's input endpoint therefore derives as non-peered with bindable leaf
slots, and ``finish_subgraph`` emits one leaf-wise input binding per peered
leaf (null leaves stay unbound) — the runtime binds each leaf position, which
is the only bindable granularity (``is_bindable`` = target-link position).

**REF adaptation uses endpoint negotiation, not another binding mode.** Input
and output schema compatibility is REF-transparent.  If an outer plain source
feeds a child ``REF`` parameter, the outer nested-node input requests the
declared ``REF`` schema and the normal to-REF output alternative supplies the
boundary handle.  The inverse path requests a plain schema from an outer
``REF`` source and receives the normal from-REF alternative.  Structural
initializers targeting a ``REF`` first compose into the same structural REF
node used by ordinary wiring.  Child-produced ``REF`` outputs are forwarded
unchanged; a plain outer consumer performs the inverse negotiation on that
forwarded endpoint.  Rebinding therefore retains the standard sampled,
EMPTY, and target-tick semantics without a nested-graph-specific reference
object or copy.

An idle child may still be on its previous evaluation time when a reference
retargets during a later parent cycle.  Nested schedule push delegation clamps
that notification to the parent's current time before recording it in either
graph.  This makes the newly sampled target visible in the current cycle and
prevents a stale child clock from attempting to schedule the parent in the
past.

Rejected explicitly at compilation, with a clear error:

- error / recordable-state roots as the sub-graph output;
- structural (non-port) sub-graph *outputs*;
- seeding ``GlobalState`` inside a sub-graph ``compose`` — nested graphs
  delegate global state to the **root** graph at runtime, so a sub-graph-seeded
  store would be silently discarded; seed it on the outer wiring instead.


``nested_<G>`` — the wiring entry
---------------------------------

``nested_<G>(w, ports..., scalars...)`` (compose-parameter order, same call
shape as ``wire<G>``, including ``{…}`` structural initializers) compiles ``G``
and adds **one** ``single_nested_graph_node`` that owns the child graph:

- outer node input schema = un-named TSB over the boundary arg schemas (field
  per arg, in order); output schema = the sub-graph output schema (forwarding
  at the output root, or the ``ParentInput`` alias for pass-through);
- scalar arguments are baked into the compiled child nodes (they configure the
  child graph), and additionally folded into the outer node's interning key —
  so two ``nested_<G>`` calls with equal inputs **and** equal scalars dedup to
  one nested node, distinct scalars do not. Interning is **lazy**: the child
  ``GraphBuilder`` and its program-lifetime node context are only created on an
  intern miss (the deferred-builder ``Wiring::add_node`` overload), so deduped
  calls leak nothing;
- a sink sub-graph (``compose`` returns ``void``) wires as a nested sink node
  (not interned, like every output-less node).

Scheduling follows the existing substrate: child start can schedule the parent
from the child's cached ``next_scheduled_time()``, and child evaluation
propagates the cached next time to the parent before the nested evaluation
block returns. A source-only sub-graph therefore drives the outer graph
(proven by ``tests/cpp/test_nested_wiring.cpp``). Child-driven **push**
propagation covers a child node scheduled by notification while the parent is
idle.

Tests: ``tests/cpp/test_nested_wiring.cpp``.


Higher-order constructs are operators
-------------------------------------

``reduce``, ``switch_`` and the current ``map_`` subset are **ordinary
operators**, not bespoke wiring functions. This mirrors the ``release/0.5``
direction (``map_`` is an ``@operator`` whose old implementation became the
default registered overload): the default implementation is one registry
candidate, the future dynamic kernels (e.g. ``reduce`` over ``TSD``) are
further overloads of the same name, and user specialisations — including ones
gated on the supplied function's identity via ``requires_`` — are selected by
the standard best-match machinery. The callable argument is the ``WiredFn``
scalar (``fn<X>()``; see *Operators > Higher-order operators*). Their markers
and default overloads live in their own ``lib/std`` family files
(``operators/higher_order.h`` + ``impl/higher_order_impl.h``) — there is
nothing special about them now that sub-graph compilation is standardised.


``reduce``
----------

``reduce(func, ts[, zero])`` reduces a time-series **collection** into one
time-series with the (associative) combiner — any wirable function: an
operator (``add_``), a node, or an ``(lhs, rhs)`` sub-graph (flattened at
every reduction node). The operator's contract covers any collection kind
(``TSL`` / ``TSD``; ``TSS`` once supported) — each kind is its own registered
overload selected by pattern rank. Fixed and dynamic ``TSL`` plus ``TSD`` use
the same native associative reduction model.

The associative forms have two arities. The number of **currently valid/live
values**, not a collection's capacity, determines the result:

- ``reduce(func, ts)`` supplies no zero. Empty input remains invalid, a
  singleton returns that value without evaluating ``func``, and two or more
  values are reduced with ``func``;
- ``reduce(func, ts, zero)`` wires the scalar as ``const(zero)`` at the element
  schema (a live time-series zero is also supported). Empty input publishes
  ``zero``; a singleton evaluates ``func(value, zero)``; with two or more live
  values, only those values are reduced and ``zero`` is not an operand.

.. note:: Compatibility deviation

   The previous Python implementation inferred an operation-specific zero when
   it was omitted and used zero/default leaves for unset fixed-``TSL`` slots.
   hg_cpp deliberately does neither. Omission means **no zero**, and unset list
   slots do not participate. Code that requires an empty value must supply one
   explicitly and account for its documented singleton application.

   The same valid-value rule applies to dynamic ``TSD`` inputs. A mapped key
   can be live before its child has produced a valid value (a phantom slot).
   hg_cpp reduces the valid subset and may therefore publish an intermediate
   aggregate; released hgraph can remain invalid until every live keyed slot
   has a value. Once those values are valid, the aggregates are identical.
   This avoids making results depend on keyed capacity or service latency and
   is an accepted deviation (issue #95), pinned by the mapped-service tests and
   the bounded ``valid-subset-reduce`` parity-family relation.

``reduce(func, ts, zero_ts, is_associative=false)`` selects the ordered form.
It requires a live time-series ``zero_ts`` and always lays out a left fold,
including for four or more elements. Invalid positions still use
``default(ts[i], zero_ts)``; once an element is valid, changes to ``zero_ts``
do not disturb that position.

Element access works on any source kind: a peered output path, a structural
child, or a **sub-graph boundary** (so ``reduce`` composes inside a sub-graph
``compose`` over a boundary TSL). Compatible scalar lifted kernels retain a
single-node fast path for fixed ``TSL`` inputs; all other associative forms use
the runtime tree below. The fast path is a **full O(N) recompute per tick**
(no modified gating) with O(1) retained state, whereas the tree is incremental
with up to N−1 live combiners — the fast path trades per-tick work for zero
nested-graph machinery, which wins at the fixed sizes it is selected for.

Tests: ``tests/cpp/test_reduce.cpp`` (including a user overload gated on the
wired function's identity, mirroring ``release/0.5``'s ``test_map_overload``).


Associative ``reduce`` runtime
------------------------------

The native kernel is one **runtime node** owning a balanced binary tree of
combiner child graphs over the live keys of a ``TSD`` or valid children of a
``TSL``. This section is derived from the earlier 2603 design record and
reconciled to the current substrate; where the two differ, this section is
authoritative.

**Vocabulary reconciliation** (2603 → current): ``ChildGraphTemplate`` /
``ChildGraphInstance`` → ``CompiledSubGraph`` (compiled once at wiring from
the ``WiredFn``, two boundary args ``(lhs, rhs)``) / ``GraphValue`` via
``make_nested_graph``; ``OutputLink`` root publication → the node's
**forwarding output** + ``bind_forwarding_output_to_source``;
``StablePayloadStore`` + per-graph slabs → two stable in-place combiner banks;
``MapViewDispatch`` slot observers → **current-key
reconciliation** when the TSD input modifies, re-points, becomes invalid, or
is first observed. Dense leaf positions intentionally do not mirror source key
slots: compacting the leaves is what keeps the live combiner count at ``n-1``.

**Core decisions kept from 2603** (the load-bearing ones):

- **Leaves are aliases, not child graphs.** A leaf position references a live
  source element output (``tsd.at(key)``); only **internal combine points**
  instantiate the combiner child graph, and only when **both** subtrees are
  non-empty, except for the singleton root described next. ``n`` live values
  normally cost at most ``n - 1`` live combiners.
- **``zero`` is only an empty/singleton input, never tree padding.** With no
  live values the output forwards to a supplied zero or remains unbound. With
  exactly one value and a supplied zero, the root owns one combiner bound as
  ``func(value, zero)``; when zero is omitted the output aliases the value and
  no combiner exists. With two or more values the zero is completely excluded
  from the tree. A zero tick therefore schedules work only in the singleton
  state.
- **Dense leaves over a power-of-two capacity.** ``key ↔ dense leaf``
  mappings; erasing a key moves the **last** live leaf into the vacated
  position (bounded rebalancing). Internal nodes are heap-indexed
  (``0`` = root, children of ``i`` at ``2i+1`` / ``2i+2``), so deeper nodes
  have higher indices.
- **Aggregate resolution** (per position): empty leaf → ``Empty``; live leaf
  → ``Leaf``; internal: both children empty → ``Empty``, exactly one
  non-empty → alias that child's aggregate, both → ``Node`` (the combiner).

**Binding discipline (the map_ lesson, restated):** value ticks normally flow
through standing bindings: a leaf tick notifies its combiner's child node
directly (the input is bound to the upstream element output), the combiner's
output tick notifies its parent combiner, and the cascade resolves within one
evaluation pass by processing combiners **deepest-first** (descending heap
index — a parent has a lower index than its children, so it evaluates after
them). When the TSD root modifies or re-points, v1 conservatively reconciles
the current key set and refreshes live combiner/root bindings; this covers
ordinary add/remove deltas and forwarding/REF retargets, including same-key
different-source retargets. Combiner evaluation is **unconditional** per
pass, like the other nested operators: a same-cycle notification lands in a
child's schedule array but not its cached next time, and an idle child
evaluate is a cheap scan. Re-binding samples held values (``bind_output``
notifies on valid-source binds), and a root **re-point to an already-valid
source is a tick of the reduce output at the publication time** (the aliased
value changed without the target writing — ``mark_modified`` through the
forwarding link), per the sampled-runtime contract.

**v1 simplifications (recorded, with the refinement path):** structural
events recompute the internal bindings in one ``O(capacity)`` pass (per-path
dirty propagation is the refinement); ``leaf_capacity`` is monotonic
(conservative shrink is the refinement). Structural recompute is **three
phases ordered so a link is never bound to or unbound from a dead target**:
create needed combiners deepest-first (parents can reference fresh
children), bind every live combiner and publish the root while every old
target is still alive, then retire displaced combiners **root-first**
(ascending heap index — a doomed parent unbinds from its still-alive doomed
child; the same teardown-direction reasoning as the ``map_`` storage-plan
ordering, and also the explicit ``ReduceNodeStorage`` destructor order).
Retirement stops the displaced graphs but preserves one previous generation
through the current engine cycle; a later-time reduce evaluation destroys that
generation before reconciling again, after dynamic consumers have moved off the
old root. Capacity growth alternates between two stable banks, so both the new
tree and the stopped previous tree have fixed addresses without per-combiner
heap allocations. At final node disposal the previous generation is destroyed
before the active generation: a stopped retired parent can still retain a target
handle to a surviving current child output, so the retired subscriber must die
before that producer.
When a keyed root first changes source, publication moves to one stable output
owned for the remaining lifetime of the reduce node. The old root is copied
before stop and the stable value is reconciled after the new root evaluates.
This lifetime is required because downstream ``REF`` values can retain child
endpoint identities after the transition cycle; recycling a temporary
snapshot would leave those references pointing into freed slot storage.
Ordinary evaluations copy only the source's modified and removed TSD slots (or
added and removed TSS slots). A later root-identity change performs one full
reconciliation. Same-capacity changes publish only logical differences, while
capacity-bank changes preserve sampled-rebind behavior by marking every live
TSD child modified (a TSS publishes its structural sample).
The node's own output is a forwarding endpoint linking either into a live
combiner output or the field-held stable publication output, so the reduce
field uses the default *before-output* plan placement (the
``nested_``/``switch_`` direction). The forwarding output is destroyed before
the field and therefore detaches before the stable publication storage dies.

**Exception / unwind policy:** reduce rebuild failures are not intended to
recover and continue execution. If child construction, binding, starting, root
publication, or evaluation throws, the exception propagates to the graph
evaluator; an outer error-capture policy may catch it, but the graph is
considered failed and should be stopped. The nested node must still be safe to
stop and destroy after such an unwind: all link mutations and retired-child
handling must preserve pointer stability, keep old targets alive until
subscribers have moved off them, and use rollback/cleanup guards so no input
subscription or forwarding output is left pointing at dead child storage. Future
optimisations to the rebuild path must keep that stop-after-failure safety
property even if they make the key/combiner update more transactional.

**Deferred:** ``TSS`` reduction.

Runtime: ``runtime/reduce_node.{h,cpp}``. Tests: ``tests/cpp/test_reduce.cpp``
(fixed/dynamic ``TSL`` and ``TSD`` cases, including every zero cardinality).


Ordered ``reduce`` over contiguous ``TSD[int, E]``
--------------------------------------------------

``reduce(func, ts, zero, is_associative=false)`` is a distinct native nested
node. ``zero`` is a live time-series initial accumulator ``A`` and ``func`` is
compiled once as ``(A, E) -> A``. This permits the accumulator/output schema
to differ from the collection element schema. Empty input forwards ``zero``;
otherwise child ``0`` consumes ``(zero, ts[0])`` and child ``i`` consumes the
previous child output and ``ts[i]``. Children evaluate from lowest to highest
index so a tick settles the complete suffix in one parent pass.

The current ordered kernel deliberately accepts only a self-contained binary
combiner with a real child output. Combiner captures and pass-through outputs
are rejected at wiring time; express those advanced cases in C++ or make the
additional inputs explicit before reducing.

The input key set must be exactly ``0..n-1``. Negative keys and holes are
wiring/runtime errors rather than an arbitrary dictionary ordering. Python
``TS[tuple[E, ...]]`` uses the existing native enumerated-TSD conversion and
then this same ordered kernel; it is not a second Python reduction runtime.

Each chain generation occupies one ``InPlaceGraphSlotStore`` bank. A length
change builds and binds the replacement chain in the inactive bank, forwards
the outer output to its new tail, then stops the old chain from tail to head.
The stopped generation remains alive through the engine cycle and is destroyed
tail-first on a later evaluation. Stable addresses, stop-before-destroy, and
subscriber-before-producer teardown therefore hold without per-child graph
allocations. Value changes and live-zero ticks use standing bindings and do
not rebuild the chain.

Runtime: ``runtime/ordered_reduce_node.{h,cpp}``. Tests:
``tests/cpp/test_reduce.cpp`` and ``python/tests/test_python_authoring.py``.


Scheduling delegation
---------------------

The RFC's clock invariant — *the parent must wake no later than the child's
next scheduled work* — is realised as two halves folded into the existing
graph ops (no separate engine/clock object; see the recorded decision):

- **Pull** (nested graph evaluation in ``graph.cpp``): child graph evaluation
  maintains a cached ``next_scheduled_time()`` as it scans and evaluates child
  nodes. Before the nested evaluation block returns, it schedules the parent
  node at that cached time. ``single_nested_graph_propagate_schedule`` remains
  for the start path because child start can schedule work before any child
  evaluation block exists.
- **Push** (``nested_schedule_node_impl`` in ``graph.cpp``): any
  ``schedule_node`` recorded on a child graph **while it is idle**
  (``started && !evaluating``) immediately schedules the parent node at the
  same time — the path a notification or wall-clock alarm takes between
  parent evaluations, and the safety net the keyed operators (``switch_`` /
  ``map_``) rely on when a child is not evaluated every parent cycle. The
  push is deliberately gated off during child evaluation (pull covers it, and
  pushing mid-evaluate would schedule a spurious extra parent cycle) and
  while the child is stopped.

The push tells a keyed parent *when* but not *which child*. For ``map_`` and
``mesh_`` that identity matters: both operators keep sparse child worklists,
and a child due purely by its own internal schedule (a service response
delivery, a scheduler alarm) must be merged with slots selected by an outer
tick. The push half therefore also invokes an optional per-child **schedule
observer** (``GraphView::set_child_schedule_observer``, installed at entry
creation with a stable per-entry ``(storage, slot)`` context), which feeds a
per-parent **schedule queue** — a min-heap of ``(when, slot)`` in the keyed
node's storage.

Every evaluation pops due slots into the candidate set; the evaluation loop's
own future child schedules push into the same queue (the pull half). At the end
of the evaluation the queue's minimum re-arms the parent once, including
deadlines belonging to children outside that cycle's sparse candidate set.
Repeated pull-side observations of the same child deadline are coalesced;
push-side observations remain distinct because different internal nodes may
schedule different times. Queue entries are otherwise lazy: a rescheduled,
stopped, or removed slot pops harmlessly because both the current per-child
deadline and the child's due-ness are re-checked. Current-cycle entries
observed after the initial queue drain are discarded before the parent is
re-armed, so they cannot hide a later deadline.

``map_`` orders its sparse candidates by stable slot. ``mesh_`` rematerializes
and sorts only its candidate slots by dependency rank on each settle pass;
newly created dependencies and same-cycle sibling notifications join that
worklist before the next pass. A source repoint or structured boundary
retarget retains the conservative all-child rebind path. ``tsl_map`` continues
to run per-child due checks over all fixed entries and needs neither observer
nor queue.

Multi-level nesting recurses up to the root naturally. The boundary *binding*
helpers shared by all nested node implementations
(``walk_ts_path`` / ``bind_input_to_source`` /
``bind_forwarding_output_to_source``) live in
``include/hgraph/runtime/nested_bindings.h`` — no bespoke bind/unbind logic
per operator.

An evaluating nested graph always has a parent node. Missing parent state is a
construction/test setup error, not a scheduling case to branch around in the
nested evaluation path.


``switch_``
-----------

``switch_(key, cases[, ts])`` routes through **one** child graph at a time,
selected by ``key`` — an operator like the rest of the family
(``wire<stdlib::switch_>(w, key, switch_cases({{k, fn<G>()}, …}[, fn<Default>]),
ts…)``).

- **Branches are ``WiredFn`` values** (graphs, nodes, or operators). Each is
  compiled into a ``SingleNestedGraphNodeSpec`` via the function's **compile
  thunk** (``WiredFn::compile`` — the same value either inlines via ``wire``
  or compiles into a child graph; the caller chooses). All branches must
  produce the same output schema after dereferencing. If branches differ only
  in root ``REF``-ness, the switch output takes the ``REF`` shape.
- **The key is just a boundary input.** The outer switch node's inputs are
  ``[key, ts…]``. A branch consumes outer input ``0`` as the key only when its
  first parameter is named ``key``; non-key branch binding paths simply shift
  past that outer input. Source-style branches (arity 0) take no bindings at
  all.
- **Runtime** (``runtime/switch_node.{h,cpp}``, on the shared
  ``nested_bindings`` helpers): two caller-owned graph-memory slots are
  allocated once, each sized and aligned for the largest branch. On a key
  change (or any key tick with ``reload_on_ticked`` exposed as
  ``switch_cases(...).reload()``), the inactive slot is reused for the new
  branch (else the default, else a runtime error), the active branch is stopped,
  and the new branch is bound and started. The stopped branch remains in the
  previous slot until the following switch, when it is destroyed before that
  slot is reused. At most one child is running.
- **Output ownership**: the switch has one fixed node output. Ordinary branch
  terminals are peered into it. For a VALUE branch under a ``REF``-shaped
  switch, the branch terminal instead owns its value inside its A/B graph slot
  and the switch publishes a reference to that terminal. No secondary backing
  output is allocated.
- **Sampled semantics** (the sampled-runtime contract; the recorded
  divergence from Python's ``value = None`` reset): the freshly selected
  branch evaluates with the *current* upstream values even when they did not
  tick that cycle. ``switch_`` starts the branch to establish its declared and
  custom input activity, binds each already-valid boundary through an explicit
  sampled target link, then schedules the affected consumers once at the switch
  time. Nested ``TSD`` / ``TSS`` descendants expose their full current
  structure during that evaluation. Input activation itself only establishes
  subscriptions and never schedules or changes ``modified`` state. Passive
  held inputs remain silent. Pinned by tests for both the active and passive
  cases.
- **Lifecycle**: switching away stops the child ``GraphValue`` but preserves
  its storage as the previous instance. The next switch destroys that previous
  instance and constructs the new branch in the same slot. Switching back
  therefore creates fresh per-branch state (pinned by test) without allocating
  another graph block. Node stop stops the active child; normal node-storage
  disposal destroys both slots. The output resolver discovers the schema by
  compiling the first branch (``resolve_default_types`` on the overloads).
- ``switch_(key, cases, *ts, **kwargs)`` takes any number of positional
  and **keyword** time-series arguments (see *Operators > Variadic operator
  parameters* and *Named arguments…*). Branches bind positional args in
  order (optionally preceded by the key) and keyword args **per branch by
  the branch's own parameter names** (``NamedPort`` / ``In<"name">`` —
  branches may declare the same names in different orders, like Python).
  Deferred: all-sink switches.

Tests: ``tests/cpp/test_switch.cpp``.


``map_``
--------

At runtime, ``map_(func, *args, **kwargs)`` owns **one child graph instance per
key or dynamic-list index** of its multiplexed ``TSD`` / dynamic ``TSL``
input(s) — an operator like the rest of the family
(``wire<stdlib::map_>(w, fn<G>(), tsd_port[, ts…])``).

- **Key lifecycle is driven by ``__keys__: TSS[K]``**. Wiring supplies either
  the explicit ``__keys__`` argument or an inferred key set from the
  multiplexed inputs. The runtime treats the key-set slot layout as
  authoritative: first bind and source re-point rebuild from live key slots;
  ordinary key ticks create or destroy children from ``slot_added`` /
  ``slot_removed``. Multiplexed TSD membership only affects element binding,
  not child existence. An invalid or unbound ``__keys__`` source stops all
  children and clears the owned output.
- **Per-key state** lives in an ``InPlaceGraphSlotStore`` indexed by the
  current ``__keys__`` slot id. Each stable slot contains the entry header and
  the aligned child graph payload; the entry owns the key ``Value``, an
  optional per-key ``TS<K>`` source output that reads that owned key directly,
  and the child ``GraphValue`` handle. Capacity callbacks reserve matching
  payload blocks before new source slots are used. The owned key is used for
  output erasure across removals and re-points. Entry member order is
  load-bearing: the child graph (subscriber) tears down before the key source
  it observes. A direct key-source handle replacement alternates two stores so
  the stopped old generation survives the replacement cycle even when the new
  source reuses its slot ids immediately.
- **Child boundary args** are sourced per ordinal (``MapArgSource``): the
  **element** binds to the parent TSD input's bound output child *at the
  entry's key*; if that key is absent from a multiplexed TSD, the child input
  is left unbound until the key appears there. The **key** (when ``func``'s
  first parameter is NAMED ``key`` — ``ndx`` on TSL maps; the Python rule,
  applied per branch on ``switch_`` too; ``arg<"__key_arg__">(Str{…})``
  renames it, ``""`` disables — arity detection is gone) binds to an
  entry-owned read-only ``TS<K>`` key source; **broadcast** args bind whole to
  the corresponding outer input. Any outer input re-point refreshes the
  existing child bindings through the shared ``nested_bindings`` helpers.
- **Output (write-through, no copy)**: the output is an *owned*
  ``TSD<K, OUT>`` — for every key a **real element is instantiated** in it
  (``TSDDataMutationView::operator[]`` at entry creation), and the child's
  terminal node is built with a **forwarding output endpoint**
  (``NodeBuilder::output_endpoint`` override, set by the operator on the
  compiled template's terminal) that the map node binds onto the parent's
  element. The child node then **writes the parent's storage directly**
  through the link (``target_link_copy_value_from`` resolves the target and
  runs the standard mutation path, so modified tracking and dict parent
  recording are exactly the owned-write path). Bindings are established at
  entry creation and refreshed when an outer input's bound output re-points
  (an upstream forwarding/REF retarget), detected with one handle compare per
  outer input per cycle. Output forwarding retargets mark the
  forwarding endpoint modified so active downstream consumers schedule for the
  retarget cycle, including retargets to an invalid source. Logical removal
  clears the output binding, stops the child, and erases the output element to
  publish the removed delta, but leaves the ``MapKeyEntry`` constructed in its
  stable slot. The key set's later ``on_erase`` callback runs the destructor;
  insertion before that callback resurrects the same stopped graph and slot.
  On node stop, ``map_`` stops every child, erases the owned TSD elements, and
  clears the output, while entry destruction remains part of node-storage
  disposal after graph-wide subscriptions have been released.
- **Only valid child outputs are map elements.** If a live child's terminal
  becomes invalid — for example, ``switch_`` flips from an arithmetic branch
  to a request/reply-backed branch whose response is still in flight — the
  owned TSD publishes a removal for that key. The response adds the key again
  when the terminal validates. Released hgraph instead publishes an empty TSD
  delta during this switch flip and retains the stale element. hg_cpp's
  valid-child-set behavior is the accepted deviation for issues
  #105/#117/#119/#133/#145, pinned by the mapped request/reply tests and the
  bounded ``switch-flip-map-removal`` parity relation.
- **Storage-plan ordering is load-bearing**: the map field is placed *after*
  ``output`` in the node storage plan (``node_storage_plan_for``'s
  ``extra_fields_after_output``) so reverse-order destruction tears the
  children (whose links point INTO the output) down before the output —
  the mirror image of ``nested_``/``switch_``, whose outer forwarding output
  links INTO the field-held child and therefore destroys first.
- **Scheduling**: an evaluated child propagates its own next time to the map
  node (the graph-level pull); children left unevaluated this cycle pull
  their pending schedule up explicitly; out-of-band child schedules push
  through the nested-graph delegation as everywhere else.
- The output schema resolver discovers ``TSD<K, OUT>`` by compiling ``func``
  at the element schema (``resolve_default_types``, like ``switch_``).
- **Fixed TSL multiplexing is a wiring-time expansion**, not a runtime node —
  Python's ``_map_no_index``: the fixed-TSL overloads inline one application
  of ``func`` per index (key = ``const(i)`` at ``TS<Int>`` when ``func``
  takes it first, broadcasts passed whole), validate the per-index output
  schemas agree, and assemble a **structural TSL** output. Selected by the
  same ``map_`` name (``requires_`` gates on a fixed-size TSL input); the
  resolver discovers ``TSL<OUT, SIZE>`` by compiling ``func`` once.
- **Dynamic TSL multiplexing uses stable runtime child slots.** The maximum
  current length of every multiplexed dynamic TSL drives monotonic growth.
  Each index owns an ``InPlaceGraphSlotStore`` entry containing its ``Int``
  index value, optional read-only ``TS<Int>`` source, child handle, and the
  child's statically planned graph payload. Existing addresses never move and
  no separate per-child graph allocation occurs. A shorter peer list leaves that
  child's argument unbound until the peer grows to the index; scalar and
  ``pass_through`` inputs broadcast whole. The owned dynamic TSL output grows
  before binding each ordinary child terminal, which writes the real parent
  element directly. Index entries are grow-only: node stop stops and destroys
  all constructed children; there is no keyed remove/erase protocol because a
  dynamic TSL does not remove positions. Pass-through child outputs and
  terminals that already require forwarding/non-peered topology are rejected;
  this initial path supports ordinary owned whole-node outputs and sink
  functions.
- **The variadic tail is classified against the child signature**: a TSD is
  multiplexed when the corresponding child parameter accepts its element. A
  whole-time-series type variable (for example ``TIME_SERIES_TYPE`` or C++
  ``Port<void>``) multiplexes the first TSD in the argument list and establishes
  its key type. A later TSD supplied to such a variable multiplexes when its key
  type matches, and otherwise passes through whole. A parameter accepting the
  concrete whole TSD always receives it directly. Non-TSD values supplied to a
  whole-time-series variable also pass through. Unresolved operator element
  variables retain element-wise map behaviour. Later multiplexed TSDs must
  agree with the established key type. The live key set is the **union** of
  their key sets; a key absent from one dict leaves that
  child input unbound (invalid) until it appears there (the phantom-element
  behaviour), and the output entry is removed only when the key has left
  every multiplexed input. Non-TSD args broadcast whole; in the TSL form a
  tail arg with the same TSL shape (the same fixed size, or dynamic size zero)
  multiplexes per index.
  Positional arguments map onto ``func`` parameters in order (after the
  key) and **keyword arguments resolve by the function's parameter names**
  (``NamedPort`` on sub-graph ports, ``In<"name">`` on nodes — the
  ``WiredFn`` carries its parameter names); multiplexed-ness never reorders.
  There is no fixed anchor parameter (the Python shape): the call is
  ``map_(func, *args, **kwargs)``. Inputs are first resolved onto ``func``'s
  parameter order; the first collection in that ordered list selects the
  TSD/TSL kernel, and the first TSD provides the key type. Membership changes
  anywhere are structural events: they create/destroy children and re-bind
  surviving entries.
- **The lifecycle is always keys-driven.** The ``__keys__`` ``TSS[K]``
  outer input (``MapNodeSpec::keys_input_index``) alone creates and destroys
  children; there is no in-node union scan. When not supplied explicitly
  (``arg<"__keys__">(tss)``, Python's ``__keys__``), the wiring derives it —
  ``keys_(tsd)`` for one multiplexed input, ``union(keys_(tsd)…)`` for
  several — exactly Python's ``__keys__ = union(*key_sets)``. ``keys_`` is a
  **zero-copy projection**: ``TSDOutputView::key_set()`` exposes the dict's
  key set as a bindable ``TSS`` view over the same storage, addressed in
  edges/bindings by the ``ts_key_set_path_component`` path sentinel (no node
  is wired; works through forwarding links — the link layer carries its own
  key-set binding). The key-set surface is a **first-class time series**:
  it carries its own modified tracking AND its own observer set, recorded and
  notified only by membership changes in the dict slot storage — subscribers
  to the key set are never woken by the dictionary's value ticks, with no
  consumer- or link-side special-casing. The ``union`` node reconciles
  removals against the full
  current membership, so an input going invalid (a switched-away source)
  drops its exclusive members. An explicit set is validated as a ``TSS`` of
  the mapped key type, split from the kwargs before ``func``-parameter
  binding (it is ``map_``'s argument, not ``func``'s), and rejected on TSL
  maps. Children for keys absent from a dict keep phantom/invalid element
  inputs, as before.
- **Argument tags — Python's ``pass_through`` / ``no_key`` wrappers.**
  ``stdlib::pass_through(port)`` and ``stdlib::no_key(port)`` return the same
  ``Port`` carrying a wiring-time tag (``WiringPortRef::ArgTag``), exactly how
  the Python wiring marks arguments — the tag is **never part of graph
  structure**: it rides through ``arg<"name">``, variadic and keyword
  arguments, is ignored by edge/source interning, and instead joins the map
  node's interning identity through the ``MapCallConfig`` scalar (function +
  ``__key_arg__`` + per-argument tags), so equal inputs with different tags
  never dedup to one node. Classification honours the tags: ``pass_through``
  forces the argument to bind **whole** (broadcast, whatever its kind — the
  only way a child can consume a full ``TSD``, or keep a same-size ``TSL``
  whole in the TSL form, and such an argument never anchors the kernel/size
  selection); ``no_key`` keeps a TSD multiplexed but **excludes it from
  key-set inference** (its key *type* still participates) — if every
  multiplexed input is ``no_key`` an explicit ``__keys__`` is required, and
  ``no_key`` is rejected on non-TSD arguments and on TSL maps.
- **Sink maps use the same keyed or indexed slot lifecycle without an output.**  A C++
  caller selects ``map_sink_``; Python infers it from an outputless graph or
  sink-node function.  Key observation, in-place child allocation,
  stop-on-remove, and destroy-on-erase are unchanged.  No parent ``TSD`` or
  per-key/index output element is allocated.  Key-only sink maps derive ``K``
  from their explicit ``__keys__: TSS[K]`` input.
- **Dynamic TSLs retain a native lifted-kernel fast path.**  A lifted scalar kernel
  (including a standard operator whose implementation exposes one) maps in a
  single native node.  It discovers the grow-only runtime length from its
  multiplexed inputs and grows its dynamic TSL output in place without child
  graphs. Arbitrary graph/node functions select the slot-backed indexed
  runtime above.

Tests: ``tests/cpp/test_map.cpp`` and
``python/tests/test_python_authoring.py``. Keyed create/destroy churn is
ASan/UBSan-verified; indexed ownership is ASan-verified by the current
acceptance gate.


``mesh_``
---------

``mesh_(func, *args, **kwargs)`` has the same external call shape and
output model as ``map_`` over ``TSD`` inputs, but child instances may read
other instances' outputs by key from inside ``func`` via Python's
``mesh_(func)[key]`` / ``get_mesh(func)[key]`` or C++
``mesh_ref<OUT>(w, key)``. A mesh therefore owns one requested child graph
per live ``__keys__`` key, plus any on-demand child graph created because
another instance requested it.

- **Key and output ownership follow ``map_``.** Explicit or inferred
  ``__keys__: TSS<K>`` drives requested instance create/remove. Each instance
  owns its key value and key-source output. The mesh node owns the outer
  ``TSD<K, OUT>`` output; child terminals are forwarding outputs bound to
  real elements in that owned TSD, so branch/map/switch terminals inside the
  instance write through to the mesh element rather than copying values.
- **TSD argument classification follows ``map_``.** A whole-time-series type
  variable multiplexes the first TSD argument and establishes the key type;
  later matching-key TSDs multiplex, while later different-key TSDs and
  non-TSDs pass through. Concrete whole-TSD parameters pass through and
  element parameters multiplex. The first multiplexed input drives the
  signature. ``pass_through`` inputs do not contribute to inferred
  ``__keys__``; ``no_key`` inputs remain multiplexed but are excluded from the
  inferred union, so all-``no_key`` inputs require explicit ``__keys__``.
  TSL-specific ``map_`` rules do not apply because ``mesh_`` has no TSL
  kernel.
- **One internal key-slot store is authoritative.** Mesh instance membership
  is a superset of ``__keys__`` because dependency reads may create keys on
  demand. A mesh therefore cannot mirror only the external set's slot ids.
  Its own ``KeySlotStore`` assigns stable slots to both requested and on-demand
  keys and drives an ``InPlaceGraphSlotStore`` observer: ``on_remove`` stops
  the child, and the later ``on_erase`` destroys the entry and in-place graph.
  A separate observer on the external ``__keys__`` source pre-reserves capacity;
  source replacement performs a full membership reconciliation so shared keys
  survive even when the replacement uses a different slot layout.
- **Cross-instance reads are runtime forwarding nodes.** The Python
  ``MeshWiringPort.__getitem__`` and C++ ``mesh_ref`` wire a
  ``mesh_subscribe`` node in the child graph. At evaluation time it resolves
  the enclosing mesh, registers ``requester -> dependency``, binds its hidden
  value input to ``self[dependency]`` for future scheduling, and forwards the
  dependency element to its own output. If the dependency is not settled yet,
  the node pauses; the enclosing mesh is the pause boundary.
- **The settle loop is dependency-ranked.** A dependency request can create an
  on-demand instance and re-rank dependents so dependencies evaluate before
  requesters. The mesh scans instances by rank until no child pauses, allowing
  transitive chains to settle in one parent cycle. Cycles are runtime errors.
  The rank-order vector retains its grown capacity, and the current requester
  key is represented by a borrowed value pointer rather than an owning copy.
- **On-demand lifetime is reference-driven.** Requested keys are kept by
  ``__keys__`` membership. On-demand keys are kept only while they have
  dependents. Retargeting or removing a requester retracts its old dependency
  edge and removes now-unreferenced on-demand instances in the same mesh
  evaluation.
- **Mesh key-set access is forwarding too.** Using a Python
  ``MeshWiringPort`` as a collection or C++ ``mesh_keys_ref<K>(w[, name])``
  wires a ``mesh_key_set`` node that forwards the enclosing mesh output's
  ``TSD`` key-set projection. Named meshes use the same wiring-time mesh
  scope as sibling lookup.
- **Limitations match the current kernel.** ``mesh_`` is a TSD runtime
  operator; dynamic-TSL meshes are not implemented. The output restrictions
  mirror ``map_`` because the mesh output is an owned TSD whose elements are
  real storage targets.

Tests: ``tests/cpp/test_mesh.cpp`` and
``python/tests/test_mesh_map_classification.py``.


``dispatch_`` — runtime type dispatch (design record)
------------------------------------------------------

Ruling (2026-07-11): dispatch is **a small key utility feeding an
enumerated** ``switch_`` — no dedicated runtime machinery. The composition
(python frontend, ``dispatch_(op, *args, **kwargs)`` / the ``@dispatch``
decorator whose body registers as the most-generic overload):

1. The *dispatch arguments* are the operator signature's ``TS[cls]``
   parameters whose ``cls`` is a python class scalar (CompoundScalar or an
   object-kind class); ``on=("name", ...)`` restricts them explicitly.
2. ``type_(arg)`` reads each dispatch argument's DYNAMIC python type per
   tick; a small **key node** maps the type (tuple) onto the enumerated
   overload keys — ``issubclass`` filtering, most-derived (MRO-depth)
   specificity, ambiguity is an error. Multi-argument dispatch flattens
   the per-argument types through a ``TSL`` → ``TS[tuple]``.
3. ``switch_(key, {classes: overload, ...}, **call_args)`` instantiates
   the winning overload's graph; an unchanged key does not reload the
   branch (ordinary ``switch_`` semantics — a re-tick of the same concrete
   type does not re-emit).

The native selector retains one flattened type-pointer tuple per registered
case, or ``O(C*A)`` metadata for ``C`` cases and ``A`` dispatch arguments.  It
does not retain the pairwise ``C*C`` specificity relation: selection finds a
provisional greatest match and then verifies it against the other matches by
walking immutable Bundle parent links.  The child payload follows the ordinary
``switch_`` A/B protocol unchanged: exactly two in-place regions are reserved,
both sized and aligned for the largest compiled branch.  The stopped previous
child remains in one region while the newly selected child occupies the other;
no child payload region is reserved per dispatch case.

``typing.Union[TS[A], TS[B]]`` overload parameters expand into two ordinary
registry candidates and two enumerated switch keys. A CompoundScalar branch
materializes the selected closed-union leaf through checked ``downcast_``
before re-entering the C++ operator registry, so normal overload ranking and
``requires=`` predicates still apply. Wiring-time scalar arguments are closed
over by the branch; only time-series ports cross the switch boundary. Object-
kind Python classes share one ``TS[object]`` schema, so their already-selected
Python overload is invoked directly after the key utility resolves the class.

Python-class scalars and ``CompoundScalar`` hierarchies both retain their
dynamic type. CompoundScalar uses the graph-scoped closed Bundle union
described in :doc:`data_structures/schemas/scalar`: the active
``TypeRecord*`` is available without inspecting erased payload bytes, while
the largest alternative is reserved once when the graph's type realization
is built. This gives ``type_`` and dispatch a stable concrete key without an
``Any`` allocation per tick. The closure is captured at top-level wiring
completion and inherited unchanged by nested graph instances.

Reconciliation with the 2603 RFC
--------------------------------

The earlier 2603 nested-graph RFC, implementation notes, and sampled-runtime
contract informed the design; where they conflict with decisions recorded for
this codebase, the current code wins:

- **No ``NestedEvaluationEngine`` / clock-delegate objects.** The separate
  engine/clock split was rejected for this runtime (run-level state folds into
  the executor ops). The RFC's clock invariant — the parent must wake no later
  than the child's next scheduled time — is realised as (a) pull-style
  propagation after child start/evaluate (exists) and (b) push propagation from
  the nested graph's ``schedule_node`` to the parent node.
- **No ``ChildGraphTemplate`` / ``ChildGraphInstance`` classes.** Template =
  ``CompiledSubGraph``; instance = ``GraphValue`` in node storage.
- **Binding modes.** Of the RFC's binding-mode catalogue only the modes the
  current milestone needs are implemented: direct input binding (exists,
  incl. leaf-wise for structural boundary args), root output forwarding
  (exists), ``alias_parent_input`` pass-through (exists, the ``ParentInput``
  output-binding kind), key-value injection (with ``switch_``/``map_`` /
  ``mesh_``), and keyed write-through forwarding for ``map_`` / ``mesh_``
  output. REF adaptation is ordinary endpoint negotiation around those modes,
  not a separate binding kind. Context import/export is ordinary outer-port
  capture; recordable-state pass-through remains rejected explicitly at wiring
  time until designed here.
- **Sampled semantics on rebind.** Per the sampled-runtime contract, when
  ``switch_`` retargets the active branch at time *t*, the new child samples any
  already-valid bound inputs at *t*. Branch construction records an explicit
  sampled target-link bind, including the current descendants of nested
  collections, and ``switch_`` schedules the relevant consumers as a separate
  branch-selection operation. Ordinary per-cycle rebinding is a no-op;
  ``make_active`` remains subscription-only and does not fabricate a
  modification. We deliberately diverge from Python's ``value = None`` reset.
- **Stable payload storage is implemented.** Fixed nested graphs are part of
  the parent node's storage plan. ``map_`` mirrors ``__keys__`` slots into
  stable entry-plus-graph blocks; ``mesh_`` uses its own observed key-slot
  store for the requested/on-demand superset; dynamic ``reduce`` alternates
  two stable positional banks. In every case stop precedes destruction and a
  graph payload is destroyed before its slot is reused.


Roadmap (this milestone)
------------------------

1. **Done — sub-graph compilation.** ``BoundarySource`` +
   ``Wiring::finish_subgraph`` + ``compile_subgraph<G>`` + ``nested_<G>``
   (this page, above).
2. **Done — associative ``reduce`` over ``TSL`` / ``TSD``, as an operator** —
   the native live-value tree is the default registered overload of the
   ``reduce`` operator and the combiner is the ``WiredFn`` scalar (see its
   section above). Omitted and explicit zero arities follow the documented
   empty/singleton/multi-value rules. ``map_`` / ``switch_`` follow the same
   operator shape.
3. **Done — scheduling push delegation + shared binding helpers.** See
   *Scheduling delegation* above; helpers extracted to
   ``runtime/nested_bindings.h``.
4. **Done — ``switch_``** (see its section above): one active branch child,
   sampled retarget on key change, key consumption as an ordinary boundary
   input. ASAN/UBSAN-verified (branch teardown/rebuild churn).
5. **Done — ``map_`` over ``TSD``** (see its section above): keyed child
   instances driven by current-key reconciliation on mapped-source
   modification/repoint; child terminals write through forwarding outputs into
   the owned ``TSD``'s instantiated elements (no copy). ASAN/UBSAN-verified
   (keyed create/destroy churn). Explicit ``__keys__`` can be the sole
   lifecycle source when every child argument is broadcast. Nested map
   terminals retain their forwarding endpoint topology and concrete child
   storage type through TSD target-link traversal.
6. **Done — associative ``reduce`` over ``TSD``** (see its section above):
   the 2603 design ported/reconciled first, then the runtime kernel —
   alias leaves, minimal combiner tree, and the documented optional-zero
   empty/singleton/multi-value behavior.
   ASAN/UBSAN-verified.
7. **Done — ``mesh_`` over ``TSD``** (see its section above): map-compatible
   output ownership plus cross-instance forwarding, on-demand instance
   creation, dependency ranking, cycle detection, and key-set forwarding.

Non-goals for the milestone: services/contexts (since landed as their own
milestone — see :doc:`services`),
push sources inside nested graphs — a graph carrying a push prefix never gets a
nested graph type interned, so ``GraphBuilder::nested_type()`` rejects it. Note
this bounds nested *children* only: service and adaptor implementations are
inlined into the top-level graph, so an implementation may own a push source
(:doc:`services`, "Implementations are inlined, not nested").
(Non-associative reduction and dynamic-TSL
reduction have since landed. Explicit ``__keys__`` and the ``pass_through`` /
``no_key`` wrappers, originally deferred, landed within the milestone —
see the ``map_`` section. ``try_except_`` landed as its own follow-on
milestone — see :doc:`error_handling`.)
