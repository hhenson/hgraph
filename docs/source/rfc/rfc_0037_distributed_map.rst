RFC 0037: Distributed ``map_`` over Externally Timed Child Graphs
=================================================================

:Status: Draft
:Author: Howard Henson
:Created: 2026-09-16
:Target: ``dmap_`` operator, a new ``hgraph-distributed`` extension, the
         externally-timed child-graph host contract in ``runtime/``
:Related: RFC 0017 (binary value codec — hard dependency),
          RFC 0022 (serializable graph manifest — identity check),
          RFC 0023 (checkpoint recovery — worker restart, deferred),
          RFC 0026 (versioned dataflow fabric — the coarse-grained sibling),
          RFC 0027 (push-source queues), RFC 0036 (reference transparency),
          ``developer_guide/nested_graphs.rst``, ``developer_guide/services.rst``

Summary
-------

Add ``dmap_``: a ``map_`` whose per-key child graphs are evaluated in worker
**processes** rather than in the calling graph, with engine time supplied from
the outside and results copied back each cycle.

The central claim of this RFC is that **this needs far less new machinery than
it appears**, because an hgraph nested child graph is *already* an externally
timed graph executor. The existing per-cycle contract between a nested node and
its child is::

    bool     GraphView::evaluate(DateTime evaluation_time)   // time supplied from outside
    DateTime GraphView::next_scheduled_time()                // what the child wants next

``src/hgraph/runtime/nested_graph_node.cpp`` drives exactly those two calls
(``single_nested_graph_evaluate`` and ``single_nested_graph_propagate_schedule``).
A child graph never reads a clock, never decides when it runs, and already
publishes its internal scheduling intent as a value its parent reads back.

``dmap_`` therefore does not invent a new execution model. It puts a transport
between those two calls and their child, and adds the one thing a process
boundary makes necessary: **copying** time-series deltas in and out instead of
sharing memory. The delta copy primitive also already exists — ``ts_delta.h``'s
``capture_delta`` / ``apply_delta`` reduce a cycle's delta to a canonical
``Value`` and back, type-erased over every replayable schema, and are what
``record`` / ``replay`` are built on.

What is genuinely new is: a wire encoding (RFC 0017), a partition-to-worker
mapping, a per-cycle request/reply barrier, and a **fail-closed wiring-time
rule** that rejects the constructs which cannot cross a process boundary —
references, services, contexts, shared outputs and push sources.

Motivation
----------

hgraph evaluates one graph on one thread by ruling (``CLAUDE.md`` §7,
2026-07-02). That ruling is about the *per-tick runtime path* being lock-free
and ``shared_ptr``-free, and it has been good for both correctness and speed.
It also means a single graph cannot use more than one core, and the workloads
that most want more cores are precisely the ones ``map_`` already describes:
the same computation over many independent keys.

A ``map_`` over 10,000 instruments is embarrassingly parallel by construction.
Each child graph is independent: it sees its own key, its own element of each
multiplexed input, and whatever broadcast inputs it was wired to. Nothing in
the child's evaluation reads another child's state. The only reason they run
sequentially is that they share a thread.

The existing distribution story, RFC 0026's ``hgraph-fabric``, deliberately
works at a different granularity: whole components exchanging durable,
versioned ``Frame`` values, with independent lifecycles and no shared engine
clock. That is the right model for "a daily table is rebuilt, then a
normalisation step follows". It is the wrong model for "evaluate these 10,000
per-key subgraphs in lockstep, this cycle". ``dmap_`` fills that gap and the
two do not compete.

The central observation
-----------------------

A nested child graph is already externally timed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``include/hgraph/runtime/graph.h`` exposes, through ``GraphOps``:

* ``evaluate_impl(context, graph, evaluation_time)`` — the *caller* supplies
  the cycle's evaluation time;
* ``next_scheduled_time_impl(...)`` — the child's own scheduling intent,
  readable from outside;
* ``start_impl(..., DateTime start_time)`` / ``stop_impl(..., DateTime stop_time)``;
* ``set_child_schedule_observer_impl(...)`` — so a child that becomes runnable
  between cycles can tell its parent.

and ``nested_graph_node.cpp`` uses them precisely as this RFC needs::

    bool single_nested_graph_evaluate(view, evaluation_time) {
        return nested.child_graph().evaluate(evaluation_time);
    }

    void single_nested_graph_propagate_schedule(nested) {
        const DateTime next = nested.child_graph().next_scheduled_time();
        nested.node().graph().schedule_node(nested.node().node_index(), next);
    }

The user-facing consequence is that the "hidden output carrying the next
scheduled time" this design needs is not a new concept to be added — it is the
existing ``next_scheduled_time()``, and ``propagate_child_schedule`` is already
the option that controls whether a parent honours it.

The partitioned / broadcast distinction already exists
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``include/hgraph/runtime/map_node.h`` classifies every child boundary argument
already::

    enum class MapArgSourceKind : std::uint8_t {
        Key,         // the per-key constant
        Element,     // the multiplexed TSD/TSL child output at this key   → PARTITIONED
        OuterInput,  // an outer (broadcast) input, bound whole            → SHARED
    };

``Element`` arguments are copied only to the worker owning that key.
``OuterInput`` arguments are broadcast to every worker that has at least one
live key. ``Key`` is a per-key constant the worker can synthesise locally from
the key itself and never needs to be sent at all. The copy-and-forward policy
follows from the existing classification rather than from new annotations.

Ownership boundary
------------------

Two pieces, deliberately split:

**Core (this repository, ``runtime/``)** — a small, transport-free
``ExternallyTimedChildHost`` abstraction extracted from what ``map_node``
already does, so that "drive a set of per-key child graphs with a supplied
evaluation time and collect their next scheduled times" has one implementation
that both ``map_`` and ``dmap_`` use. Core gains **no** transport, no sockets,
no serialization of its own beyond what ``ts_delta.h`` already provides.

**Extension (``extensions/distributed``, package ``hgraph-distributed``)** —
the ``dmap_`` operator, the partitioner, the worker process entry point and the
IPC transport. It depends on the core SDK and on RFC 0017's codec.

This split follows the promotion gate in ``rfc_0000.rst``: the transport is not
mathematically general and has no claim on core. The child-host abstraction is
a refactor of code that already exists in core and stays there.

.. note::

   The split is the part of this RFC least settled by evidence. If the
   extension turns out to need runtime internals that the SDK does not expose,
   the honest resolution is to name those hooks explicitly and add them to the
   SDK — not to move the transport into core. See *Unresolved questions*.

The per-cycle contract
----------------------

The wire protocol is deliberately tiny, because the nested-graph contract it
mirrors is tiny.

Request (outer → worker), once per partition per cycle::

    struct CycleRequest {
        DateTime  evaluation_time;
        KeyDelta  keys;             // keys added / removed for THIS partition
        DeltaList shared;           // (arg_ordinal, delta Value) for OuterInput args
        DeltaList partitioned;      // (key, arg_ordinal, delta Value) for Element args
    };

Reply (worker → outer)::

    struct CycleReply {
        DeltaList outputs;          // (key, output_ordinal, delta Value)
        DateTime  next_scheduled_time;   // min over this partition's child graphs
        Status    status;           // ok | node_error(payload)
    };

Every ``delta Value`` is exactly what ``capture_delta(TSInputView)`` produces —
a ``Value`` over ``TSValueTypeMetaData::delta_value_schema`` — and is applied
on the far side with ``apply_delta(TSOutputView, ValueView)``. Both are already
implemented, already type-erased, and already exercised by ``record`` /
``replay``. The only missing layer is the byte encoding, which is RFC 0017.

The worker hosts an ordinary ``map_``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A worker does **not** implement per-key lifecycle logic of its own. It runs a
normal ``map_`` node over the key subset it owns, inside a graph it builds from
the same wiring code as the caller. Key add/remove from ``CycleRequest::keys``
is applied to that ``map_``'s key set; child construction, destruction, output
element lifetime and ``__keys__`` semantics are then the existing, tested
behaviour rather than a reimplementation.

This is the main reason the design is small: ``dmap_`` is a *partitioning
front-end plus N ordinary ``map_`` nodes*, not a new nested-graph engine.

Scheduling
~~~~~~~~~~

The ``dmap_`` node schedules itself at ``min`` over the partitions'
``next_scheduled_time`` replies, which is what
``single_nested_graph_propagate_schedule`` does for one child. A worker whose
children are all idle returns ``MAX_ET`` and costs nothing.

The barrier, and why v1 is synchronous
--------------------------------------

In v1 the ``dmap_`` node **blocks inside its own** ``eval()`` until every
partition with work has replied. Workers run concurrently with each other; the
caller waits for the slowest.

This is not the only option. ``graph.cpp``'s evaluation loop already supports a
node returning ``false`` to request a **mid-cycle pause**, holding the cursor in
graph state and resuming at the *same* evaluation time without redoing per-cycle
setup::

    // The node loop drives state.evaluation_cursor (the node_id) directly so a
    // pause can resume mid-cycle. When a node returns false it has requested a
    // pause: the cursor is already sitting on that node ... the next evaluate at
    // the same time continues from there WITHOUT redoing the per-cycle setup

That machinery is exactly what an asynchronous dispatch would want. It is
**not** used in v1 for one concrete reason: today a pause is resolvable only by
an enclosing ``mesh_``, and the root graph throws when one escapes —
``"root graph evaluation paused with no resolver"`` (``executor.cpp``). Making
the root executor a pause resolver that can wait on an external completion is a
separate change with its own risks to the simulation and real-time executors.

The cost of the synchronous choice is small: the runtime is single-threaded by
ruling, downstream nodes need the results before they can run, and the only work
lost is same-rank work independent of ``dmap_``. The pause path is recorded here
as the v2 upgrade and the reason not to design the transport in a way that
forecloses it.

Determinism
-----------

This is the property that makes the feature worth having, and it should be
stated as a contract rather than a hope.

Because engine time is supplied by the caller and a worker never reads a clock,
never runs a push source (banned below) and never advances its own time, a
``dmap_`` run produces results **identical to the equivalent ``map_`` run**,
regardless of how many partitions exist, how they are assigned, or how long any
worker takes. Distribution changes throughput and latency. It must not change
semantics.

Two consequences follow:

* The partition count and the partition function are **not** part of the
  program's meaning, so they may be configuration rather than wiring.
* The test plan below is largely differential: ``dmap_`` versus ``map_`` over
  the same recorded inputs, asserting equality — which the record/replay and
  parity machinery already supports.

What v1 rejects, and why
------------------------

Each of these is a **wiring-time** rejection with a diagnostic naming the
offending argument, not a runtime failure. Fail closed: a construct that cannot
be shown to cross the boundary is refused.

References (``REF<T>``)
~~~~~~~~~~~~~~~~~~~~~~~

``TimeSeriesReference`` holds a ``TSOutputHandle target`` — a pointer into
*this process's* graph memory — and composite references are trees of the same
(``include/hgraph/types/time_series_reference.h``). There is no value to copy;
a reference is an address. Every boundary argument must therefore be
**dereferenced before it is sent**, so the child sees a value.

RFC 0036 ("Reference Transparency Has Four Owners", Accepted) is the machinery
that decides where a REF is transparent and where it is materialised. The
``dmap_`` boundary is a forced materialisation point and should be expressed
through that RFC's owners rather than as a special case beside them. Getting
this wrong in the *other* direction is a known, expensive failure mode — see
``nested-boundary-ref-rebind``: comparing the wrong handle at a nested boundary
re-binds and re-ticks every cycle.

Services, contexts and shared outputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Banned in v1, and the reason is structural rather than incidental. All three
flavours are implemented as REF-over-graph-local-state
(``developer_guide/services.rst``):

    **Shared output** (``runtime/shared_output_node.h``): the source owns a
    ``REF<T>`` output and graph-local REF state …
    **Context** (``runtime/context_node.h``): the same primitive with
    wiring-scoped keys …

A child in another process has no access to that state. Request/reply services
are worse than merely unreachable: they depend on a rank and cycle hand-off
inside the *owning* graph, where "a dynamically started child cannot safely
schedule an outer request source whose rank may already have passed, so its
request reaches the owning graph on the following cycle". A cross-process child
cannot participate in that protocol at all.

The eventual answer is the wrapper the original sketch anticipates: a service
proxy that presents itself to the child as an ordinary input and is serviced by
the caller's graph across the transport. That is deferred deliberately —
request/reply across a per-cycle barrier introduces a distributed round trip
*inside* one engine cycle, which is a design problem in its own right and should
not be smuggled into the first working model.

Push sources inside a child
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Banned. A push source injects events on its own thread and wakes the executor
(``CLAUDE.md`` §7: push-source senders and the real-time executor CV are the
only sanctioned cross-thread runtime boundary). Inside a worker, its events
would arrive on the worker's timeline rather than the caller's, which breaks
the externally-timed contract and the determinism guarantee with it.

Forwarding output modes
~~~~~~~~~~~~~~~~~~~~~~~

``MapOutputBindingMode`` has three values; only one is transportable::

    ChildTerminalWritesElement,              // value written through   → OK
    OutputElementForwardsToChildTerminal,    // reference forwarding    → rejected
    OutputElementForwardsToParentSource,     // reference forwarding    → rejected

The two forwarding modes make the map output element a view onto the child's
output rather than a copy of it. Across a process boundary there is nothing to
forward to. ``dmap_`` forces ``ChildTerminalWritesElement``.

Non-encodable schemas
~~~~~~~~~~~~~~~~~~~~~

Any boundary schema whose ``delta_value_schema`` the RFC 0017 codec cannot
encode — most obviously Python-object scalars (RFC 0003 / RFC 0004) held by
identity rather than by value. Rejected at wiring with the schema named.

Partitioning
------------

The live key set is the union of ``MapNodeSpec::multiplexed_inputs``, or the
explicit ``__keys__`` ``TSS[K]`` when wired (``keys_input_index``) — unchanged
from ``map_``. Partition assignment is a pure function of the key::

    partition = partitioner(key) % worker_count      // default: stable hash

Requirements on the partitioner: pure, stable across processes, and independent
of insertion order — a key must land on the same worker in the caller and in any
restarted worker. The default is a stable hash of the key's canonical value
encoding, *not* ``std::hash``, whose values are not stable across processes or
builds.

Rebalancing on worker-count change is out of scope for v1: worker count is fixed
for the run.

Worker bootstrap and identity
-----------------------------

A worker cannot be sent the graph. RFC 0022 is explicit that a manifest
"does **not** serialize executable code, function pointers, credentials, or live
resources", and that an application "reconstructs a graph through its ordinary
C++ or Python wiring path and validates the resulting manifest".

So the model is: **the worker runs the same program**, wires the same child
template through the same code path, and the manifest is the *cross-process
agreement check* rather than the transport. On a mismatch the run fails at
startup with a manifest diff, which is exactly the failure RFC 0022 exists to
make legible. This matches how Python's ``spawn`` start method already behaves
(the module is re-imported in the child), which is why the original
multiprocessing experiment works at all.

Failure handling
----------------

v1: a worker that crashes, times out or reports a node error fails the
``dmap_`` node with a ``NodeError`` naming the partition and the key, through
the existing per-node capture path (``developer_guide/error_handling.rst``), so
``try_except_`` behaves as it does for any other node.

Worker restart and resumption are deferred. When they land, RFC 0023's
checkpointing is the mechanism — a restarted worker must recover its children's
state, and that is precisely a checkpoint restore.

Python contract
---------------

.. code-block:: python

    @graph
    def dmap_(func, *args, __keys__=None, __workers__: int = 4, **kwargs):
        """As map_, but evaluates per-key child graphs in worker processes."""

Call-shape parity with ``map_`` is a requirement, not an aspiration: the point
of a separate name is to keep the distribution decision explicit while the
model is proven, not to grow a second dialect. ``__workers__`` and an optional
``__partitioner__`` are the only additions.

Serialization consequences
--------------------------

This RFC is blocked on RFC 0017 and should not be implemented before it.
RFC 0017 is currently *Proposed*, states that it "deliberately decides **no
transport**", and identifies itself as "the piece every candidate transport
needs". ``dmap_`` is the first concrete consumer, and implementing it will
therefore exercise RFC 0017's envelope — schema identity, monotonic sequence,
image/delta discriminator — against a real bidirectional stream rather than a
store.

Performance and memory
----------------------

The gain is bounded by the fraction of cycle time spent in child evaluation,
against a per-cycle cost of: one ``capture_delta`` per changed boundary input,
one encode, one write, one read, one decode and one ``apply_delta`` per changed
output, plus a barrier.

That cost is per *changed* value, and deltas are already the unit, so a sparse
cycle is cheap. But it is emphatically not free, and the honest statement is
that ``dmap_`` will be **slower** than ``map_`` for cheap children — the
crossover is an empirical question this RFC does not pretend to answer in
advance. The benchmark orchestrator must report the crossover (children per
cycle × per-child cost) before the operator is recommended for anything, and the
raw JSON committed beside the summary (``benchmark-raw-evidence``).

Shared memory for the delta payloads is the obvious optimisation and is
deliberately not in v1: it should be chosen against a measured baseline, not
assumed.

Alternatives considered
-----------------------

**Threads instead of processes.** Rejected: the single-threaded ruling exists
because the per-tick path is lock-free and ``shared_ptr``-free; making child
graphs thread-safe would relitigate the ruling for the whole runtime. Processes
keep that ruling intact — each worker is a single-threaded runtime.

**Use ``hgraph-fabric`` (RFC 0026).** Right tool, wrong granularity: whole-value
``Frame`` publication with independent lifecycles and no shared engine clock,
where ``dmap_`` needs per-key deltas in lockstep with the caller's cycle.

**Asynchronous dispatch via the pause cursor.** Deferred, not rejected; see
*The barrier* above. It is the intended v2.

**Distribute at the executor rather than at ``map_``.** Partitioning an
arbitrary graph requires a cut with no reference-carrying edges and no shared
services, which is the hard general problem. ``map_`` hands us a cut that is
already clean by construction. Starting here buys a working model; generalising
later remains open.

Unresolved questions
--------------------

#. **Does the extension need core hooks the SDK does not expose?** The
   core/extension split above is asserted, not proven. It should be settled by
   attempting the extension against the installed SDK before any core refactor
   lands.
#. **Transport choice.** A framed stream over a socketpair is the portable
   default; Windows has no ``socketpair`` and needs a loopback socket or named
   pipe. Shared memory is the optimisation. RFC 0034 (NATS) is the natural
   remote transport once a transport interface exists.
#. **Does ``dmap_`` belong in the 1.0 surface?** RFC 0005 freezes the API. A
   provisional operator inside an extension is the safer answer.
#. **Worker-side scheduling fidelity.** ``next_scheduled_time`` is a ``min``
   over the partition; whether a worker should report per-key times so the
   caller can skip partitions with no due work is an optimisation to measure.
#. **Key skew.** A hash partitioner handles cardinality, not cost. Cost-aware
   assignment needs feedback the v1 protocol does not carry.

Acceptance criteria and test plan
---------------------------------

The differential criterion is primary; everything else supports it.

#. **Equivalence.** For a corpus of ``map_`` graphs, ``dmap_`` produces output
   identical to ``map_`` — same values, same ticks, same engine times — across
   worker counts 1, 2 and N > keys, and across partitioner permutations. Driven
   by the existing record/replay machinery.
#. **Scheduling.** A child using ``schedule`` / alarms fires at the same engine
   times under ``dmap_`` as under ``map_``, including a child scheduled between
   input ticks (the case that exercises ``next_scheduled_time`` rather than the
   input path).
#. **Lifecycle.** Keys added and removed mid-run build and tear down children in
   the owning worker only; a key removed from all multiplexed inputs destroys
   its child; ``__keys__`` semantics match ``map_``.
#. **Rejections.** Each banned construct — a ``REF`` boundary argument, a
   service or context consumer, a push source in the child, a forwarding output
   mode, a non-encodable schema — fails at **wiring** with a diagnostic naming
   the argument. One test per construct; a silent acceptance is a bug.
#. **Failure.** A killed worker fails the node with a ``NodeError`` naming the
   partition, and ``try_except_`` observes it as for any node.
#. **Native and Python.** C++ behavioural tests in the extension's suite plus
   the Python surface, per the parity acceptance rule
   (``developer_guide/parity_matrix.rst``).
#. **Benchmarks.** The crossover point reported, raw JSON committed.

Implementation status
---------------------

Draft. No implementation. Blocked on RFC 0017.

References
----------

* ``include/hgraph/runtime/graph.h`` — ``GraphOps::evaluate_impl``,
  ``next_scheduled_time_impl``, ``set_child_schedule_observer_impl``
* ``src/hgraph/runtime/nested_graph_node.cpp`` — ``single_nested_graph_evaluate``,
  ``single_nested_graph_propagate_schedule``
* ``include/hgraph/runtime/map_node.h`` — ``MapArgSourceKind``,
  ``MapOutputBindingMode``, ``MapNodeSpec``
* ``include/hgraph/types/time_series/ts_delta.h`` — ``capture_delta`` / ``apply_delta``
* ``include/hgraph/types/time_series_reference.h`` — ``TimeSeriesReference``
* ``src/hgraph/runtime/graph.cpp`` — the evaluation cursor and mid-cycle pause
* ``developer_guide/nested_graphs.rst``, ``developer_guide/services.rst``,
  ``developer_guide/error_handling.rst``
