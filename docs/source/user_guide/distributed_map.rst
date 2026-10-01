Distributed maps
================

``hgraph.dmap_`` runs mapped child graphs in separate worker processes. Each
worker uses the native ``map_`` wiring and child lifecycle. The parent supplies
evaluation times and propagates scheduled child work back into its own graph.
Python callbacks run in each worker's interpreter.

Define the child in an importable module, for example ``strategy_nodes.py``:

.. code-block:: python

   from hgraph import TS, graph

   @graph
   def notionals(price: TS[float], quantity: TS[int], multiplier: float = 1.0) -> TS[float]:
       return price * quantity * multiplier

Use it from a graph:

.. code-block:: python

   from hgraph import TS, TSD, dmap_, graph
   from strategy_nodes import notionals

   @graph
   def distributed(prices: TSD[str, TS[float]], quantities: TSD[str, TS[int]]) -> TSD[str, TS[float]]:
       return dmap_(notionals, prices, quantity=quantities,
                    multiplier=100.0, __workers__=4)

Call and data semantics
-----------------------

Positional and named arguments, wiring-time scalar configuration,
``pass_through``, ``no_key``, ``__keys__``, ``__key_arg__`` and ``__label__``
follow ``map_``. Dictionary keys use the same inferred union or explicit key
set. Fixed and dynamic lists preserve original indices; whole-list and
whole-dictionary arguments are broadcast when ``map_`` classifies them that
way. An outputless child creates a distributed sink map and returns ``None``.
The default worker count is two. ``in_process=True`` is an explicit diagnostic
mode using the same worker plans and boundary transfer. Process workers have a
finite 60-second response deadline by default. Set ``__worker_timeout__`` to a
positive number of seconds (at most 24 hours) to adjust it; expiry terminates
the affected worker and fails the run. This deadline applies to process
communication, including bootstrap transfer, dispatch and reply collection;
it cannot interrupt Python code while wiring in the parent or callbacks in
``in_process`` diagnostic mode.

Time-series boundaries support scalar ``TS``, ``SIGNAL``, ``TSS``, ``TSD``, fixed/dynamic
``TSL``, ``TSB`` and ``TSW`` recursively. Transfers preserve invalid collection
members, removals, list extent, partial bundles and window history. References
are materialized into values at the boundary; child graphs may use references
internally, but no pointer or binding into another process is transported.

Ordinary ticks transfer changed data. New or rebound subtrees carry their
current state once, including original window sample timestamps. Steady-state
window updates transfer the sample/clear operation rather than copying the
whole window. The parent captures and encodes common input changes once before
fan-out. Transport volume scales with changed input data and worker count;
each worker computes only its own mapped keys or indices. Large broadcast
inputs still consume storage in every worker.

Portable scalar values include the supported native numeric, text, byte,
temporal, enum and compound/collection types, Arrow frames and series, and
owned/shared value payloads. Time zones cross by semantic identity, rather than
process-local registry handles. Bound codec plans retain the graph's immutable
type realization so derived compound values preserve their concrete fields.
Unsupported live handles or opaque Python objects fail during wiring.

Process constraints
-------------------

Workers use the caller's Python executable and import paths. A Python child
must be a module-level importable graph/node; registered native operator names
are also supported. Wiring-time scalar arguments must have a portable value
encoding or an importable type recipe. Python code, closures and live objects
are not pickled. Functions defined in ``__main__``, notebook-local functions
and lambdas cannot be reconstructed by process workers. Bootstrap metadata,
including scalar configuration and import paths, travels over the connected
channel rather than command-line arguments.

Transport limits
----------------

Every channel frame, including bootstrap and cycle messages, is bounded, and
decoding has a shared work and nesting budget. These are defaults, not protocol
constants: they stop a malformed length prefix or a hostile payload from
allocating without limit, and they are expected to be raised for a boundary
whose legitimate traffic is larger.

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - ``dmap_`` argument
     - Default
     - Bounds
   * - ``__max_frame_bytes__``
     - 64 MiB
     - One whole framed message: bootstrap, cycle, or a worker's checkpoint
       image (:doc:`component_recovery`).
   * - ``__max_decode_work__``
     - 1,000,000
     - Decoded values, collection inventories and sparse list extents, shared
       across one payload.
   * - ``__max_decode_depth__``
     - 256
     - Nesting levels within one payload.

Each must be a positive integer; ``None`` keeps the default. Raise
``__max_frame_bytes__`` when a cycle's encoded delta -- or a completed day's
worker image, which crosses the same channel -- is larger than the cap,
and ``__max_decode_work__`` when a collection has more members than the budget
counts -- the work budget counts elements rather than bytes, so a wide
dictionary exhausts it while its frame is still small. ``__max_decode_depth__``
bounds recursion over nested schemas, so it is a property of the boundary type
rather than of the data; raising it far trades a clear error for a deeper
decode stack.

A limit applies to **both ends**. One setting configures the caller's channel
and travels in each worker's command line, because a cap only one side holds
would have the stricter end reject what the other was willing to write. An
exceeded limit names the size it refused and the limit it refused it against,
so the number to raise is in the error.

A boundary delta is fully decoded and validated before it changes live output;
allocation failure during application still fails the run without rollback.

Workers execute **trusted code** with the caller's operating-system identity,
environment, working directory, and filesystem and network permissions.
**Process separation is not a security sandbox.** The callable's module is also
imported in the parent while constructing its recipe. Do not use ``dmap_`` to
execute untrusted modules or treat graph-state isolation as an access-control
boundary.

Workers do not inherit the parent's runtime ``GlobalState``, services,
contexts, captured outer ports or live resources. Worker-local state remains
available; keys beginning with ``__hgraph_distributed_`` are reserved for
transport. Push sources, component
checkpoint recovery, worker restart and rebalancing remain unsupported.
Restrictions on valid child shapes imposed by ordinary ``map_`` also apply;
for example, a shape refused by its dynamic-list implementation does not gain
new semantics through distribution.

Worker evaluation or stop errors fail the parent run. Pool cleanup releases
remaining workers when a failure unwinds. Sink side effects occur in worker
processes: their global ordering is not defined across partitions, and failure
does not make external side effects transactional.

Native embedding
----------------

``hgraph/runtime/distributed_map_wiring.h`` exposes ``DistributedMapInput``,
``prepare_distributed_map_pool`` and ``wire_distributed_map``. Descriptors retain
the original argument names and map tags. Native and Python clients share
these prepared plans, the worker pool, transport and executor.
``WorkerPoolConfig::timeout`` sets the process communication deadline in
milliseconds and defaults to 60000. An embedding frontend can set
``recipe_over_channel`` to send its prepared recipe as the first bounded frame;
the worker receives ``@hgraph-channel-bootstrap:1`` as its command-line recipe
marker and must retain that same channel for subsequent cycle messages.

``WorkerPoolConfig::limits`` is the native form of the transport limits above:
a ``TransportLimits`` holding ``max_frame_size`` and a ``BinaryDecodeLimits``
``decode``. The pool configures each spawned worker's channel from it and
passes it in that worker's ``argv`` as ``--hgraph-worker-max-frame``,
``--hgraph-worker-max-work`` and ``--hgraph-worker-max-depth``; a worker
launched without those flags serves on the library defaults.
``PipeEndpoint::set_max_frame_size`` configures one endpoint directly, and
``serve_worker`` takes the decode budget a hand-rolled worker loop should
use. ``hgraph/runtime/distributed_limits.h`` holds
``DEFAULT_MAX_FRAME_SIZE``.

A native executable registers a ``PreparedWorkerRecipe`` whose function
reconstructs one plan for a supplied group/count. ``bind_distributed_map_recipe``
selects its name for every partition; ``run_worker_if_requested`` dispatches
that factory when the executable starts as a worker. The application must link
the same recipe registration into both programs. No captured factory closure
is transported. An embedding frontend can instead supply its own executable
bootstrap and call ``serve_worker`` with the reconstructed plan.

Benchmark the actual child workload before choosing the worker count. The
native benchmark corpus in :doc:`../rfc/rfc_0037_distributed_map` does not measure
Python interpreter startup or Python callback costs.
