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
channel rather than command-line arguments. Every channel frame, including
bootstrap and cycle messages, is limited to 64 MiB; oversize messages fail
instead of allocating unbounded buffers. Decoding also has a shared budget of
1,000,000 work units and 256 nesting levels. Collection inventories, decoded
values, and sparse list extents consume that budget. The lower-level native
codec and ``BoundaryTransfer`` APIs accept ``BinaryDecodeLimits`` overrides.
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
