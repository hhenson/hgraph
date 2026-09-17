Distributed maps
================

``hgraph.dmap_`` runs mapped child graphs in separate worker processes. Each
worker hosts an ordinary native ``map_``; the parent sends changed inputs and
drives the worker at graph evaluation times. Scheduled child work is propagated
back to the parent. Python callbacks run in each worker's own interpreter.

Define the child in an importable module, for example ``strategy_nodes.py``:

.. code-block:: python

   from hgraph import TS, compute_node

   @compute_node
   def expensive_value(ts: TS[int]) -> TS[int]:
       return sum(i * ts.value for i in range(10000))

Use it from a graph:

.. code-block:: python

   from hgraph import TS, TSD, dmap_, graph
   from strategy_nodes import expensive_value

   @graph
   def distributed(values: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
       return dmap_(expensive_value, values, __workers__=4)

The current API accepts one ``TSD[K, TS[V]]`` input and a child returning
``TS[R]``. A first parameter named ``key`` can receive ``TS[K]``, as with
``map_``. Child state, key removal/recreation, self-scheduling, and quiet cycles
use the native map implementation. Native graphs composed through Python and
Python-authored compute nodes are both supported. Boundary values need a
native binary codec; numeric, text, temporal and supported composite scalar
values cross without per-cycle Python serialization.

Workers start with the caller's Python executable and import paths. The child
and its boundary type annotations must be importable by module-qualified name.
Functions defined in ``__main__``, notebook-local functions, lambdas and closures
are refused for process execution. Wrap a native operator in a module-level
``@graph`` when using it as the child. Workers do not inherit the parent's
runtime ``GlobalState`` or live Python objects.

``in_process=True`` runs the same worker plans locally for diagnostics. It is
explicitly opt-in. Process execution remains the default. Worker evaluation
errors fail the parent run; a failed worker stop also fails the run, and pool
ownership releases the remaining workers during cleanup.

Current limits
--------------

Multiple inputs, broadcast inputs, explicit ``__keys__``, custom partition
functions, and collection-shaped child outputs are not exposed by this first
Python API. Push sources, parent services/contexts, checkpoint recovery,
worker restart and rebalancing are unsupported. Stepped executors explicitly
reject component-recovery configuration because they have no completed-day
publication boundary.

The native canonical delta currently cannot preserve a newly added key whose
value is still invalid. ``dmap_`` inherits that documented deviation from
``map_``; see :doc:`../rfc/rfc_0037_distributed_map`. Distribution also incurs
transport and encoding costs, so benchmark the actual child workload before
choosing the worker count.

Native embedding
----------------

``hgraph/runtime/distributed_map_wiring.h`` exposes
``prepare_distributed_map`` and ``wire_distributed_map`` for runtime-resolved
native schemas and embedding frontends. Both use ``WorkerPool``. A prepared
plan owns the compiled child and boundary; interpreter launch arguments and
an optional executor phase runner supply frontend-specific bootstrap and GIL
management. The existing ``dmap_impl`` and registered native-worker recipes
remain supported.
