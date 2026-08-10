Authoring, execution and services
=================================

Authoring decorators
--------------------

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Name
     - Use
   * - ``graph``
     - Compose nodes and operators without adding evaluation code.
   * - ``compute_node``
     - Evaluate Python code when active inputs tick and publish a result.
   * - ``sink_node``
     - Evaluate for side effects and produce no output.
   * - ``generator``
     - Publish timestamped values from an iterator.
   * - ``push_queue``
     - Adapt an external callback or queue into a source.
   * - ``operator``
     - Declare an overloadable authoring contract.
   * - ``dispatch`` / ``dispatch_``
     - Select a Python implementation by wiring-time scalar or type criteria.
   * - ``component``
     - Define a record/replay-aware graph component.

Decorated Python callables are adapters into the native wiring and execution
path. They do not create a second Python graph engine.

Running and testing
-------------------

``run_graph`` and ``evaluate_graph`` build and run an application graph using
``GraphConfiguration``. Use ``eval_node`` from ``hgraph.test`` for compact
per-cycle tests:

.. testcode::

   from hgraph import TS, compute_node
   from hgraph.test import eval_node

   @compute_node
   def twice(value: TS[int]) -> TS[int]:
       return value.value * 2

   assert eval_node(twice, [1, None, 3]) == [2, None, 6]

``GlobalContext`` and ``GlobalState`` hold graph-scoped configuration and
services. Configuration is copied into execution and copied back for Python
callers after the run; do not use unrelated process globals for graph state.

Services and adaptors
---------------------

Declare interfaces with ``reference_service``, ``subscription_service`` or
``request_reply_service``. Attach implementations with ``service_impl`` and
``register_service``. Adaptors use ``adaptor``, ``adaptor_impl`` and
``register_adaptor``; service adaptors use the corresponding
``service_adaptor`` names.

``get_service_inputs`` and ``set_service_output`` are advanced implementation
helpers for custom service/adaptor wiring. Prefer the declared service stubs in
application code.

Source migration note
---------------------

The 0.5 decorator names ``pull_source_node``, ``push_source_node`` and
``const_fn`` are not supported top-level decorators in 0.8. Use ``generator``
for timestamped pull-style sources, ``push_queue`` for externally pushed data,
and ordinary scalar Python code plus ``const`` when a wiring-time value must
become a time series. See :doc:`../user_guide/python_compatibility` for the full
migration table.
