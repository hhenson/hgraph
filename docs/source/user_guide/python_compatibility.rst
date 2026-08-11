Migrating Python code from 0.5 to 0.8
=====================================

The 0.8 line keeps Python as the supported end-user authoring interface while
moving graph wiring, execution, time-series storage and standard operators to
the C++ runtime. Most graph, node and operator code should move without a
redesign. The main changes affect code that imported Python runtime internals
or mutated live endpoint topology.

What remains supported
----------------------

The following authoring patterns remain part of the public Python surface:

* ``TS``, ``TSS``, ``TSD``, ``TSL``, ``TSB``, ``TSW``, ``REF`` and
  ``SIGNAL`` type expressions;
* ``graph``, ``compute_node``, ``sink_node``, ``generator`` and ``push_queue``;
* operator syntax, subscripted type resolution, ``map_``, ``reduce``, ``mesh_``
  and ``switch_``;
* ``run_graph``, ``evaluate_graph``, ``GraphConfiguration`` and
  ``hgraph.test.eval_node``;
* services, adaptors, components and record/replay; and
* Python-authored nodes running inside native and mixed graphs.

Operators are exposed dynamically from the native registry. A name can
therefore be supported even when it is not present in ``hgraph.__all__``:

.. testcode::

   import hgraph as hg

   assert callable(hg.add_)
   assert "add_" in dir(hg)
   assert "add_" not in hg.__all__

Use the generated :doc:`../reference/python_api_inventory` when checking
whether a top-level name is available.

Removed implementation types
----------------------------

The names in this table were implementation mechanisms in 0.5 and are not a
supported 0.8 authoring surface.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - 0.5 name or pattern
     - 0.8 authoring approach
   * - ``HgTypeMetaData``
     - Use public type expressions such as ``TS[int]`` and reflection helpers
       from ``hgraph.reflection``. Parsed metadata is owned by the runtime.
   * - ``AbstractSchema``
     - Derive from ``CompoundScalar`` or ``TimeSeriesSchema``, or use a
       standard dataclass as a nominal scalar schema.
   * - ``SetDelta``
     - Read ``TSS`` changes with ``added()`` and ``removed()``; construct an
       explicit change with ``set_delta(added=..., removed=...)``.
   * - ``InjectableTypesEnum``
     - Annotate parameters with the concrete public injectable markers:
       ``STATE``, ``SCHEDULER``, ``CLOCK``, ``LOGGER`` or
       ``EvaluationEngineApi``.
   * - Concrete ``TimeSeries*Input`` and ``TimeSeries*Output`` classes
     - Annotate the logical type (for example ``TS[int]`` or ``TSD[K, V]``).
       The node receives a guarded native view appropriate to that type.
   * - Endpoint binding and parenting methods
     - Express topology while wiring with graphs, ``REF`` values, services and
       higher-order operators. Do not rebind or re-parent live endpoints.
   * - Python ``WiringObserver`` subclasses
     - Use the bound ``hgraph.test.WiringTracer`` or
       ``GraphConfiguration(trace_wiring=...)``.
   * - Mutating ``Graph`` and ``Node`` lifecycle methods
     - Run through ``run_graph``/``evaluate_graph`` and control execution with
       ``SCHEDULER`` or ``EvaluationEngineApi``.

Live runtime API baseline
-------------------------

The compatibility baseline is the public 0.5.41 surface, but runtime objects
must be checked as live callback values rather than only as top-level Python
classes. The 0.8 contract tests therefore inject and execute every supported
runtime family:

* all ``TS``, ``SIGNAL``, ``REF``, ``TSS``, ``TSD``, ``TSL``, ``TSB`` and
  ``TSW`` input and output views, including their collection ranges, deltas,
  child access and output mutation;
* ``STATE``, ``RECORDABLE_STATE``, ``SCHEDULER``, ``CLOCK``,
  ``EvaluationEngineApi``, ``GlobalState``, ``Traits``, ``LOGGER`` and
  ``NODE`` injectables;
* the read-only graph reached through ``NODE.graph``; and
* ``CompoundScalar.to_dict()`` and ``CompoundScalar.from_dict()``.

The same tests inspect the generated ``_hgraph.pyi`` declarations. This makes
a method that exists in native code but is absent at runtime—or a runtime
method whose typing signature is lost—a compatibility failure.

The audit restored the complete 0.5 scheduler API
(``next_scheduled_time``, ``is_scheduled``, ``is_scheduled_now``,
``has_tag()``, ``pop_tag()``, ``schedule()``, ``un_schedule()`` and
``reset()``), normal mapping methods on injected ``GlobalState``, read-only
``Traits`` injection, and the ``RECORDABLE_STATE.as_schema`` view.

Established exclusions
~~~~~~~~~~~~~~~~~~~~~~

Two 0.5 members are deliberately outside the supported Python interface:

* ``REF.value.output`` will not be supported. A reference token exposes only
  the safe ``is_empty``, ``has_output`` and observation-time ``is_valid``
  metadata. It never provides a path to the live, mutable output endpoint.
  Express output access through wiring or an explicit ``TS_OUT`` parameter.
* ``NODE.notify()`` is not exposed. Use the injected ``SCHEDULER`` for
  absolute, relative or tagged scheduling. The narrower
  ``NODE.notify_next_cycle()`` convenience remains available.

Both exclusions are asserted against live callback objects and the generated
``_hgraph.pyi`` stub so they cannot be reintroduced accidentally.

Items requiring an explicit compatibility decision
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following 0.5 runtime members are not silently classified as either
supported or obsolete. They expose native topology or execution ownership,
and restoring them could make callback-scoped objects unsafe or create a
second runtime control surface. Code that relies on one of these members needs
an explicit migration decision before moving to 0.8.

.. list-table::
   :header-rows: 1
   :widths: 27 34 39

   * - 0.5 surface
     - Current 0.8 position
     - Decision to make
   * - Input topology observation: ``parent_input``, ``has_parent_input``,
       ``bound``, ``has_peer``, an input's bound ``output`` and
       compound-reference items
     - Not exposed on callback views.
     - Decide whether read-only topology is a genuine extension API or should
       be replaced by owned diagnostics. This is separate from permitting
       topology mutation and from the permanently excluded
       ``REF.value.output`` path.
   * - Endpoint mutation: ``re_parent``, ``bind_output``, ``un_bind_output``,
       ``do_bind_output``, ``do_un_bind_output``, ``parent_output``,
       ``bind_input``, ``subscribe``, ``unsubscribe``, ``copy_from_*``,
       ``apply_result`` and ``mark_*``
     - Wiring and the C++ runtime own these operations. Public output views
       retain value/collection mutation, ``clear()``, ``invalidate()`` and
       ``can_apply_result()``.
     - The current migration is to express topology during wiring. A new
       public endpoint-management contract would require a first-class C++
       design rather than compatibility methods on Python wrappers.
   * - Node internals: ``signature``, ``scalars``, ``input``/``inputs``,
       ``start_inputs``, ``output``, ``recordable_state``, ``scheduler`` and
       ``error_output``
     - ``NODE`` provides identity/status and its graph.
       ``notify_next_cycle()`` is the only node-level scheduling convenience;
       explicit scheduling uses ``SCHEDULER``.
       Callback inputs, ``TS_OUT``/``RECORDABLE_STATE``, ``SCHEDULER`` and
       error-capture helpers provide task-specific access.
     - Decide whether a read-only inspection snapshot is needed by library
       writers. Direct setters and ``eval()`` remain executor responsibilities.
   * - Graph executor internals: ``engine_evaluation_clock``,
       ``evaluation_engine``, ``push_source_nodes_end``, ``schedule``,
       ``schedule_node()``, ``evaluate_graph()`` and ``copy_with()``
     - The graph view is read-only. ``EvaluationEngineApi``, ``SCHEDULER`` and
       ``Traits`` are injected independently.
     - Continue the migration unless a concrete extension cannot be expressed
       through those narrower contracts.
   * - Lifecycle transition flags and controls: ``is_starting``,
       ``is_stopping``, ``initialise()``, ``start()``, ``stop()`` and
       ``dispose()``
     - ``is_started`` is observable; lifecycle control is not exposed.
     - Read-only transition flags may be considered for diagnostics. Lifecycle
       mutation remains owned by the executor.
   * - Mutable traits: ``set_traits()`` and ``copy()``
     - Injected ``Traits`` provides ``get_trait()`` and ``get_trait_or()`` and
       is read-only after wiring.
     - If Python library writers need to author traits, add a wiring-time API;
       do not mutate graph metadata from a running callback.

Decorator and source changes
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - 0.5 spelling
     - 0.8 spelling
   * - ``pull_source_node``
     - Use ``generator`` for a timestamped iterator source.
   * - ``push_source_node``
     - Use ``push_queue`` for values supplied by callbacks or another thread.
   * - ``const_fn``
     - Run ordinary scalar Python during wiring when no time-series input is
       required; use ``const`` to publish its result as a time series.
   * - Direct runtime source/output construction
     - Declare a node or graph and let the native wiring planner allocate its
       endpoints and storage.

When porting a source, keep the scalar/time-series distinction explicit. A
scalar parameter is fixed while wiring; a ``TS[...]`` parameter can change
during the run.

Reduction zero and validity
---------------------------

The 0.8 associative ``reduce`` contract depends on the number of
**currently valid** collection values. The ``zero`` argument is the result for
an empty reduction and participates in the singleton case; it is not a general
left-fold initializer.

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Situation
     - 0.5 behaviour
     - 0.8 behaviour
   * - No valid values; ``zero`` supplied
     - A plain empty collection normally produced ``zero``, but keyed or
       mapped reductions could remain invalid while a child slot existed but
       had not yet produced a valid value.
     - Publish ``zero`` immediately. An existing but unresolved child slot is
       not a live reduction value.
   * - No valid values; ``zero`` omitted
     - Infer an operation-specific identity, such as ``0`` for addition.
     - Remain invalid. A previously valid reduction is invalidated when its
       last live value disappears.
   * - One valid value
     - Combine through the reduction tree and its zero/default leaves.
     - Return the value directly when ``zero`` is omitted; otherwise evaluate
       ``func(value, zero)``.
   * - Two or more valid values
     - Tree padding could apply a non-identity ``zero`` a
       capacity-dependent number of times.
     - Reduce only the valid values; ``zero`` is not an operand.

For example, this 0.8 graph publishes ``0`` even before any mapped or keyed
child has produced a value:

.. code-block:: python

   @hg.graph
   def total(values: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
       return hg.reduce(hg.add_, values, zero=0)

Omit ``zero`` (or pass ``None`` explicitly) when an empty reduction must be
invalid. That choice also invalidates the result if the collection later
becomes empty; it does not latch the first valid value. Code that must suppress
only the initial zero but publish zero after a later emptying needs an explicit
application-level liveness gate.

With a true identity value, the eventual aggregate is normally unchanged; the
main migration risk is output validity and first-tick timing. Re-test reductions
fed by ``map_``, ``mesh_`` or ``switch_`` where collection slots can exist
before their values become valid. Do not use a non-identity ``zero`` as an
associative accumulator seed. Use the ordered ``is_associative=False`` form
with a live zero input when a deterministic left fold is required. See
:ref:`python-operator-reduce` for the complete current contract.

Set and dictionary deltas
-------------------------

Node code should consume the logical change API rather than depend on a
particular delta object class:

.. testcode::

   from hgraph import TSS, compute_node, set_delta
   from hgraph.test import eval_node

   @compute_node
   def mirror_delta(values: TSS[int]) -> TSS[int]:
       return set_delta(added=values.added(), removed=values.removed())

   assert eval_node(mirror_delta, [
       {1, 2},
       set_delta(added={3}, removed={1}),
   ]) == [
       set_delta(added={1, 2}),
       set_delta(added={3}, removed={1}),
   ]

For ``TSD``, return ordinary delta dictionaries and use ``REMOVE`` or
``REMOVE_IF_EXISTS`` to remove keys. Current values remain available through
the time-series view.

Record/replay and test migration
--------------------------------

The old test-only ``record_to_memory``, ``replay_from_memory`` and
``set_replay_values`` helpers are replaced by the normal record/replay path:

* configure recording with ``set_record_replay_config`` and
  ``record_replay_scope``;
* retrieve recorded values with ``get_recorded_value`` or ``Run.recorded``;
* seed focused node tests with ``hgraph.test.eval_node``; and
* use ``Wiring.set_replay`` only in advanced custom harnesses.

DataFrame bitemporal column names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DataFrame record/replay uses ``__date_time__`` for evaluation time and
``__as_of__`` for revision time by default, as in 0.5. Configure both names
before wiring or directly writing a frame when an external schema requires
different columns:

.. testcode::

   import hgraph as hg

   with hg.GlobalState():
       hg.set_table_schema_date_key("event_time")
       hg.set_table_schema_as_of_key("observed_at")
       assert hg.get_table_schema_date_key() == "event_time"
       assert hg.get_table_schema_as_of_key() == "observed_at"

The names are scoped to the current ``GlobalState`` and are used by table
conversion and DataFrame storage metadata. Inside a Python node, declare a
``GlobalState`` injectable and pass it as ``global_state=`` when calling
``DataFrameStorage.write_frame`` directly; runtime code must not call
``GlobalState.instance()``.

The separate ``lower()`` frame-call interface defaults to ``date`` and
``as_of``; pass its ``date_col`` and ``as_of_col`` arguments when those frames
use another convention.

Diagnostics
-----------

Use ``GraphConfiguration(trace=True)`` for evaluation tracing and
``GraphConfiguration(trace_wiring=True)`` for wiring tracing. Profiling accepts
``profile=True`` or an ``EvaluationProfiler`` from ``hgraph.test``. The
``hgraph.debug`` package exposes owned diagnostic snapshots and the Inspector
UI; see :doc:`tools/inspector` and
:doc:`python/tasks/debug_and_profile`.

Adapting custom integrations
----------------------------

Code that only authors graphs and nodes should remain in Python. Code that
needs runtime storage, custom native operators or maximum per-tick performance
should use the public C++ API and expose a narrow Python binding when needed.
Do not recreate runtime semantics in a Python compatibility layer.

Migration checklist
-------------------

#. Replace imports of removed metadata, endpoint and lifecycle classes.
#. Move topology changes back to graph wiring.
#. Update source decorators and test-only recording helpers.
#. Review ``reduce`` calls for empty-input validity and explicit-zero timing.
#. Run focused graph tests with ``eval_node`` and compare multi-tick results.
#. Check the generated API inventory for dynamically exposed operators.
#. Validate live services, adaptors and record/replay in an installed wheel.

Detailed implementation-level compatibility decisions and parity evidence live
in :doc:`../developer_guide/parity_matrix` and
:doc:`../developer_guide/roadmap`. They are useful for maintainers; application
authors should treat the public reference and this migration guide as the
contract.
