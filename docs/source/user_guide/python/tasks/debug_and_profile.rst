Debug and profile a graph
=========================

Start with the least intrusive diagnostic that answers the question.

.. list-table::
   :header-rows: 1
   :widths: 38 62

   * - Need
     - Configuration
   * - See node evaluations and values
     - ``GraphConfiguration(trace=True)``
   * - See graph construction and overload selection
     - ``GraphConfiguration(trace_wiring=True)``
   * - Aggregate execution timings
     - ``GraphConfiguration(profile=True)``
   * - Keep a profile snapshot
     - Pass ``hgraph.test.EvaluationProfiler`` as ``profile``
   * - Inspect a live graph interactively
     - Wire ``hgraph.debug.inspector`` and install the web/Perspective extras
   * - Consume owned diagnostic rows
     - Register ``hgraph.debug.GraphDiagnostics`` as a lifecycle observer

Focused test diagnostics
------------------------

``eval_node`` accepts ``__trace__``, ``__trace_wiring__``, ``__profile__``,
``__observers__``, ``__trace_back_depth__`` and ``__capture_values__``. These
exercise the same native path as application execution.

When a run raises, increase ``trace_back_depth`` to retain more activation
context and enable ``capture_values`` only when the values are safe to log.
The captured information can contain application data.

Owned snapshots
---------------

Profiler and graph-diagnostics snapshots own their strings, values and Arrow
handles. They can be retained after execution. Callback arguments supplied to
a custom ``EvaluationLifeCycleObserver`` are guarded views; copy the ordinary
fields you need instead of retaining the view.

The Inspector is described in :doc:`../../tools/inspector`. It is useful for
interactive navigation; owned snapshots are a better fit for assertions,
metrics export and automated diagnostics.
