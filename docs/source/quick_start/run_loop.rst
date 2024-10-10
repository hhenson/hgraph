The run loop
------------

HGraph is an event processing package. The consequence of this is that, to do any useful
work, an event loop must be started and allowed to run. The event (or run) loop requires an
entry-point (or top level) graph to evaluate, as well as a configuration, which describes how the
run-loop should operate. For example:

.. code-block:: Python

    from hgraph import evaluate_graph, GraphConfiguration, graph

    @graph
    def my_main_graph():
        ...

    ...
    config = GraphConfiguration()
    evaluate_graph(my_main_graph, config)


The graph can be evaluated using one of two modes, namely: ``REAL_TIME`` or ``SIMULATION`` (the default).

These modes are set on the ``GraphConfiguration`` object. For example:

.. code-block:: Python

    from hgraph import GraphConfiguration, EvaluationMode
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME)

The real-time loop can process events prepared on separate threads, such as events from
web servers, messaging systems, etc.

The simulation mode is used to run backtest, this mode simulates the time
and uses event sources that are deterministic, such as databases, data-frames, etc.

The start and stop times can also be set. When running in real-time mode, time moves
based on the system clock (unless the graphs engine time is set to a point in the past, in which case it will
run the same as simulation mode until it catches up with the current time).

In simulation mode, time flows as fast as is possible and the graph stops evaluation once
the last event (within the end-time) is processed.

For more information about configuration and graph runner, see :doc:`../reference/graph_run_loop`.

For a more detailed description of the run-loop concept see :doc:`../concepts/run_loop_concept`.
