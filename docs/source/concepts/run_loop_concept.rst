Run Loop Concept
================

The run-loop is central to the HGraph package. The run-loop is the mechanism used to
dispatch events (or ticks) into the graph for evaluation. The run-loop is a similar to
an operating systems process scheduler.

The run loop has two key modes of operation, namely:

* ``SIMULATION`` - A mode used to run through scheduled events as fast as possible
* ``REAL_TIME`` - The mode to use when scheduling events to occur at roughly true time.

Typically ``SIMULATION`` is used to run testing of the graph's behaviour. Whereas ``REAL_TIME``
is used to run the production system. The ``REAL_TIME`` mode of operation can be set to
start in the past, this allows for a form of recover / warm up prior to running the graph.
When the time is in the past, the graph will advance time in the same mannor as with
``SIMULATION``.

Nodes in the graph are scheduled in topologically ranked order. The topology is defined
by the connection of nodes (since the connections are directed, this forms a Directed
Acyclic Graph) the rank is the max count of connections from source to current node.

Sink nodes are placed at the max rank (although this is not guaranteed, the only guarantee
is that the nodes are evaluated in order to ensure that nodes with a smaller rank are evalauted
prior to nodes of a higher ranking).

The operations of the run loop can be observed using an instance of ``EvaluationLifeCycleObserver``.
This has callback methods that can be implemented to be notified of each key step the graph makes
as it evaluates itself.

Life-Cycle
----------

The life-cycle of the graph is as below:

.. plantuml::

    @startuml
    [*] --> Starting : start()
    Starting --> Evaluating
    Evaluating --> Stopping : stop()
    Stopping --> [*]
    @enduml

The graph as well as each node in the graph are taken through this basic life-cycle.

The master graph is started when the run-loop is started. Nested graphs (such as those used
to implement ``map_``, are started when they are brough into existence, this is data driven.

The observer will be notified prior to the graph starting or stopping calling the methods:

* ``on_before_start_graph(self, graph: Graph)``
* ``on_after_start_graph(self, graph: Graph)``

and

* ``on_before_start_node(self, node: Node)``
* ``on_after_start_node(self, node: Node)``

for start and the counterpart method for stop.

Once the graph has been taken though the start life-cycle. The evaluation loop will
run, in all modes, the core loop is roughly as below:

.. code-block:: Python

    current_time = start_time
    while current_time <= end_time:
        self.evaluate_graph()
        next_time = next_scheduled_time()
        self.wait_until(next_time)
        current_time = next_time

In the real-time engine the wait will wait until the time is reached and in the simulation
mode this will return immediately.

.. note:: This is not the actual code, just the conceptual view of how it works.

