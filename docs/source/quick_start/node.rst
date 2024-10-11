node
====

A node is the key evaluable element in the graph. There are three key types of nodes, namely:

* Source - Initiates ticks (or events) into the graph
* Compute - Processes inputs and produces outputs.
* Sink - Accepts inputs and does not produce any outputs (takes events off of the graph).

source_node
-----------


In Python, source nodes are typically implemented using the ``generator`` decorator.

For example:

.. code-block:: Python

    from hgraph import generator, TS, MIN_ST, MIN_TD

    @generator
    def my_generator() -> TS[int]:
        for i in range(10):
            yield MIN_ST + i * MIN_TD, i

As can be seen, this has no time-series inputs and produces a time-series of integer values.
The generator will be created at start time and will yield a sequence of times and values.
The times are when the value is to be scheduled for delivery.

There are a number of standard source nodes in the graph library, it is quite unlikely that
users of the library will be writing generators, but would instead uses those provided
by the library.

.. note:: Generators are PULL source nodes, these are not suitable for handling asynchronous sources.

For more details on pull source nodes see: :doc:`../concepts/node_based_computation`

To handle asynchronous sources, there is the concept of a PUSH source node. These nodes
are used to process events that occur at times that are not pre-determined. The best
method in Python is to use the ``push_queue`` decorator. This is depicted below:

.. code-block:: Python

    from hgraph import push_queue, TS

    def _user_input(sender: Callable[[str], None]):
        while True:
            s = sys.stdin.readline().strip("\n")
            sender(s)
            if s == "exit":
                break

    @push_queue(TS[str])
    def my_push_queue(sender: Callable[[str], None]) -> TS[str]:
        threading.Thread(target=_user_input, args=(sender,)).start()

The function accepts at least one parameter, namely the sender. This is a callable
that accepts a single parameter (of the scalar value associated with the time-series
type). The decorator takes the type of the queue.

The function associated to the decorator will be called during the start life-cycle.
This is the time to create the thread, event-loop or whatever is required to initiate
the asynchronous behaviour.

It is then possible to call the sender each time a new event is to be dispatched. The
event will be queued and delivered within the graph's event loop. Push events are
delivered as soon as is practical, the event time will be the time the event loop is
started (or the next engine time when running behind). For more information refer to
:doc:`../concepts/node_based_computation`.

compute_node
------------

The compute node is the work horse of the nodes. These nodes accept inputs and produce
a time-series result (or output). The decorator is ``compute_node``.

An example is below:

.. code-block:: Python

    from hgraph import compute_node, TS

    @compute_node
    def my_compute_node(a: TS[int], b: TS[int]) -> TS[int]:
        return a.value + b.value

In this example we have a compute node that takes two time-series inputs and returns a
single time-series response. Time-series inputs are time-series types. The time-series
type have properties, in this case one of the properties is the value, representing last
value that was placed in the time-series.

See :doc:`../concepts/node_based_computation` for more details on compute nodes.

sink_node
---------

The sink node is the last key node type. The sink node is very similar to the compute node,
with the exception that it does not have an output. The sink node is responsible for
consuming nodes in a graph and forms the leaves in the DAG.

.. code-block:: Python

    from hgraph import sink_node, TS

    @sink_node
    def my_sink_node(a: TS[str]):
        print(a.value)

Here the sink node takes the value and prints it out. In all cases, no further ticks
are produced. Once all the sink nodes are processed, the graph evaluation is marked as complete
and the next cycle is started (or we will wait until we are ready to start the next cycle).



