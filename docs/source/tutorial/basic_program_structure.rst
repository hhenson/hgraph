Basic Programming Structure
===========================

The most basic form of an HGraph application is as follows:

.. testcode::

    from hgraph import graph, evaluate_graph, GraphConfiguration, const, debug_print
    from logging import INFO

    @graph
    def main():
        world = const("world")
        debug_print("Hello", world)

    evaluate_graph(main, GraphConfiguration(default_log_level=INFO))

Results in:

.. testoutput::

    ... Hello: world

If we ignore the imports, the application consists of a ``graph`` definition and the use of the ``evaluate_graph``
function.

``evaluate_graph``
------------------

HGraph is ultimately an event processor. It provides a functional style syntax and a library of types and functions
to assist with building efficient event handling logic. All event processing engines require an event loop to be
established, the event loop is used to dispatch events to the event handling logic. There are different types
of event handling loops in industry. In the case of the current implementation of HGraph, the model in use is a
single threaded while loop that will dispatch events in time-order.

The ``evaluate_graph`` is the function that will establish this event loop. It will not return until the loop is
considered as done.

The function takes at least two arguments, the first is the main graph to evaluate, the second is the configuration
used to setup and operate the event loop. The configuration contains a few key concepts, such as the ``run_mode``,
``start_time``, and ``end_time``.

``graph``
---------

The graph supplied to the ``evaluate_graph`` function is also often referred to as the main graph or the top-level
graph. This is effectively the program you wish to run. Typically this graph will define the key source nodes,
register any services, define the high-level business logic and the sink-nodes.

These graph's can take in scalar (non-time-series) parameters, typically the configuration information and can optionally
return a time-series result. When a result is returned the results is collected in memory and provided as a list of
tuples of time and value. For production code this should be avoided as it will create a memory overhead as the results
are collected in memory to be returned as the end of evaluation.

.. note::  The example provided is the smallest meaningful graph that can be constructed (i.e. a source node and a sink
           node). This is because if we only provided a source node, it will get pruned from the graph (we only compute
           values that are consumed, without a sink-node there are no consumers so the source node will be removed).

Aside
-----

The code described above is run as a doc-test, you can run this by copying and pasting into a console.
When running this as an application, follow standard Python patterns for running code, below is a very simple example
of how this may be done:

.. code-block::

    from hgraph import graph, evaluate_graph, GraphConfiguration, const, debug_print
    from logging import INFO

    @graph
    def main():
        world = const("world")
        debug_print("Hello", world)

    def main():
        evaluate_graph(main, GraphConfiguration(default_log_level=INFO))

    if __name__ == "__main__":
        main()

For more advanced applications there may be a requirement to parse args, etc. HGraph provides no features to handle
these requirements, instead use your favourite tools to perform these tasks.
