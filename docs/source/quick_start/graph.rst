graph
=====

A graph has two meanings, depending on context. In one context a graph represent the
set of nodes that are evaluated by the evaluation engine. In the coding context, the
graph represents a wiring function that describes the relationship between nodes.

We use decorators to mark functions to indicate the intent of the function.

In this case a graph looks as follows:

.. code-block::

    @graph
    def my_graph():
        ...

All function signatures in hgraph are required to be typed using type-hints. For example:

.. code-block::

    @graph
    def my_graph(ts: TS[int], v: str) -> TS[str]:
        ...

In this example we take in two inputs and return a time-series value of type string.

Code in the graph is only ever evaluated at wiring time (when the graph is built) and
prior to evaluation. This describes the relationships between nodes.

A simple example is below:

.. code-block::

    from hgraph import graph, const, debug_print

    @graph
    def my_graph():
        c = const("World")
        debug_print("Hello", c)

In this scenario we are constructing two nodes, the ``const`` which will tick one
at the start of the evaluation and the output of the ``const`` this output is then
passed to the input of ``debug_print`` (another node). This produces a graph with
two nodes and one edge.

