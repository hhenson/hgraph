Graph
=====

Most actual coding in HGraph is intended to be using the ``graph`` decorator. This decorator indicates the function
is resposible for wiring together graph and nodes. Wiring describes the process of describing the configuration of nodes
and the linkages between the outputs and the inputs.

The use of the ``compute_node`` and related node decorators is really intended for extending the system behaviour with
new primitives. The ``graph`` is intended for describing the intended behaviour of the graph.

We have encoutered graphs before, but we will go through their behaviour in more detail now.

To start with lets create a simple graph, note the graph follows the same signature pattern as for nodes.

.. testcode::

    from hgraph import graph, TS, add_
    from hgraph.test import eval_node

    @graph
    def g(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return add_(lhs, rhs)

    assert eval_node(g, [1], [None, 2]) == [None, 3]

In this example we are using the :func:`add_ <hgraph.add_>` library node. This does much the same as what we saw in the
:doc:`operators` tutorial section.

Note, the signature structure is the same for nodes, this allows for nodes and graph to be interchanged, that is the
user may start with a node based implementation of some logic and then re-factor the logic into a graph, or visa-versa.

The graph signature does not support property injection, that is the use of injectables (such as loggers, state, etc.)
are not supported in the graph signature. These are only for use in nodes.

