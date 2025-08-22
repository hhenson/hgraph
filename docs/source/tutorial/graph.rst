Graph
=====

Most actual coding in HGraph is intended to be using the ``graph`` decorator. This decorator indicates the function
is resposible for wiring together graph and nodes. Wiring describes the process of describing the configuration of nodes
and the linkages between the outputs and the inputs.

The use of the ``compute_node`` and related node decorators is really intended for extending the system behaviour with
new primitives. The ``graph`` is intended for describing the intended behaviour of the graph.

We have encoutered graphs before, but we will go through their behaviour in more detail now.

Basic example
-------------

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

Composition
-----------

In functional programming behaviour is often extended using a composition pattern. Here is an example of composition:

.. testcode::

    from hgraph import graph, TS, add_
    from hgraph.test import eval_node

    @graph
    def double(lhs: TS[int]) -> TS[int]:
        return add_(lhs, lhs)

    @graph
    def g(lhs: TS[int]) -> TS[int]:
        return double(lhs)

    assert eval_node(g, [1, 2, 3]) == [2, 4, 6]

In this simple example we create a new specialisation of ``add_`` called ``double``. Double composes ``add_`` to
compute the result.

Polymorphism
------------

This is largely implemented via the ``@operator`` decorator.

This opertor approach describes a concept and then support type and parameter based specialisations. We covered this
in the previous section. The key concept here is that the implementation can be different for each implementation and
the framework takes care of selecting the best match of implementation to call signature.

Another mechanism available is via the typing system, specifically through the use of ``TypeVar`` types and a useful helper
marker, the ``AUTO_RESOLVE``. Lets start with a simple example that simply uses ``TypeVars`` to templatise the function.


.. testcode::

    from hgraph import graph, TS, add_, SCALAR
    from hgraph.test import eval_node

    @graph
    def add(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
        return add_(lhs, lhs)

    @graph
    def g(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return add(lhs, rhs)

    # Use the function using the g wrapper (this calls add with ``int`` type)
    assert eval_node(g, [1, 2, 3], [1, 2, 3]) == [2, 4, 6]

    # Uses add by specifying the expected type (in this case float)
    assert eval_node(add[float], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == [2.0, 4.0, 6.0]


This shows the use of template (``TypeVar``) types that can be called with different types. The type-system will ensure
the constraints specified are honoured, in this case the ``lhs``, ``rhs`` and output types must be the same.

This does not do any special at this point, so lets consider the next step:

.. testcode::

    from hgraph import graph, TS, add_, SCALAR, AUTO_RESOLVE, cast_
    from hgraph.test import eval_node
    import pytest

    @graph
    def add(lhs: TS[SCALAR], rhs: TS[SCALAR], _tp: type[SCALAR] = AUTO_RESOLVE) -> TS[float]:
        if _tp is int:
            lhs = cast_(float, lhs)
            rhs = cast_(float, rhs)
        elif _tp is not float:
            raise RuntimeError(f"Can't handle this type: {_tp}")
        return add_(lhs, lhs)

    assert eval_node(add[int], [1, 2, 3], [1, 2, 3]) == [2.0, 4.0, 6.0]
    assert eval_node(add[float], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == [2.0, 4.0, 6.0]

    with pytest.raises(RuntimeError):
        eval_node(add[str], ["a"], ["b"])

Here we make use of the ``AUTO_RESOLVE`` marker. This requests the framework to resolve the type of the ``TypeVar``
and provide it to the function. Using the type it is possible to define different paths of behaviour.

.. note:: The ``AUTO_RESOLVE`` will work with graph and node decorated functions.


Higher Order Functions
----------------------

Another approach to provide for re-use and extensibility is the concept of higer order functions.

Graph code can accept other functions as paramters and then make use of these to provide flexibility of behavior.

Here is an example:

.. testcode::

    from hgraph import graph, TS, add_, sub_, SCALAR
    from hgraph.test import eval_node
    from typing import Callable

    @graph
    def apply_function(lhs: TS[SCALAR], rhs: TS[SCALAR],
                        fn: Callable[[TS[SCALAR], TS[SCALAR]], TS[SCALAR]]) -> TS[SCALAR]:
        return fn(lhs, rhs)

    assert eval_node(apply_function[int], [1, 2, 3], [1, 2, 3], add_[int]) == [2, 4, 6]
    assert eval_node(apply_function[int], [1, 2, 3], [1, 2, 3], sub_[int]) == [0, 0, 0]

This example is the most simplistic use of the the approach, but as can be seen we can pass a time-series
function, make use of it in the graph and write logic that can be extended by the user dynamically.

.. note:: It is not possible to pass a time-series function (or make use of one) inside of a node decorated
          function. Only graph decorated functions can accept time-series functions as inputs.

