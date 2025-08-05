Operators
=========

Generics provide the ability to template the types of a function, operators provide the concept of function polymorphism.

The ``operator`` decorator describes an abstract function, this can be thought of as an interface or contract definition.
The operator describes a function name and it's intended interpretation (i.e. the function it is expected to perform).
The operator will also describe the expected inputs and outputs of the function.

The operator is then overridden by actual implementations, the implementation should start with the name of the function
being overridden and then a short descriptive extension to the name. The engine resolves the correct implementation
based on the value of it's inputs.

Here is an example:

.. testcode::

    from hgraph import compute_node, TS, operator, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2, graph
    from hgraph.test import eval_node

    @operator
    def add_(lhs: TIME_SERIES_TYPE_1, rhs: TIME_SERIES_TYPE_2) -> TIME_SERIES_TYPE_1:
        ...

    @compute_node(overloads=add_, requires=lambda m, s: s["__strict__"] == True)
    def add_strict(lhs: TS[int], rhs: TS[int], __strict__: bool) -> TS[int]:
        return lhs.value + rhs.value

    @compute_node(overloads=add_, valid=(), requires=lambda m, s: s["__strict__"] == False)
    def add_not_strict(lhs: TS[int], rhs: TS[int], __strict__: bool) -> TS[int]:
        if lhs.valid and rhs.valid:
            return lhs.value + rhs.value
        elif lhs.valid:
            return lhs.value
        else:
            return rhs.value

    @graph
    def g(lhs: TS[int], rhs: TS[int], __strict__: bool) -> TS[int]:
        return add_(lhs, rhs, __strict__=__strict__)

    assert eval_node(g, [1], [None, 2], __strict__=True) == [None, 3]
    assert eval_node(g, [1], [None, 2], __strict__=False, ) == [1, 3]

In this example, we define an ``add_`` operator.