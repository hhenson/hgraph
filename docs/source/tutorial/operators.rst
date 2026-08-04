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

    @compute_node(overloads=add_, requires=lambda m, __strict__: __strict__ == True)
    def add_strict(lhs: TS[int], rhs: TS[int], __strict__: bool) -> TS[int]:
        return lhs.value + rhs.value

    @compute_node(overloads=add_, valid=(), requires=lambda m, __strict__: __strict__ == False)
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

In this example, we define an ``add_`` operator. The operator itself has no body, the ``...`` is
all there is. It exists to declare the name, the shape of the signature and the intent. Nothing
calls the operator's body, so an operator on its own cannot be evaluated.

We then provide two implementations, both marked with ``overloads=add_``. Each is a normal
``compute_node``, and each narrows the operator's signature: where the operator accepts any two
time-series types, both implementations accept ``TS[int]``. This narrowing is how the framework
knows which implementations are candidates for a given call.

Two candidates match the call in ``g``, so something more than the types is needed to choose
between them. That is what the ``requires`` clause does. It is called at wiring time and returns
``True`` if this implementation is applicable. Here ``add_strict`` requires ``__strict__`` to be
``True``, and ``add_not_strict`` requires it to be ``False``, so exactly one of them will ever
apply.

The difference between the two implementations is in how they handle an input that has not ticked
yet. ``add_strict`` takes the default behaviour, which is that all inputs must be valid before the
function is called. ``add_not_strict`` sets ``valid=()``, so it is called as soon as either input
ticks and takes responsibility for checking validity itself. That is why the two assertions produce
different results for the same inputs:

+----------+---------+---------+--------------------+------------------------+
| **tick** | **lhs** | **rhs** | **strict output**  | **non-strict output**  |
+----------+---------+---------+--------------------+------------------------+
|     1    |     1   |         |                    |            1           |
+----------+---------+---------+--------------------+------------------------+
|     2    |     1   |     2   |          3         |            3           |
+----------+---------+---------+--------------------+------------------------+

Resolution rules
----------------

When more than one overload matches, the framework selects the most specific one. In broad terms
the ranking works as follows:

1. Any implementation whose signature will not resolve against the supplied arguments is rejected.
   A ``requires`` clause returning ``False`` (or a message) rejects the implementation in the same
   way.
2. Of those remaining, the implementation with the most specific type signature wins. A signature
   using concrete types (``TS[int]``) is more specific than one using a constrained type-var
   (``TS[NUMBER]``), which in turn is more specific than an unconstrained one
   (``TIME_SERIES_TYPE``).
3. If the two best implementations are equally specific, this is an error and the wiring reports
   an ambiguous overload, listing the tied candidates.

The operator itself is never a candidate. Its body is not called, so if nothing matches the wiring
fails with a "no matching candidates found" error listing every implementation that was rejected
and why. If you want a catch-all, register a deliberately generic overload; it will rank worst and
so only be selected when nothing more specific applies.

.. note:: When an overload is not being selected as expected, run the graph with
          ``__trace_wiring__=True``. This reports each candidate considered and why it was
          accepted or rejected, which is normally quicker than reasoning about it from the
          signatures alone. See :doc:`unit_testing`.

Why use operators
-----------------

Operators are the mechanism behind the arithmetic that graph code takes for granted. When you write
``a + b`` in a graph, the wiring layer rewrites it as ``add_(a, b)`` and the resolution above picks
the implementation appropriate to the types of ``a`` and ``b``. That is how the same ``+`` works for
``TS[int]``, ``TS[float]``, ``TSD[str, TS[float]]``, and so on, without any of them knowing about
each other.

The same route is open to your own code. If you define a new time-series shape, implementing the
existing operators for it makes it work with the library's generic code without modifying any of
that code.

.. testcode::

    from hgraph import compute_node, graph, TS, add_
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from hgraph import CompoundScalar

    @dataclass(frozen=True)
    class Money(CompoundScalar):
        amount: float
        ccy: str

    @compute_node(overloads=add_)
    def add_money(lhs: TS[Money], rhs: TS[Money]) -> TS[Money]:
        l, r = lhs.value, rhs.value
        assert l.ccy == r.ccy, "Cannot add different currencies"
        return Money(amount=l.amount + r.amount, ccy=l.ccy)

    @graph
    def g(lhs: TS[Money], rhs: TS[Money]) -> TS[Money]:
        return lhs + rhs

    assert eval_node(g, [Money(1.0, "USD")], [Money(2.0, "USD")]) == [Money(3.0, "USD")]

Note that ``g`` uses ``+`` and never mentions ``add_money``. Registering the overload was enough.