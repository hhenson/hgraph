Unit Testing - Basics
=====================

Unit testing event based solutions requires a bit of extra facilitation. There are a number of different use cases
for testing, unit testing focuses on testing individual components or occasionally simple scenarios. Integration
testing is not considered in this section.

HGraph does not dictate the use of test frameworks, internally we make use of `pytest <https://docs.pytest.org>`_.
Instead HGraph provides a light weight set of tools to simplify event based testing.

Most code written is in the form of ``compute_node`` 's or ``graph`` 's that are in the form of a ``compute_node``.
That is the logic has time-series inputs and time-series outputs. What we want to test is: if, given a set of inputs,
the outputs are correct.

``eval_node``
-------------

To support this we introduce the ``eval_node`` function.

.. testcode::

    from hgraph.test import eval_node
    from hgraph import compute_node, TS

    @compute_node
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        return lhs.value + rhs.value

    def test_my_add():
        assert eval_node(my_add, [1, 2], [3, 4]) == [4, 6]

The ``eval_node`` function takes as the first parameter the ``graph`` or ``compute_node`` to evaluate. It then takes
a sequence of arguments that are passed to the function to evaluate. Time-series inputs are defined using a list of
values, each value produces a "tick" to the function. The output is the collected results converted to a list.
