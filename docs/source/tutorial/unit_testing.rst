Unit Testing - Basics
=====================

Unit testing event based solutions requires a bit of extra facilitation. There are a number of different use cases
for testing, unit testing focuses on testing individual components or occasionally simple scenarios. Integration
testing is not considered in this section.

HGraph does not dictate the use of test frameworks, internally we make use of `pytest <https://docs.pytest.org>`_.
Instead HGraph provides a light weight set of tools to simplify event based testing.

Most code written is in the form of ``compute_node`` 's or ``graph`` 's. The graph typically take some inputs and return
a result, it is possible to evaluate other shapes, but these are the most common for unit testing.

What we want to test is: if, given a set of inputs, that the outputs are correct.

The ``eval_node`` Function
--------------------------

To support this we introduce the ``eval_node`` function.

.. testcode::

    from hgraph.test import eval_node
    from hgraph import compute_node, TS

    @compute_node
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        return lhs.value + rhs.value

    def test_my_add():
        # Will output: [4, 6] - first tick: 1+3=4, second tick: 2+4=6
        assert eval_node(my_add, [1, 2], [3, 4]) == [4, 6]

The ``eval_node`` function takes as the first parameter the ``graph`` or ``compute_node`` to evaluate. It then takes
a sequence of arguments that are passed to the function to evaluate. Time-series inputs are defined using a list of
values, each value produces a "tick" to the function. The output is the collected results converted to a list.

The ``eval_node`` function runs in SIMULATION mode and by default will start and ``MIN_ST`` (the constant defining the first
possible start time, which is currently 1970-01-01 00:00:00.000001). Each entry in the list will be introduced to the
graph in ``MIN_TD`` increments (in Python this is 1 microsecond intervals). In no tick is expected to be produced, use None
to indicate this. Also, by default, the results are collected into an array of ``MIN_TD`` increments. When nothing changed
a ``None`` will be placed in the output list.

For example:

.. testcode::

    from hgraph.test import eval_node
    from hgraph import compute_node, TS

    @compute_node
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        return lhs.value + rhs.value

    def test_my_add():
        # Will output: [None, 6] - first tick: no output due to None, second tick: 2+4=6
        assert eval_node(my_add, [None, 2], [3, 4]) == [None, 6]

In this case we have included a None if the ``lhs`` input, since the node (using the defaults) requires both inputs to
be valid before computing a result, thus the first engine cycle will produce no output. Thus we get ``None`` as a
response for the first tick.

At times this behaviour is not desirable, and it is more convenient to only observe when a value is in fact modified.
Examples of this include when dealing with non-deterministic events or those where the timing between events is large.
Where say we are testing a delay component with a second delay, this would result in an array with 999,999 ``None`` s between
meaningful events. For these scenarios we support the ``__elide__`` option.

.. testcode::

    from hgraph.test import eval_node
    from hgraph import compute_node, TS

    @compute_node
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        return lhs.value + rhs.value

    def test_my_add():
        assert eval_node(my_add, [None, 2], [3, 4], __elide__=True) == [6]

Now we only get the actual ticked values inserted into the output list.

Sometimes there are issues in the code and it is not obvious what the problem is. When debugging compute nodes, it is
possible to just put a break-point in the code and debug, but when testing more complicated graphs, it may be difficult
to determine what is going on. There are two extreme options that can be used to trace exactly what is going on in
the graph. These are: ``__trace__`` and ``__trace_wiring__``. These both log detailed tracing messages that can be
helpful to debug the code. The ``__trace_wiring__`` is useful to determine which override an operator is selecting and
why. The ``__trace__`` parameter will create a trace of each step the graph takes during evaluation, including which
includes life-cycle's such as grah and nodes being started, evaluated, and stopped.

During evaluation it is possible to see which nodes are evaluated, what inputs are marked as modified along with the
input value and the result produced (if any result is produced). This can help to identify why a particular issue is
created. However, this is very detailed and can be a bit overwhelming in complicated scenarios.

Using these options just requires setting the attributes to True.

There are also options to adjust the start and end times when required to validate the codes behaviour, when setting
the start and end times, elide is usually set true as well.

The ``debug_print`` Function
----------------------------

Another useful probe when trying to trace issues in code is the ``debug_print`` operator. This takes a label and a
time-series and will print out the value of the time-series each time it ticks.

This can be useful to place at different points in the flow to create a targeted probe to inspect behaviour.

For example:

.. testcode::

    from hgraph.test import eval_node
    from hgraph import graph, TS, debug_print

    @graph
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        out = lhs + rhs
        debug_print("lhs", lhs)
        debug_print("rhs", rhs)
        debug_print("lhs + rhs", out)
        return out

    def test_my_add():
        assert eval_node(my_add, [None, 2], [3, 4], __elide__=True) == [6]

This produces output that would look something like this::

    2025-05-20 12:28:11,912 [hgraph][DEBUG] Wiring graph: eval_node_graph()
    2025-05-20 12:28:11,916 [hgraph][DEBUG] Creating graph engine: EvaluationMode.SIMULATION
    2025-05-20 12:28:11,917 [hgraph][DEBUG] Starting to run graph from: 1970-01-01 00:00:00.000001 to 2299-12-31 23:59:59.999999
    [1970-01-01 00:00:00.000229][1970-01-01 00:00:00.000001] rhs: 3
    [1970-01-01 00:00:00.000036][1970-01-01 00:00:00.000002] lhs: 2
    [1970-01-01 00:00:00.000059][1970-01-01 00:00:00.000002] rhs: 4
    [1970-01-01 00:00:00.000089][1970-01-01 00:00:00.000002] lhs + rhs: 6
    2025-05-20 12:28:11,917 [hgraph][DEBUG] Finished running graph

The output is sent to std out, it includes the wall clock (which in simulation mode is simulated) and the engine time
next, finally the label and the value of the time-series is presented.

This is really useful to pin-point where, in a complex flow, things are not behaving as expected.

It is also reasonably easy to scan the code base for the debug_print to remove prior to putting the code into production.
As a rule of thumb it is recommended to not commit code with debug statements in it.

``breakpoint_``
---------------

When working with nodes (e.g. ``compute_node`` and ``sink_node``) it is possible to place a break-point in your code
much as you would in any python code. However, as your use of the framework matures, most of the code is likely to be
written as ``graph`` code, once you do this, the drawback is that putting a breakpoint in your code is only hit during
the wiring phase of the evaluation cycle.

To assist with breaking into the evaluation flow in a graph, there is the ``breakpoint_`` operator. Here is an example
of this:

::

    from hgraph.test import eval_node, breakpoint_
    from hgraph import graph, TS

    @graph
    def my_add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        # The code we want to test
        out = lhs + rhs
        out = breakpoint_(out)
        return out

    def test_my_add():
        assert eval_node(my_add, [None, 2], [3, 4], __elide__=True) == [6]

What will occur is that each time ``out`` ticks the code will break inside the ``breakpoint_`` operator.
This will give access to the time-series input value. From that the rest of the graph and it's values are reachable
via the debugger.

The general usage pattern is to use the operator as a pass-through (as in the example). This ensures the break-point
will be reached in the correct rank order (i.e. after the value is created and before it is consumed).

There are a couple of other variations that can be used, namely:

Conditional
    ``breakpoint_(condition, value)`` where the break-point is only triggered when the condition or value changes and
    the value of the condition is ``True``.

Many
    ``breakpoint_(**kwargs)`` where many inputs can be provided and the operator will break each time any one of the
    inputs is modified.

