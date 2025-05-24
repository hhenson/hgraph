The Compute Node
================

Whilst writing pure graph code is best [#f1]_, writing compute nodes (or occasionally sink nodes) is useful when
learning how the graph works as well as supporting complex use cases that may not be easily implemented using a graph.

Compute nodes wrap up behaviour that is triggered when the inputs are modified. A compute node is a decorated Python
function. All HGraph decorated functions require explicit typing of the inputs and the output. A compute node must
have at least one time-series input and one time-series output declared. For example::

    @compute_node
    def my_compute_node(ts: TS[int]) -> TS[int]:
        ...

A compute node can also have non-time-series (or scalar) values as well. This can be thought of as configuration for
the node. For example::

    @compute_node
    def my_compute_node(ts: TS[int], s: str) -> TS[int]:
        ...

In this case we accept ``s`` as an input. when calling the compute node, scalar values are captured and associated
to that instance of the compute node.

Node Instance
-------------

The compute node can be instantiated many times, if (i) all the inputs are the same and (ii) they are found in the same
evaluation graph, these will be conflated into a single node to reduce unnecessary computation.

For example::

    a = const(1)

    b = my_compute_node(a, "1")
    c = my_compute_node(a, "1")

In this case b and c refer to the same node instance.

.. uml::
    
    @startuml
    
    rectangle "b" as b
    rectangle "c" as c
    rectangle "my_compute_node('1')" as compute
    rectangle "const(1)" as const
    
    compute -up-> const
    b -up-> compute
    c -up-> compute
    
    @enduml

However, if any of the inputs differ, then we have multiple instances created. For example::

    a = const(1)

    b = my_compute_node(a, "1")
    c = my_compute_node(a, "2")

.. uml::

    @startuml

    rectangle "b" as b
    rectangle "c" as c
    rectangle "my_compute_node('1')" as compute1
    rectangle "my_compute_node('2')" as compute2
    rectangle "const(1)" as const

    compute1 -up-> const
    compute2 -up-> const
    b -up-> compute1
    c -up-> compute2

    @enduml

Let's create a simple compute node to experiment with it's behaviour.

.. testcode::

    from hgraph import compute_node, TS
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        return ts1.value + ts2.value

    assert eval_node(my_compute_node, [1, 2], [3, 4]) == [4, 6]

This code currently add's inputs from ``ts1`` and ``ts2``.

Timing
------

But what would happen if the timing of the ticks were different? Consider::

    eval_node(my_compute_node, [None, 1, 2], [3, None, 4])

In this case ``ts2`` will first tick with 3, and then, in the next engine cycle, ``ts1`` will tick with 1.
Finally, in the third evaluation cycle, ``ts1`` ticks with 2 and ``ts2`` with 4.

How, does the code evaluation proceed?
::

    eval_node(my_compute_node, [None, 1, 2], [3, None, 4]) == [None, 4, 6]

So the first tick does not produce an output. The reason for this is due to the default configuration of the
``compute_node``. The default configuration for a the compute node is to make all inputs active (make the node respond
to each modification of each input) and to assume that all inputs must be valid before calling the evaluation function.
Valid is defined as having a value set on the inputs corresponding output.

So in the example above we can see that only one input was valid on the first engine cycle (``ts2``). Thus the function
was not evaluated in the first engine cycle. In the next engine cycle ``ts1`` is modified making both inputs valid,
this results in the function being called and the result produced (4). Another important point to note is that the
inputs don't contain the values that have been modified in the current engine cycle only, but refer to the last computed
value of the output bound to the input.

If we were to look at the time-line of value it would look something like this:

+----------+---------+---------+---------+
| **tick** | **ts1** | **ts2** | **out** |
+----------+---------+---------+---------+
|     1    |         |     3   |         |
+----------+---------+---------+---------+
|     2    |     1   |     3   |     4   |
+----------+---------+---------+---------+
|     3    |     2   |     4   |     6   |
+----------+---------+---------+---------+

Where the blank spaces represent no value.

valid
-----

Perhaps we would prefer the behaviour where we treat no-value to imply 0 and then we could change the evaluation to
produce:

+----------+---------+---------+---------+
| **tick** | **ts1** | **ts2** | **out** |
+----------+---------+---------+---------+
|     1    |         |     3   |    3    |
+----------+---------+---------+---------+
|     2    |     1   |     3   |     4   |
+----------+---------+---------+---------+
|     3    |     2   |     4   |     6   |
+----------+---------+---------+---------+

To do this we need to adjust the code in two places, first we need to change the default validation behaviour and
secondly we are now responsible for ensuring a time-series input is in fact valid, these changes are shown below::

    @compute_node(valid=tuple())
    def my_compute_node(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        lhs = ts1.value if ts1.valid else 0
        rhs = ts2.value if ts2.valid else 0
        return lhs + rhs

To change the validation behaviour, the decorator option ``valid`` is set to be an empty tuple. This configures the
node to ignore validation checks before calling the wrapped function. This means that one of the inputs could be unset,
i.e. having no valid value when the function is called. It is now the functions responsibility handle that possible
scenario, which is where the additional logic comes in. Here we ask the time-series if it is valid, if it is we use
the value, otherwise we provide a default. In the general case your code needs to correctly handle the case where a
node could be invalid. The ``valid`` takes a tuple of values, we used an empty tuple to indicate we require none of the
inputs to be valid, but it is possible to indicate a subset of time-series inputs must be valid, this depends on your
use-case.

active
------

As mentioned previously, the default state of a node is that all inputs are marked as being active, or in other words,
the node will be scheduled for evaluation if any of the outputs bound to the inputs are modified.

But, what if we only wanted to be activated if one of the inputs was modified?
The related question may be: Why would you only want to be activated when one input was modified (or a subset of inputs)?
This second question is interesting and will be discussed in an aside at this section.

To control which inputs cause the node to be evaluated, there are two mechanisms, the first is similar to the
``valid`` attribute. Let's assume we wish to make ``ts1`` the only input that will trigger evaluation. Below is
the example::

    @compute_node(active=('ts1',))
    def my_compute_node(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        ...

This declares that the ``ts1`` input is to be active, and no others (in this case ``ts2``) will cause the node to be
activated. This tick table shows how this affects the output::

     eval_node(my_compute_node, [None, 1, None, 2], [3, None, 4, 5]) == [None, 4, None, 7]

In this example the output is only emitted when ``ts1`` ticks.

This can also be controlled programmatically. Consider the function ``first``, which returns the first value and
then stops emitting any further values, the implementation of this would look like:

.. testcode::

    from hgraph.test import eval_node
    from hgraph import graph, TS, debug_print

    @compute_node
    def first(ts: TS[int]) -> TS[int]:
        ts.make_passive()
        return ts.value

    assert eval_node(first, [1, 2, 3]) == [1, None, None]

In this example we receive the first tick and then we mark the input ``ts`` as passive. The node then stops being
activated from that input.



Aside
-----

Why would you only want some inputs marked active and others not?

To discuss this, let's take a example of a trade-acceptance node. In this scenario we make the following assumptions:

1. There is an input that represent the trade to accept.
2. We need to validate the price on the trade for price slippage.
3. We make use configuration to drive trade-acceptance.

The signature for the trade acceptance may take the form below::

    @compute_node(active=("trade",))
    def trade_acceptor(
        trade: TS[TradeRequest],
        market_data: TSD[InstrumentId, TS[BidAsk]],
        config: TS[TradeAcceptorConfig]
    ) -> TSB["accept": TS[TradeRequest], "reject": TS[TradeRequest]]:
    ...

If we did not restrict the activation of this node we will be called each time market data or configuration changed.
Since we are only interested in the values of these inputs when we receive a trade request. It is not desired to
be notified when the non-trade-request items are updated.
So, by setting the active input to be the ``trade`` input, we can ensure that we only get activated when we need to
produce a result. It is important to note that the other inputs that are not marked as active does not mean that the
value is not up-to-date. It just means that when the value is modified it does not cause the function to be evaluated.
Thus when the function is evaluated as a consequence of the TradeRequest being modified the other inputs will have the
most recent values present.


.. [#f1] If you are new to this style of computation and have not read all the other documentation, just ignore this
         opinionated statement for now. We will cover graph based logic as the tutorial progresses.

