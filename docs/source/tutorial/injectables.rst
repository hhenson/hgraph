Injectables
===========

The programming model of HGraph is functional, there are no classes and the functions are intended to pure. That is
the function should have no side effects, and have no embedded state. That said, there are a number of scenarios
where having access to the runtime-environment and an efficient means of tracking state is required.

The model is also to be declarative as much as is possible. So we follow a dependency injection model where dependencies
are defined as a function input that advises the runtime what is required.

State
-----

Let's start with the most basic, state. In functional programming state can be achieved through the use of feedback,
but that is somewhat heavy-weight approach to tracking state. We can achieve the same thing by using the state injectable.

.. note:: Since the state is provided by the framework, we can control what gets injected, thus the function itself is
      still pure virtual, but in practice we achieve the effect of stateful behaviour.

Let's extend the last example into ``take`` where the function returns the first ``n`` items. To do this, we need
to count the number of times the function has been activated. To keep count we require state. There are a couple of
methods to manage state, but the simplest for is to follow the pattern below:

.. testcode::

    from hgraph import compute_node, TS, STATE
    from hgraph.test import eval_node
    from dataclasses import dataclass

    @dataclass
    class TakeState:
        count: int = 0

    @compute_node
    def take(ts: TS[int], count: int = 1, _state: STATE[TakeState] = None) -> TS[int]:
        _state.count += 1
        if _state.count == count:
            ts.make_passive()
        return ts.value

    assert eval_node(take, [1, 2, 3, 4], count=2) == [1, 2, None, None]

Looking into the code we see the following:

1. The use of a dataclass object to define the shape and initialisation of the state object.

2. The state is declared by setting the type for ``_state`` to be ``STATE``. The name of this injectable is a
   recommended pattern, but in this case, not required. The use of the generic form ``[]`` tells the system what
   the shape of the state should be. In this case ``STATE[TakeState]`` makes the state be the shape of ``TakeState``.

3. Injectables are generally dealt with by setting their value to be None. The injectable is not intended to be set
   by the user, so making it a kwarg with None as it's value makes it clear that it is not required to be provided
   by the user.

Scheduling
----------

The next most common thing of interest is to be able to schedule the function for evaluation, lets consider a simple
timer style function, lets call it ``pulsar``, the function needs to generate a value on a repeated basis. Here is
an example of how this could be implemented:

.. testcode::

    from hgraph import compute_node, TS, SCHEDULER, MIN_ST, MIN_TD
    from hgraph.test import eval_node
    from datetime import timedelta

    @compute_node
    def pulsar(ts: TS[timedelta], _scheduler: SCHEDULER = None) -> TS[bool]:
        out = True if _scheduler.is_scheduled_now else False
        _scheduler.schedule(ts.value, tag="S")
        return out

    assert eval_node(pulsar, [timedelta(microseconds=2)],
            __end_time__=MIN_ST+6*MIN_TD) == [False, None, True, None, True]

Notice the use of the ``SCHEDULER`` type to indicate the injectable of the scheduler API. When the code is evaluated
the scheduler is provided to the function. This provides the ability to schedule the function for evaluation using
the schedule method. The scheduler also provides a few other useful methods such as the the ``is_scheduled_now`` to
assist with identification of state such as if the node is currently scheduled, if the node was activated due to being
scheduled, as well as the ability to indicate when to schedule the node.

Another thing to note is the use of ``__end_time__`` in this test. We need to do this as there is no natural end
of evaluation and as such, without setting the end-time, this will evaluate until ``MAX_ET``, which for all practical
purposes would be forever see :ref:`infinite_loop` in :doc:`common_pitfalls`.

Logging
-------

It is often considered as good practice to log messages describing status or events that are useful to understand
the evaluation path of your code. This can help with monitoring the code in production, as well as for forensics
when an issue is uncovered, using good logs can help trace where the issue occurred.

It would be possible for the code to just use the standard Python logging interface, but this creates uncontrolled
side effects, it is better to instead be able to indicate to the runtime that the function will emit logging information
by requesting the logger injectable. The logger provided is owned by the framework and allows for consistency, etc.

This is how we use the logger injectable.

.. testcode::

    from hgraph import compute_node, TS, LOGGER
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(ts: TS[int],  _logger: LOGGER = None) -> TS[int]:
        _logger.info("Value: %s", ts.value)
        return ts.value

    assert eval_node(my_compute_node, [1, 2, 3]) == [1, 2, 3]

This produces output along the lines::

    2025-05-25 11:24:59,054 [hgraph.eval_node_graph.my_compute_node][INFO] Value: 1
    2025-05-25 11:24:59,054 [hgraph.eval_node_graph.my_compute_node][INFO] Value: 2
    2025-05-25 11:24:59,054 [hgraph.eval_node_graph.my_compute_node][INFO] Value: 3


Access the Output
-----------------

Another very useful thing to do is to have access to the results that you have already produced. Not all code
needs access to the produced results, but if you did not have access to the output, some coding patterns would require
you to track the output as state which is obviously not a great usage pattern.

Let's consider the ``dedup`` function, this is easily implemented as follows:

.. testcode::

    from hgraph import compute_node, TS
    from hgraph.test import eval_node

    @compute_node
    def dedup(ts: TS[int],  _output: TS[int] = None) -> TS[int]:
        if _output.valid and _output.value == ts.value:
            return None
        return ts.value

    assert eval_node(dedup, [1, 1, 2, 2]) == [1, None, 2, None]

This is the only scenario where the name of the input variable is constrained. The output injectable is identified
by the kwarg ``_output``. The value of the type is not validated and the output will be provided to the function.

It is also possible to use::

    _output: TS_OUT[int] = None

The second form ensures your IDE will give you the ability to resolve output only code-completions on the type. This
is not required and is only a neatness.

The output time-series type has many of the same attributes of the input type (other than methods such as
make_active/passive which only make sense to the input type).

It is also possible to set values on the output directly, though it is generally better practice to leave the application
of values to follow the standard return path.

Also note how we first checked to see if the output was valid before any other checks on the output. This is important
especially for the first time the function is evaluated as the output would not have any value set at that point in time.

Engine Time
-----------

If you need to perform time-based computations, access to the current engine time is import. There are two key clocks
that code may be interested in accessing, the first if the engine-clock. This tell the code what the engine considers
as the current time, this time is based on the event or events that have caused this engine cycle. The other time of
interest is the wall clock. This is intended to represent the current time your computer reports, but when we run in
simulation mode, this is actually a virtual time that is actually the current engine time plus the time taken in real-time
to get to the point in time when the wall clock property is accessed, this gives the best simulation of real-time,
when not in real-time.

To access the time, lets consider a ``lag`` function which computed the run-behind a computation engine is experiencing.
This is computed as wall-clock time minus the engine time.

.. testcode::

    from hgraph import compute_node, TS, EvaluationClock, MIN_TD
    from hgraph.test import eval_node
    from datetime import timedelta

    @compute_node
    def lag(ts: TS[int],  _clock: EvaluationClock = None) -> TS[timedelta]:
        return _clock.now - _clock.evaluation_time

    assert eval_node(lag, [1])[0] > MIN_TD  # On a very fast computer this could fail

Here we see come of the use of the clocks ``now`` property representing the wall clock and ``evaluation_time``
representing the current evaluation time of the graph. This can be used to perform throttling operations as well
as other useful delay functions as well.

The Engine API
--------------

There are times when a node requires the ability to interact with the the evaluation engine. This can be achieved
through the use of the ``EvaluationEngineApi`` injectable.

There are a number of use-cases for using this injectable, for now we will consider the use of the start time and end
time properties.

.. testcode::

    from hgraph import compute_node, TS, EvaluationEngineApi, MIN_ST
    from hgraph.test import eval_node
    from datetime import timedelta

    @compute_node
    def remaining_run_days(ts: TS[bool],  _api: EvaluationEngineApi = None) -> TS[int]:
        return (_api.end_time - _api.evaluation_clock.evaluation_time).days

    assert eval_node(remaining_run_days, [True], __end_time__ = MIN_ST + timedelta(days=2)) == [2]

The engine api also provides the ability to request the engine to stop as well as other lower level api's.
For more information see :class:`hgraph.EvaluationEngineApi` in :doc:`../reference/injectables`.

There are a few other injectables, but they are intended more for framework developers or are currently
more experimental in nature.

