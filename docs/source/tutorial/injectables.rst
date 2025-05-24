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

    assert eval_node(pulsar, [timedelta(microseconds=2)], __end_time__=MIN_ST+6*MIN_TD) == [False, None, True, None, True]

Notice the use of the ``SCHEDULER`` type to indicate the injectable of the scheduler API. When the code is evaluated
the scheduler is provided to the function. This provides the ability to schedule the function for evaluation using
the schedule method. The scheduler also provides a few other useful methods such as the the ``is_scheduled_now`` to
assist with identification of state such as if the node is currently scheduled, if the node was activated due to being
scheduled, as well as the ability to indicate when to schedule the node.

