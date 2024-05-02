Injectable Attributes
=====================

Injectable attributes allow you to request for a number of useful objects to be
provided to the node function when it is evaluated.

STATE
-----

Since hgraph is a functional programming paradigm, it does not support 'state' in the 
traditional sense. There are no objects and no state. However, there are cases where
the notion of state is required. In hgraph state is injected into the function, the 
state can be thought of as feed-back property, where the user sets the state in one
activation and then receives the state in the next activation as an input.

The state itself operates as a dictionary with the ability to deference values using 
either the __getitem__ or __getattr__ methods.

The following is an example of a node that uses state:

```python
from hgraph import compute_node, TS, graph, run_graph, STATE
from hgraph.test import eval_node


@compute_node
def window(ts: TS[int], size: int, _state: STATE = None) -> TS[tuple[int, ...]]:
    window = _state.window
    window.append(ts.value)
    if len(window) == size:
        return tuple(window)


@window.start
def window_start(size: int, _state: STATE) -> STATE[int]:
    from collections import deque
    _state.window = deque(maxlen=size)
```

The example above is a common pattern, using start to intialise the state and then
using the state in the node function. It would be possible to simulate different
states by a testing engine to consider different paths without having to re-run
all routes through the node.

Also note that naming the argument as ``_state`` is not required, but following convention
is useful. Also using the _ to prefix the arg helps to indicate to the user of the
node that the argument is not expected to be provided. Setting the value to None is
to ensure that this becomes a kwarg and does not need to be provided by the user.


SCHEDULER
---------

The scheduler provides the ability to schedule the node for future times. This is 
another way to get ticks into the graph other than the traditional source node.
This does not so much as introduce data, but will cause the node to be scheduled.

An example of using the scheduler is as follows:

```python
from hgraph import compute_node, TS, graph, run_graph, SCHEDULER, STATE, MIN_TD

@compute_node
def lag(ts: TS[int], _scheduler: SCHEDULER = None, _state: STATE = None) -> TS[int]:
    """Lag the input by one time step."""
    out = None
    if _scheduler.is_scheduled:
        out = _state.last_value

    if ts.modified:
        _scheduler.schedule(ts.last_modified_time + MIN_TD)
        _state.last_value = ts.value

    return out


@lag.start
def lag_start(_state: STATE):
    _state.last_value = None
```

In this example, we attempt to send the value we receive by one time step.

We are using the ``is_scheduled`` property to determine if we are being called as a 
consequence of a scheduled request. This is similar to the ``modified`` property
of a time-series input.

We also use the ``schedule`` method to schedule the node for future evaluation.


EvaluationClock
---------------

The evaluation clock provides the ability to request the current evaluation time, 
as well as the concept of now.

Here is an example of using the evaluation clock:

```python
from hgraph import sink_node, TIME_SERIES_TYPE, EvaluationClock

@sink_node
def print_tick_time(ts: TIME_SERIES_TYPE, _clock: EvaluationClock = None):
    print("Tick time: ", _clock.evaluation_time)
    print("Now: ", _clock.now)
    print("Cycle Time: ", _clock.cycle_time)
```

This prints out the various times that are available from the evaluation clock.


EvaluationEngineApi
-------------------

The evaluation engine api provides access to the evaluation engine.
This is useful to interact with the engine, the most useful options being
able to register pre- and post-evaluation hooks.

Here is an example of using the evaluation engine api:

```python
from hgraph import compute_node, TS, EvaluationEngineApi

@compute_node
def register_hooks(ts: TS[int], _engine: EvaluationEngineApi = None) -> TS[int]:
    _engine.add_after_evaluation_notification(lambda: print(f"After evaluation [{ts.value}]"))
    _engine.add_before_evaluation_notification(lambda: print(f"Before evaluation [{ts.value}]"))
    return ts.value
```

_output
-------

This is the only injectable attribute that is defined by it's argument name rather than
the type associated to the argument name and provides access to the output time-series
value associated to the node.

An example is provided below:

```python
from hgraph import compute_node, TS, TS_OUT


@compute_node
def sum_(ts: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
    return _output.value + ts.value if _output.valid else ts.value
```

The output has all the properties of the time-series input, but also has the ability
to set the value.


LOGGER
------

The logger injectable attribute provides access to the logger object.

```python
from hgraph import TS, LOGGER, sink_node

@sink_node
def log(ts: TS[int], _logger: LOGGER = None):
    _logger.info(f"Logging: {ts.value}")
```

