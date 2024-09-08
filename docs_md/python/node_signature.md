Node Signature Extension
========================

The node signatures extend the component signature in their ability to
mark interest in special arguments to be provided as inputs.

These special inputs are only useful to runtime behaviour and as such, are
not meaningful to graph components.

These special inputs include:

* _clock: ``EvaluationClock``
* _engine: ``EvaluationEngineApi``
* _state: ``STATE``
* _scheduler: ``SCHEDULER``
* _log: ``LOGGER``   
* _output: ``TSOut[str]`` (for example)

In all cases, except for ``_output``, the names of the special inputs are not important, but for consistency
follow the naming patterns when possible.

In the case of ``_output``, the name is what is used to identify the special input and the type must
be compatible with the output of the component.

In all cases the special input should be followed by ``= None`` as the framework wil inject the value
into the method at runtime. This should not be set by users of the code. For example:

```python
@generator
def const(value: SCALAR, _engine: EvaluationEngineApi=None) -> TIME_SERIES_VALUE:
    ...
```

The runtime engine will supply the instance when called by the runtime, but these can never be supplied
during the wiring phase, as such setting these to None as kwargs allows the
wiring builder detect misconfigurations more easily, using an ``_`` before the name (i.e. ``_clock``) 
will help hide this property from the users of the nodes.

EvaluationClock
---------------

The evaluation clock provides access to the evaluation engines clock, this exposes useful
such as:

* evaluation_time - The time the engine is evaluating the graph for.
* now - The wall clock time as of now, in simulation mode this is not the computer clock.
* cycle_time - The time taken till now to evaluate this loop (or wave) of the graph.

EvaluationEngineApi
-------------------

Provides access to the engines public API. This provides information such as:

* start_time - The time when the engine was configured to start.
* end_time - The time the engine was configured to stop.
* evaluation_clock - The evaluation clock for this engine.
* is_stop_requested - If the engine has been requested to stop.

This also provides the ability to interact with the engine such as:

* request_engine_stop - Allows the node to stop the running engine.
* add_before_evaluation_notification - Add a callback to be called before the next evaluation cycle.
* add_after_evaluation_notification - Add a callback to be called after the current evaluation cycle.
* add_life_cycle_observer - Add a life-cycle observer to the engine
* remove_life_cycle_observer - Remove a previously registered life-cycle observer.

STATE
-----

Given the paradigm of evaluation of the graph is functional, all behaviour is
stateless and should be idempotent. However, we also live in a stateful world,
this allows the node to track state, in a stateless way. That is, the node
is not the owner of it's state, but rather the state is provided to the node.
For the most part the state provided is the state computed during the last
evaluation of the node, but this can also be managed to deal with testing
with assumed prior states, etc.
The STATE object is effectively a dictionary from the users point of view,
with the ability to access keys using ``.<key>`` type syntax as well as ``['key']`` 
syntax. The user can think of it as ``self``.

Another way of thinking about ``STATE`` is to think of it as a feedback cycle, where
state is injected in and modifications of state are returned out of the node and then
fed back in during the next evaluation cycle.


SCHEDULER
---------

This provides access to the nodes scheduling logic, allowing the node to add scheduling
requests. The basic functionality is:

* is_scheduled - True if the node was activated due to a schedule request.
* schedule - Schedule the node for future evaluation at a particular time or time-delta.
* un_schedule - Remove a previous scheduling event, this only works with tagged scheduled items.
* pop_tag - Removes and returns a previous scheduled event with the tag name.
* has_tag - Indicates if the scheduler has a tag currently schedule.
* next_scheduled_time - The time the node is next scheduled.
* reset - Removes all pending scheduled events (both tagged and non-tagged events)


LOGGER
---------

This provides access to the system logger.


_output
-------

Ofttimes it useful to be able to see the previous state of the output when computing
a new result, having access to the values of the outputs reduces anti-patterns where
values are cached in state that could be read directly from the output node.

The output can be directly typed using the <time-series-type>Out syntax to identify
the output or use ``TIME_SERIES_TYPE``, etc. to type the input variable. Remember in this
one case it is the input argument name that defines the fact that the node is interested
in having the value injected, not the associated type, unlike the other injectable inputs.

In either case the output will be provided, but in the former, the IDE will be able to
resolve the type signature and be able to provide auto-completion support. The latter mode
will only have the most basic signature support.

For example:

```python

@compute_node
def count(ts: TIME_SERIES_TYPE, _output: TSOut[int]) -> TS[int]:
    return _output.value + 1
```

Using the output it is also possible to set the output directly, for example:

```python

@compute_node
def count(ts: TIME_SERIES_TYPE, _output: TSOut[int]) -> TS[int]:
    _output.value += 1
```

In which case nothing is returned, but the output is updated. If the output is updated
multiple times during the evaluation of the node, only on final *tick* is generated, the value
of which is dependent on the output type, for example a TS type will only have the last
value set being available, but a TSS type will show the net state of operations performed
during the evaluation of the node.
