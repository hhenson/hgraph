Node Signature
==============

The node signatures extend the component signature in their ability to
mark interest in special arguments to be provided as inputs.

These special inputs are only useful to runtime behaviour and as such are
not meaningful to graph components.

These special inputs include:

* context: ``ExecutionContext``
* state: ``NodeState``
* output: ``TSOut[str]`` (for example)

The names of the arguments are user defined, but following the suggested
names makes it easier to read the code (where sensible).

These special variable must appear in the formal kwargs sections and be defined
to have None as the default value, for example:

```python
@generator
def const(value: SCALAR, context: ExecutionContext=None) -> TIME_SERIES_VALUE:
    ...
```

The runtime engine will supply the instance values, but these can never be supplied
during the wiring phase, as such setting these to None as kwargs allows the
wiring builder detect misconfigurations more easily, and the kwarg aspect
hides the value from the user.

ExecutionContext
----------------

The execution context provides access to information about the state of
the running graph, this includes useful attributes such as:

* current_engine_time - The time the engine is evaluating the graph for.
* wall_clock_time - The wall clock time as of now, in simulation mode this is not the computer clock.
* engine_lag - The time taken till now to evaluate this loop (or wave) of the graph.

NodeState
---------

Given the paradigm of evaluation of the graph is functional, all behaviour is
stateless and should be idempotent. However, we also live in a stateful world,
this allows the node to track state, in a stateless way. That is, the node
is not the owner of it's state, but rather the state is provided to the node.
For the most part the state provided is the state computed during the last
evaluation of the node, but this can also be managed to deal with testing
with assumed prior states, etc.
The NodeState object is effectively a dictionary from the users point of view,
with the ability to access keys using ``.<key>`` type syntax as well as ``['key']`` 
syntax. The user can think of it as ``self``.

Output
------

Ofttimes it useful to be able to see the previous state of the output when computing
a new result, having access to the values of the outputs reduces anti-patterns where
values are cached in state that could be read directly from the output node.

The output can be directly typed using the <time-series-type>Out syntax to identify
the output or use ``TIME_SERIES_OUTPUT`` to indicate the variable is to receive a reference
to the outputs.

In either case the output will be provided, but in the former, the IDE will be able to
resolve the type signature and be able to provide auto-completion support. The latter mode
will only have the most basic signature support.

For example:

```python

@compute_node
def count(ts: TIME_SERIES_TYPE, output: TSOut[int]) -> TS[int]:
    return output.value + 1
```

Using the output it is also possible to set the output directly, for example:

```python

@compute_node
def count(ts: TIME_SERIES_TYPE, output: TSOut[int]) -> TS[int]:
    output.value += 1
```

In which case nothing is returned, but the output is updated. If the output is updated
multiple times during the evaluation of the node, only on final *tick* is generated, the value
of which is dependent on the output type, for example a TS type will only have the last
value set being available, but a TSS type will show the net state of operations performed
during the evaluation of the node.
