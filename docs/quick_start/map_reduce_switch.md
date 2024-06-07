Map, Reduce, Switch
===================

map_
----

The ``map_`` function is a special function that allows us to apply a node across a collection
of values. This currently supports mapping over TSL and TSD collections.

The ``map_`` function can take both collections and normal time-series values, but
there needs to be at least one collection in the inputs, or at least a key-set.

Here is an example of using the ``map_`` function:

```python
from hgraph import compute_node, TS, graph, TSD, map_


@compute_node
def convert(ts: TS[int]) -> TS[str]:
    """Convert the input to a time series."""
    return str(ts.value)


@graph
def graph(tsd: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
    return map_(convert, tsd)
```

The ``map_`` function can also take ``TSL`` inputs. As depicted in the example below
(using the same ``convert`` function as above):

```python
@graph
def graph_tsl(tsl: TSL[TS[int], Size[2]]) -> TSL[TS[str], Size[2]]:
    return map_(convert, tsl)
```

There are some other useful features available in ``map_``, such as the ability to
mark inputs as pass-through and non-key. These markers provide hints to the 
``map_`` function to help it better guess how to map the inputs of the ``map_`` to 
the inputs of the supplied function.

For example:

```python
from hgraph import sink_node, TS, graph, TSD, map_, pass_through, TIME_SERIES_TYPE
from hgraph.test import eval_node

@sink_node
def print_input(key: TS[str], ts: TIME_SERIES_TYPE, mode: str):
    print(f"[{mode}] {key.value}: {ts.delta_value}")


@graph
def graph_undecided(tsd: TSD[str, TS[int]]):
    map_(print_input, tsd, "No Passthrough")
    map_(print_input, pass_through(tsd), "Passthrough", __keys__=tsd.key_set)


print(eval_node(graph_undecided, tsd=[{"a": 1, "b": 6}, {"a": 2, "b": 7}]))
```

This shows the use of the ``pass_through`` function to mark the input as a pass-through.
In this case it is possible to interpret the ts input as either a ``TS[int]`` or a ``TSD[str, TS[int]]``.
The ``map_`` logic will interpret the input as a ``TS[int]`` and that the input supplied is a multiplexed
input. But it is possible to override this behavior by wrapping the input in a ``pass_through`` function.

You can also see the use of the ``__keys__`` argument, this allows us to provide a 
specified key set that is used to create instances of the mapped function.

Non-key inputs are wrapped in a similar ways as pass-through inputs, but using the
``non_key`` function. This is useful when you want to map over a collection of
inputs where one of the inputs may be larger than the set of keys you wish the 
``map_`` to operate over.

You can also supply a lambda instead of a graph or node to the ``map_`` function. The lambda will be
treated as a graph and its signature deduced from the inputs given to map_ and the names of the lambda arguments.

```python
from hgraph import graph, map_, TSD, TS

@graph
def g(tsd: TSD[str, TS[int]]):
    return map_(lambda i: i + 1, i=tsd)

```

reduce
------

Reduce support applying a function across a collection of values recursively to produce
a single value.

For example:

```python
from hgraph import TS, graph, TSD, reduce
from hgraph.nodes import add_


@graph
def graph_reduce_tsd(tsd: TSD[str, TS[int]]) -> TS[int]:
    return reduce(add_, tsd, 0)

```
This will sum the values of the inputs and produce a time-series of values
representing the sum of the inputs.

Note the requirement of a zero value.

Reduce currently can accept both ``TSD`` and ``TSL`` inputs, but does not support
mixed inputs (i.e. where the LHS and RHS of the reduction function are different types)
for anything other than the ``TSL`` input type with the is_associated argument set to True.

Reduce will also accept lambdas as the reduction function, the lambda has to have two arguments to be compatible.


switch_
-------

The ``switch_`` function provides the mechanism to switch between different functions
that share the same function signature, but provide different implementations.

For example:

```python
from hgraph import TS, graph, switch_
from hgraph.nodes import add_, sub_


@graph
def graph_switch(selector: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return switch_({
        "add": add_,
        "sub": sub_,
    }, selector, lhs, rhs)
```

In this case we initially wire in the ``add_`` functions, based on the value of 'selector'.
Then until the selector changes, the inputs are few to add that is the output.
Then, when we tick 'sub', the inputs are switched to the ``sub_`` function and the results
represent the process through the ``sub_`` function.

The combination of ``map_``, ``reduce`` and ``switch_`` provide the basis for dynamic
graph construction. With ``map_`` de-multiplexing the inputs and ``switch_`` providing
the ability to switch between different functions based on a selection criteria. 
Finally the ``reduce`` is able to convert a multiplexed result into an aggregate value.

An example of this would be when dealing with a stream of various orders, then each
order is de-multiplexed, then using the order-type the order is associated to 
the appropriate order handling function. The result is re-multiplexed.

The user may be interested in some aggregates from the order stream, such as the
net open value of the orders. This can be achieved by using the ``reduce`` function
to operate over the orders' dollar value, for example, using the ``add_`` function.

Switch also supports lambdas. The lambda has to have the same signature as the other functions in the switch.

```python
from hgraph import TS, graph, switch_

@graph
def graph_switch_lambda(selector: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return switch_({
        "add": lambda lhs, rhs: lhs + rhs,
        "sub": lambda lhs, rhs: lhs - rhs,
    }, selector, lhs, rhs)
```
