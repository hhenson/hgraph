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

Not the requirement of a zero value.

Reduce currently can accept both ``TSD`` and ``TSL`` inputs, but does not support
mixed inputs (i.e. where the LHS and RHS of the reduction function are different types)
for anything other than the ``TSL`` input type with the is_associated argument set to True.


