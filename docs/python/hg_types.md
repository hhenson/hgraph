HGraph Type System
==================

The Hg type system defines a set of scalar and time-series values.

As of now the following scalar types are supported:

* bool
* int
* float
* str
* date
* datetime
* time
* timedelta
* tuple
* frozenset
* frozendict
* CompoundScalar
* type

The following time-series types are supported:

* TS
* SIGNAL
* TSL
* TSB
* TSS
* TSD
* REF

For the basic atomic types (bool, int, etc.) these are as presented, for the collection types, they are also required
to be typed, with tuple supporting both heterogeneous and homogenous properties. For example,
```tuple[str, int, date]``` for heterogeneous instances, and ``tuple[int, ...]`` for homogenous instances.

The [``CompoundScalar``](schema_based_types.md#compoundscalar) is akin to a POPO (or ``dataclass``). It supports constructing more complex data structures, 
where the values are intended to change / be propagated as one atomic change.

The time-series types are what are likely to be new.

Time-series types
-----------------

Time series types are separated into input and output types. An input type is what is used to receive a time-series
value in a function, and output types are what are returned from a function.

The output type is the one that holds the actual value, the input type is a wrapper around the output.

Thus:

```python
@compute_node
def my_func(ts: TS[int]) -> TS[str]:
    ...
```

describes a function which takes a ``TS[int]`` input, and 'returns' a ``TS[str]`` output. The output has the ability to
set the time-series value, but the input can only read the value.

### Inputs

The input is bound (or one of it's children) to an output. Once bound the input delegate methods such as ``value``,
``modified``, etc. to the bound output. The input can be bound, un-bound and re-bound during the graphs life-time.
If an input is not bound, then it will respond with ``valid`` as False. Once bound it will respond with the state of
the bound output.

The input itself does not store state (with a few technical exceptions).

Another key difference is that an input supports the concept of being *active*, through the methods:
* ``active`` - True if this input is active (receiving notifications of change).
* ``make_active`` - Makes the input active (subscribes the node for modification notifications)
*  ``make_passive`` -  Makes the input in-active (un-subscribes the node for modification notifications of the bound output)

This ability to mark the input as being active or not controls how the node is scheduled in the graph, if all the
inputs are marked as passive, then the node will only ever be scheduled if it makes use of a scheduler.

It can be useful to mark a select number of the inputs as active and the remainder as passive, for example, when
accepting a trade, the node would have the trade_request input marked as active, but keep the market_data input passive.
(The market data output is likely to tick far more frequently then the trade_request output) This insures the
node is scheduled when there is work to do, and still has access to the most up-to-date market data, but does not
get scheduled each time the market data changes.

The input can also be *peered* or *non-peered* (``has_peer``). A peered input, has a one-to-one correspondence with
a bound output, whereas a non-peered input will have no direct output bound to it, but will instead have it's children
bound to various different outputs.

Since the connection between an output and an input describe an edge in the graph, this allows for inputs that 
represent the termination of a single edge, and (non-peered) inputs that represent the termination of multiple different
edges into the node. By definition, non-peered inputs can only represent collection types (``TSL``, ``TSB`` and ``TSD``).

### Outputs

Outputs have ``subscribe_node`` and ``un_subscribe_node`` methods. These allow inputs to indicate their interest in
being scheduled by the output when it is modified.

The user will never call these directly, instead they are called as part of the ``mark_active`` and ``mark_passive``
calls.

Outputs also hold the state of the time-series value. There is only ever one source of truth present. It also
tracks when the output was last updated, which is useful to determine if the value was modified in this engine-cycle
or previously. The property is ``modified`` to determine the current state, the input will delegate to this property
when called on the input.

When the value property is set, the output will mark the value as being modified, and schedule all subscribed nodes.

In the Python API, the value can be set using a standard assignment operation. The assigned value is interpreted
based on the time-series type. For example a ``TS[int]`` will expect an integer value and will set the internal
value to be the new value supplied, but a ``TSL[TS[int], Size[2]]`` would expect either a tuple or list of size 2, 
or a dictionary with integer keys (in the range of 0-1) with values of type int. When setting with a dictionary,
only the indices supplied are modified. Those that are not supplied as assumed to have no change.

### TS

This is the most simplistic time-series value. Think of this as a column in a data-frame with an associated datetime
index where you see the value as-of the current evaluation time. By default, there is no buffering available to the
user of the type. So you only ever see the current value (as-of).

There is no difference between the value and the delta value.

### SIGNAL

This is a special adaptor time-series type, it can bind to any output type, and will activate whenever the output
is modified. It will have as value True when modified and False otherwise.

This is useful when the input changing is the only thing of interest. This is ONLY available as an input type.

### TSL

The TSL is a time-series list. As of now the list is a constrained size list. When defining a TSL the user is required
to specify the type of collection and the size of the collection. This is done by supplying this information as
a type tuple, i.e. ``TSL[TS[int], Size[2]`` where Size is a special type that allows us to specify the size of the
collection as a generic input. The first argument is the time-series type the list contains.

A list can be peered or non-peered with a useful constructor method ``from_ts`` that can construct a non-peered
``TSL`` from a list of similar types outputs.

The instance can be de-referenced at wiring or run-time. For example:

```python
tsl: TSL[TS[int], SIZE].from_ts(ts_1, ..., ts_n)
...

my_func(tsl[2])  # at wiring time extract the 3rd element

@compute_node
def my_other_func(tsl: TSL[TS[int], SIZE], ndx: TS[int]) -> TS[int]:
    return tsl[ndx.value].value
```

### TSB

The time-series bundle, represents a heterogeneous collection of time-series values. The structure of the bundle
is described by supplying a [``TimeSeriesSchema``](schema_based_types.md#timeseriesschema) instance to the type
description. For example: ``TSB[MySchema]``.

The ``TSB`` is a collection class and can be peered or non-peered. This is a useful tool for passing related time-series
values around the graph.

The ``TSB`` has a useful method ``as_schema`` which makes the type appear to be the schema type which helps with
PyCharm type inference. 

```python
from hgraph import TimeSeriesSchema, TSB, TS, sink_node, graph
from hgraph.nodes import debug_print

class MySchema(TimeSeriesSchema):
    p1: TS[int]
    p2: TS[str]
    
    
@graph
def my_graph(tsb: TSB[MySchema]):
    ...
    debug_print("Test", tsb.p2)
    
@sink_node
def print_tsb(tsb: TSB[MySchema]):
    print(f"p1: {tsb.as_schema.p1.value}, p2: {tsb.p2.value}")

```

### TSS

The time-series set, this represents a set over time. The ``TSS`` is a set over a scalar type expressed as, for example:
``TSS[str]``. The set uses the ``SetDelta`` to describe the change in the set. This contains the set of added and removed 
elements.

When ``delta_value`` is called the ``SetDelta`` is returned. This can also be used to set the value. If the
value is assigned with a set, the ``TSS`` will add the values and remove any elements wrapped with the ``Removed`` 
marked, for example:

```python
from hgraph import Removed, compute_node, TSS

@compute_node
def f(...) -> TSS[int]:
    return {1, 2, Removed(3)}
```

This also has methods:
* ``added_elements`` - The elements added in this engine cycle.
* ``removed_elements`` - The elements removed in this engine cycle.


The set can also implement operations such as ``__contains__``.

### TSD

This is by far the most complex time-series type. It supports a dynamically sized collection of time-series
values, keyed by a scalar type. Example usage: ``TSD[str, TS[int]]``.

The ``TSD`` uses dictionary values to represent both ``value`` and ``delta_value``. The setter will accept a dictionary
input and will add keys that do not exist, remove keys when the value is ``REMOVE`` (failing if the key does not exist)
or ``REMOVE_IF_EXISTS`` (which does not fail if the key does not exist). If the key does exist the value is updated.

The ``TSD`` also contains an associated ``key_set`` which is a ``TSS`` of the keys describing the ``TSD``.
The ``TSD`` also supports ``added_[keys|values|items]``, ``removed_[keys|values|items]`` and
``modified_[keys|values|items]``. 

Almost all the functionality is only available during run-time within a node.
The ``key_set`` is accessible at wiring time.


### REF

The reference type, allows for passing a reference to an output around the graph. This is useful when the code is 
focused on manipulating streams of time-series values, but does not actually have an interest in the actual value,
for example:

```python
from hgraph import compute_node, REF, TS

@compute_node
def if_(condition: TS[bool], true_: REF[TS[str]], false_: REF[TS[str]]) -> REF[TS[str]]:
    if condition.value:
        return true_.value
    else:
        return false_.value
```

This code will tick when the condition changes, or if the bound outputs associated to the references change.
It is seldom that the bound outputs change, but more likely the values of the outputs will change. This approach
ensures the correct output is bound to the receiver, and the ``if_`` node will only be called occasionally, 
when the condition changes (or the bound outputs change).

This type is largely used by library functions.
