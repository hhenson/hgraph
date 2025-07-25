Typing
======

TimeSeriesValue (TS)
--------------------

The most basic type is the ``TS`` type. We have been using this in the previous examples, this represents a time-series
of "scalar" values. Where a scalar value is a non-time-varying data type. These include things such as int, float, tuple,
etc. Scalars should be immutable, although this constraint is not formally enforced, using a mutable data type can cause
undefined behaviour if the value is mutated during processing.

We have already covered a number of examples using the key time-series properties such as ``value``, ``active``, and
``valid``. There are two other important properties, namely:

* ``delta_value`` - This represents the change in value but is equivalent to ``value`` in the case of the ``TS`` type.

* ``last_modified_time`` - The time the value was last changed.

* ``modified`` - A boolean value representing if this input was modified in this engine cycle.

Modified can be useful when there are more than one input that can change and the code behaves differently depending
on which input was modified, for example:

.. testcode::

    from hgraph import compute_node, TS
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        if ts1.modified and ts2.modified:
            return ts1.value + ts2.value
        elif ts1.modified:
            return ts1.value
        else:
            return ts2.value  # ts2 must have been modified in this case

    assert eval_node(my_compute_node, [1, 2, None], [3, None, 4]) == [4, 2, 4]

As mentioned previously, HGraph is strongly typed. Whilst Python itself does not enforce any
form of typing, this is not true for HGraph functions. They require each input and output to
be typed. These types are validated when connecting an output to an input. There is no
automatic type conversions, thus an output of type ``TS[int]`` cannot be bound to a type of ``TS[float]``
without an explicit type cast.

Casting
-------

To facilitate type conversions a number of casting utility functions exists, these are:

* cast\_
* downcast\_
* downcast_ref

For now we will focus on ``cast_`` and ``downcast_``.

To convert from one type to another use ``cast_``, this can be used
on any type that supports a constructor that takes in the source value
and returns an instance of the to value.

For example:

.. testcode::

    from hgraph import graph, TS, cast_
    from hgraph.test import eval_node

    @graph
    def cast_int_to_float(ts: TS[int]) -> TS[float]:
        return cast_(float, ts)

    assert eval_node(cast_int_to_float, [1, 2, 3]) == [1.0, 2.0, 3.0]

This casts the integer input into a float.

.. note:: As mentioned earlier, most code users write is expected to be
          in the form of graph code. All nodes (compute/sink/source) are wired together
          in the graph decorated functions. It is not possible to call a node from within a node
          function. Since ``cast_`` is a compute node, it must be called within a ``graph`` decorated
          function.

The downcast allows a type to be re-cast to a child type, for example:

.. testcode::

    from hgraph import graph, TS, downcast_, MIN_ST
    from hgraph.test import eval_node
    from datetime import date, datetime

    @graph
    def cast_date_to_datetime(ts: TS[date]) -> TS[datetime]:
        return downcast_(datetime, ts)

    assert eval_node(cast_date_to_datetime, [MIN_ST]) == [MIN_ST]

This may seem a bit strange, since we supply a datetime instance to start with. But in Python
datetime is an instance of date, thus the type checking logic correctly accepts the datetime
instance as a date. But if we got rid of the ``downcast_`` operator the graph would complain.

Additionally if we supplied a date as an input, the downcast would raise an assertion error.

CompoundScalar
--------------

The downcast operator is generally more widely used when working with the ``CompoundScalar``
type. This type provides a more complex structure for a value (or scalar) type.

The compound scalar is a type safe data class. All compound scalar classes have ``CompoundScalar`` as
the base. It is recommended to use the ``dataclass`` to tag the class.

Here is an example::

    @dataclass
    class MyCompoundScalar(CompoundScalar):
        p1: str
        p2: int

It is possible to use all the defined types available in HGraph as types for the properties. It is possible
to provide default values as well, for example::

    p1: str = "Hello"

As well as setting the value to ``None``, which is useful to describe optional fields, i.e.::

    p1: str = None

Scalar types are considered as immutable and atomic, in this case the compound scalar represents the collection
of values that tick together.

The types can be sub-classed as well.

To use the type, these are type to time standard time-series type (``TS``)


TimeSeriesBundle (TSB)
----------------------

Sometimes it is useful to describe a related collection of time-series values, these related values do not necessarily
change in unison with each other, but do form a natural grouping. Alternatively they may represent values that, whilst
they do change together may be computed separately.

An example of this scenario is a mid and spread, they are related in that they are computed from the inside bid and offer
price, but depending on the market or use-case the mid price is more likely to change than the spread. Thus they are
likely to tick at different rates. Also, it is quite standard for a mid price to be computed independently from the
spread when pricing an instrument, but the values need to be grouped together as they are both required to know
the value of the instrument when considering side.

Using this example we can group time-series values together as follows::

    from hgraph import TSB, TimeSeriesSchema, graph
    from dataclasses import dataclass

    @dataclass
    class MidSpread(TimeSeriesSchema):
        mid: TS[float]
        spread: TS[float]


    @graph
    def my_price_logic(price: TSB[MidSpread], ...) ->  ...

We declare the schema or shape of the bundle in much the same way as for the ``CompoundScalar``, however, in this case
the types are all time-series types. With a ``TimeSeriesScheam``, all properties must be time-series types. Whereas
for the ``CompoundScalar`` all types much also be scalar types.

With both ``TS`` of ``CompoundScalar`` and ``TSB`` of ``TimeSeriesSchema``, it is possible to dereference the individual
properties of the schemas by using the standard dot notation, for example::

    @graph
    def my_price_logic(price: TSB[MidSpread], ...) ->  ...
        a = price.mid

.. note:: When dereferencing a property of a bundle, during wiring, there is no cost. Doing the same with a ``TS`` of
          ``CompoundScalar`` incurs a cost of a node to extract the property from the compound scalar and emit it as
          a time-series value.

To construct a TSB value we consider two options, one in ``graph`` mode and one in a ``compute_node``.

.. testcode::

    from hgraph import TS, TSB, TimeSeriesSchema, graph, CompoundScalar, combine
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from frozendict import frozendict as fd

    @dataclass
    class BidAsk(CompoundScalar):
        bid: float
        ask: float

    @dataclass
    class MidSpread(TimeSeriesSchema):
        mid: TS[float]
        spread: TS[float]

    @graph
    def to_mid_spread(price: TS[BidAsk]) -> TSB[MidSpread]:
        mid = (price.bid + price.ask) / 2.0
        spread = price.ask - price.bid
        return combine[TSB[MidSpread]](mid=mid, spread=spread)

    assert eval_node(to_mid_spread, [BidAsk(bid=100.0, ask=101.0)]) == [fd(mid=100.5, spread=1.0)]

This shows the use of the dot dereferencing of a compound scalar. Remember this does incur two nodes to extract the
bid and ask time-series values. This also shows the use of many standard operators such as divide and subtraction.
HGraph supports most of the Python operators at wiring time allowing for writing code is a very similar fashion to
standard Python. But this is really just building up a dependency graph of nodes with the operators being replaced
with computation nodes. These nodes will be evaluated when the inputs tick.

The use of the ``combine`` operator is depicted here. The operator is a generic operator that will be resolved into
the correct node (or logical) instance. In this case let the ``combine`` operator that we which to combine time-series
values together into a ``TSB`` with the schema ``MidSpread``. If no refining parameters are provide (the
``[TSB[MidSpread]]`` the combine always assumes it is producing a ``TSB`` instance and will create an un-named type.
Un named TSB instances are defined dynamically and will match a named type based on the properties matching, that is::

    combine[TSB[MidSpread]](mid=mid, spread=spread)

is equivalent to::

    combine(mid=mid, spread=spread)

It is also possible to combine time-series values into a compound scalar, for example::

    ask = ...
    bid = ...
    combine[TS[BidAsk]](bid=bid, ask=ask)

In this case it is required that the output type is provided to produce the correct output type, otherwise we would
instead create an un-named bundle of the values.

Lets consider the other approach, using a ``compute_node``:

.. testcode::

    from hgraph import TS, TSB, TimeSeriesSchema, graph, CompoundScalar, compute_node
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from frozendict import frozendict as fd

    @dataclass
    class BidAsk(CompoundScalar):
        bid: float
        ask: float

    @dataclass
    class MidSpread(TimeSeriesSchema):
        mid: TS[float]
        spread: TS[float]

    @compute_node
    def to_mid_spread(price: TS[BidAsk]) -> TSB[MidSpread]:
        price = price.value  # get the actual value
        mid = (price.bid + price.ask) / 2.0
        spread = price.ask - price.bid
        return dict(mid=mid, spread=spread)

    assert eval_node(to_mid_spread, [BidAsk(bid=100.0, ask=101.0)]) == [fd(mid=100.5, spread=1.0)]

This code looks very similar to the previous example, the only real difference is the requirement to extract the
value from price before performing the computations and here we return the bundle as a dictionary of modified values.

In this case the code will produce fewer nodes as the nodes to extract ``bid`` and ``ask`` are not required,
not will there be nodes for the mathematical operations. This code is likely to run faster then the previous example
whilst the runtime-engine remains in Python. However, once the engine is migrated to C++, experience indicates that
the prior code will often outperform the second version as it is all evaluated in C++ and not in Python.

That said, with all performance statements, validation of your particular use-case is always important.

Finally, lets view how to access the properties of a ``TSB`` inside of a compute node.

.. testcode::

    from hgraph import TS, TSB, TimeSeriesSchema, graph, CompoundScalar, compute_node
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from frozendict import frozendict as fd

    @dataclass
    class BidAsk(CompoundScalar):
        bid: float
        ask: float

    @dataclass
    class MidSpread(TimeSeriesSchema):
        mid: TS[float]
        spread: TS[float]

    @compute_node
    def to_bid_ask(price: TSB[MidSpread]) -> TS[BidAsk]:
        mid = price.mid.value
        half_spread = price.spread.value / 2.0
        return BidAsk(bid=mid-half_spread, ask=mid+half_spread)

    assert eval_node(to_bid_ask, [fd(mid=100.5, spread=1.0)]) == [BidAsk(bid=100.0, ask=101.0)]

Here we see that each time-series property is represented as a time-series within the compute node. Thus we need
to get the value of the property. Each property also responds to all other time-series methods such as ``modified``, etc.

It is also possible to request the value of the time-series bundle directly, this will return a dictionary of keys and values.
This is also the first time that the ``delta_value`` returns something different, this will return the dictionary of values
that was modified in this engine cycle.

Exercise
........

Try creating a compute node (or sink node) that prints the ``value`` and ``delta_value`` with different input
combinations being ticked.

TimeSeriesList (TSL)
--------------------

The ``TSL`` is the time-series equivalent of a list, at this point in time, the list have a fixed size. This list is
of homogenous time-series values. This is different to the ``TSB`` which is a collection of heterogeneous time-series
values. When specifying the ``TSL`` two generics need to be provided, the first is the time-series type making up the
elements of the list and the second is the size of the list, for example:

.. testcode::

    from hgraph import compute_node, TSL, TS, Size
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        return tsl[0].value + tsl[1].value

    assert eval_node(my_compute_node, [(1, 2), (3, 4)]) == [3, 7]

.. note:: The use of the ``Size`` class to specifying the size of the list. This is done as Python does not support
          values as generics and only types. This provides a mechanism to specify the type including it's size using
          the generic tooling provided by Python.

When accessing a collection type, as with the ``TSB``, referencing an element of the type within a node the return value
is the time-series value, in this case it is ``TS[int]`` that gets returned.

If value is called on the collection type, the returned value is the collection of recursive calls to value on the
elements of the collection, for example:

.. testcode::

    from hgraph import compute_node, TSL, TS, Size
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(tsl: TSL[TS[int], Size[2]]) -> TS[tuple[int, ...]]:
        return tsl.value

    assert eval_node(my_compute_node, [(1, 2), (3, 4)]) == [(1, 2), (3, 4)]

Collection types can be dereferenced in graph code as well, for example:

.. testcode::

    from hgraph import graph, TSL, TS, Size
    from hgraph.test import eval_node

    @graph
    def my_compute_node(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        return tsl[0] + tsl[1]

    assert eval_node(my_compute_node, [(1, 2), (3, 4)]) == [3, 7]

This code is the same as the node implementation. Since we are at graph level, the ``+`` operator results in the
following equivalent code::

     @graph
    def my_compute_node(tsl: TSL[TS[int], Size[2]]) -> TS[int]:
        return add_(tsl[0], tsl[1])

Where the ``add_`` node takes two TS inputs.

TimeSeriesSet (TSS)
-------------------

Another often used data type is the ``set``, the time-series equivalent is the time-series set or ``TSS``.
This is a collection time-series type as well, but behaves more closely to the TS type as it can only contain
scalar values.

The type supports tracking the contents of a set over time and can provide the changes made in the form of the
``SetDelta`` protocol class. The delta contains the items added and removed. The type itself contains the current
state (accessible via the ``value`` property). The ``SetDelta`` is obtained from the ``delta_value`` property on
the time-series instance.

Here is an example of the ``TSS`` used in a compute node.

.. testcode::

    from hgraph import compute_node, TSS, set_delta
    from hgraph.test import eval_node

    @compute_node
    def my_compute_node(tss_1: TSS[int], tss_2: TSS[int]) -> TSS[int]:
        added = (tss_1.added() - tss_2.value) | (tss_2.added() - tss_1.value)
        removed = tss_1.removed() - tss_2.value
        removed |= tss_2.removed() - tss_1.value
        return set_delta(added=added, removed=removed)

    assert eval_node(my_compute_node, [frozenset({1, 2}),], [frozenset({3, 4})]) == [frozenset({1, 2, 3, 4})]

TimeSeriesDict (TSD)
--------------------

This represents a dictionary of time-series values, the ``TSD`` is comprised of a ``key_set`` that is a ``TSS`` instance.
The values of the dictionary are themselves time-series values in the same manor as for the ``TSB`` and ``TSL``
collection types. This is currently the only dynamic type, in that it can grow and shrink the number of collected
time-series values.

Another way to think of the ``TSD`` is to view it as a multiplex of time-series values.

The ``TSD`` takes generics as for dict, i.e. ``TSD[K, V]`` where the ``K`` must be a keyable scalar value (must support
the hashable protocol). and ``V`` is a time-series type.

The following key behaviours are provided by the ``TSD`` that are accessible in the node, namely:

``key_set``
    As already discussed this is a time-series set with type ``K``. The set contains the keys of the dictionary.

``keys()``, ``values()``, ``items()``
    As for any dictionary, these represent an iterator over the keys, values, and items. Where values are the time-series
    type instances.

``modified_keys()``, ``modified_values()``, ``modified_items()``
    As above, but will only provide values that have been modified in this engine cycle.

``valid_keys()``, ``valid_values()``, ``valid_items()``
    As above, but will only provide values that are valid in this engine cycle.

``added_keys()``, ``added_values()``, ``added_items()``
    As above, but will only provide values that have been added in this engine cycle.

``removed_keys()``, ``removed_values()``, ``removed_items()``
    As above, but will only provide values that have been removed in this engine cycle.

Standard methods such as ``__len__``, ``__iter__``, and ``__contains__`` are implemented as expected for a dict.

Here is an example to create a ``TSD`` as an output:

.. testcode::

    from hgraph import compute_node, TSD, REMOVE_IF_EXISTS, REMOVE, TS
    from hgraph.test import eval_node
    from frozendict import frozendict as fd

    @compute_node(valid=("key", "value"))
    def my_compute_node(key: TS[int], value: TS[str], remove: TS[int]) -> TSD[int, TS[str]]:
        out = {}
        if key.modified or value.modified:
            out[key.value] = value.value
        if remove.modified:
            out[remove.value] = REMOVE_IF_EXISTS
        return out

    assert eval_node(my_compute_node,
                [1, None, 2],
                ["a", "b", "c", "d"],
                [None, None, None, 1]
            ) == [fd({1: "a"}), fd({1: "b"}), fd({2: "c"}), fd({1: REMOVE, 2: "d"})]

In this example we create a time-series dictionary from the time-series supplying keys and values and removing
keys when the remove time-series ticks.

Note the use of ``valid`` to advice the engine that we only require the ``key`` and ``value`` attribute to be
valid, thus if the ``remove`` has not ticked the code will still be evaluated. See what happens if you remove the
``valid`` constraints.

We also use ``REMOVE_IF_EXISTS``, this is a soft instruction to the ``TSD`` to remove a key, if the key does not
exist then it nothing happens. If we had used ``REMOVE``, this will raise an exception if the key does not exist.
In this example this would work, try change this and then supply a key that does not exist to see how that behaves.

The delta-value of the ``TSD`` will contain ``REMOVE`` if a key is removed.

Next an example of using a ``TSD`` as in input is considered:

.. testcode::

    from hgraph import compute_node, TSD, REMOVE, TS
    from hgraph.test import eval_node
    from frozendict import frozendict as fd

    @compute_node
    def my_compute_node(tsd: TSD[int, TS[str]], key: TS[int]) -> TS[str]:
        if key.value in tsd:
            v = tsd[key.value]
            if v.modified or key.modified:
                return v.delta_value


    assert eval_node(my_compute_node,
                [fd({1: "a"}), None, fd({1: "b"}), fd({2: "c"}), fd({1: REMOVE, 2: "d"})],
                [None, 1, None, 2]
            ) == [None, "a", "b", "c", "d"]

This is a very low performing approach to extracting a value from a ``TSD`` based on the key.
This shows the basic dictionary nature of the input.

Note that this has a graph solution that is more performant, here is the example of this:

.. testcode::

    from hgraph import graph, TSD, REMOVE, TS
    from hgraph.test import eval_node
    from frozendict import frozendict as fd

    @graph
    def my_compute_node(tsd: TSD[int, TS[str]], key: TS[int]) -> TS[str]:
        return tsd[key]

    assert eval_node(my_compute_node,
                [fd({1: "a"}), None, fd({1: "b"}), fd({2: "c"}), fd({1: REMOVE, 2: "d"})],
                [None, 1, None, 2]
            ) == [None, "a", "b", "c", "d"]

The ``TSD`` has a number of useful features that can be accessed in graph mode, these include:

``[SCALAR|TS|TSS]``
    By using the ``[]`` operator on the time-series dictionary with a scalar value (say ``tsd[1]``) or a time-series
    of scalar values, this returns a time-series of values for the matching key, if a time-series set is used, then
    the set is used to filter the keys in the dictionary.

``key_set``
    Returns a reference to the key-set of the time-series dictionary.

