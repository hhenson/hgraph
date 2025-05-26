Typing
======


As mentioned previously, HGraph is strongly typed. Whilst Python itself does not enforce any
form of typing, this is not true for HGraph functions. They require each input and output to
be typed. These types are validated when connecting an output to an input. There is no
automatic type conversions, thus an output of type TS[int] cannot be bound to a type of TS[float]
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


TimeSeriesBundle
----------------

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

