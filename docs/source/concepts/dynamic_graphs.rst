Dynamic Graphs
==============

Whilst some logic can be implemented with simple graphs that can be fully determined at wiring time, many rely on
dynamic shapes. Dynamic graphs, are those that can change over time, for example, processing user orders. Each
new order requires a new order handler graph to be constructed, but the set of orders will vary over time, thus we
make use of dynamic graph operators to support this behaviour.

There are currently three key dynamic graph tools in HGraph, these are:

* map\_
* reduce
* mesh\_

Map
---

As with the standard Python map, this takes a function or in the case a graph, node, or lamba (which will be treated
as a graph) and the multiplexed inputs. The map function then returns the multiplexed result.

For example:

::

    a: TSD[str, TS[float]] = ...
    b: TSD[str, TS[float]] = ...
    c: TSD[str, TS[float]] = map_(lambda a, b: a + b, a, b)

In the example above we use the lambda syntax to create a graph that sums two inputs, ``a`` and ``b`. We supply two
time-series dictionaries and assign the result to ``c``.

The ``map_`` operator will create a new instance of the graph for each unique key in the maps of ``a`` and ``b``.
When the key goes away, so will the graph. When a new key is constructed so is a new graph.

This allows us to write graphs to deal with the problem at a singular level and apply the solution to a collection of
the same.

The ``map_`` operator can be used with both ``TSD`` and ``TSL`` data-types (not at the same time). When applied to
``TSL`` types, the graphs are initialised at wiring time and not dynamically as currently ``TSL`` data-types fixed
length.

There are a number of useful options to deal with difference scenarios you may encounter, these include:

Knowing the key
...............

It can be useful to know what the key is that created the graph instance, for example, when processing orders it
the collection may be keyed with the order id. Then it could be useful to know the id. To do this there are two
strategies, they are:

1. Add an attribute named ``key`` as the first argument in the argument list with the type of ``TS[<key_type>]``.

    For example:

    ::

        @graph
        def order_handler(key: TS[str], order: TS[Order]) -> ...

        orders: TSD[str, TS[Order] = ...
        results = map_(order_handler, orders)

    In this example, the ``order_handler`` function takes in the key, which will be the order id that keys the ``order``
    collection.

2. Use the ``__key_arg__`` kwarg property of ``map_``.

    If the code we want to use in the map has a different name for the key, it is possible to override the name of the
    key using the ``__key_arg__`` kwarg. For example:

    ::

        @graph
        def order_handler(order_id: TS[str], order: TS[Order]) -> ...

        orders: TSD[str, TS[Order] = ...
        results = map_(order_handler, orders, __key_arg__='order_id')

Manging the key set
...................

When there are multiple inputs provided to the ``map_``, it is possible that some of the inputs should not contribute
the set of keys that construct new graph instances. There are a couple of solutions to this, these are:

1. Use of the ``no_key`` marker.

    This is useful when there is an input that needs to be de-multiplexed, but for which the set of keys is larger
    (or different) than the set of keys we wish to create new graph instances with. An example of this is provided
    below:

    ::

        @graph
        def price(instrument_id: TS[str], request: TS[PriceRequest], market_data: TS[Price]) -> TS[Price]:
            ...

        requests = TSD[str, TS[PriceRequest]] = ...
        market_data = TSD[str, TS[Price]] = ...

        result = map_(price, requests, no_key(market_data), __key_arg__ = 'instrument_id')

    In this example, the requests are supplied by users requesting a price for a particular instrument, and the
    ``market_data`` variable represents the collection of all available market data. Here we only want to instantiate
    a pricing graph for each client request. So we mark it as ``no_key``. This excludes the keys from this input.

2. Use ``__keys__`` kwarg.

    This kwarg allows the set of keys used to construct new graph instances to be set by the code. Setting this
    will only create new graphs for the key set provided. For example:

    ::

        @graph
        def my_logic(a: TS[int], b: TS[int], c: TS[str]) -> TS[str]:
            ...

        a: TSD[str, TS[int]]
        b: TSD[str, TS[int]]
        c: TSD[str, TS[str]]

        result = map_(my_logic, a, b, c, __keys__ = b.key_set)

    In this example, we are only using the keys from the ``b`` input using the ``key_set`` of this input.
    Note that it is possible to construct any valid set of keys, this does not have to come from any of the inputs,
    but remember that only keys that match an entry in the key set will be de-multiplexed and made use of.

    Using the ``__key_set__`` to set the de-multiplex keys is also helpful when multiple input are provided with
    different key types (for example, ``TSD[int, ...]`` and ``TSD[str, ...]``) then it is difficult for the operator
    to know which is the de-multiplexing key set and which is not (for example there is insufficient information in
    the mapped signature to work this out).

You can't touch this
....................

Finally, there are times, when an input fits with the correct key type, but the input is not intended to be
de-multiplexed. When this can be determined by inspecting the mapped functions signature, this is not a problem, but
that is not always the case.

To ensure we don't de-multiplex the input, we use the ``pass_through`` marker to advice the ``map_`` operator not to
de-multiplex the input, for example:

::

    @graph
    def scale(a: TIME_SERIES_TYPE, b: TS[float]) -> TS[float]:
        ...

    a: TSD[str, TS[float]] = ...
    b: TSD[str, TS[float]] = ...

    result = map_(scale, pass_through(a), b)

In this case, there is no way for the ``map_`` operator to guess what to do with a, and since we wish it to be
supplied to the scale function as is, we mark it as ``pass_through``. This is then supplied to each instance of the
newly constructed graph's as is and only ``b`` is de-multiplexed.

