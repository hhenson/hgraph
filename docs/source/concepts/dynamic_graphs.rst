Dynamic Graphs
==============

Whilst some logic can be implemented with simple graphs that can be fully determined at wiring time, many rely on
dynamic shapes. Dynamic graphs, are those that can change over time, for example, processing user orders. Each
new order requires a new order handler graph to be constructed, but the set of orders will vary over time, thus we
make use of dynamic graph operators to support this behaviour.

There are currently three key dynamic graph tools in HGraph, these are:

* map\_
* reduce
* switch\_
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

In the example above we use the lambda syntax to create a graph that sums two inputs, ``a`` and ``b``. We supply two
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

Reduce
------

Another common issues is to take a collection and repeatedly apply binary function to the items (and the results) to
convert the collection to a single value. This is similar to the ``reduce`` function in functools package.

The ``reduce`` operator can be applied to ``TSL`` and ``TSD`` collection types. When reducing ``TSL`` inputs, the
reduce operator will statically build the reduction graph, but with the ``TSD`` result it must produce a dynamic
graph that changes when items are added or removed.

The current implementation will create a balanced binary tree to reduce the result, this means that the initial
pass through the results will cost O(n.log(n)) but subsequent updates will take O(log(n)) to process (assuming that
the number of changes are small).

Reduction requires the provision of a ``zero`` value in order to correctly operate. The easiest way to supply a zero
is to implement the ``zero`` operator for the payload data-type (for example, reducing ``TSD[..., TS[int]]`` using
``add_``, an operator for ``zero(TS[int]], add_)`` would be required to return a valid zero value.

Below is a simple example:

::

    values: TSD[str, TS[float]]
    result: TS[float] = reduce(add_, values)

This relies on the existence of the appropriate zero value.

Alternatively try:

::

    values: TSD[str, TS[MyDataType]]
    result: TS[MyDataType] = reduce(add_, values, MyDataType())

Where ``MyDataType()`` represents a zero value.

Switch
------

It is often a requirement to have different behavior based with the same input signature. To do this,
we have the ``switch`` operator. This works a bit like a case statement, by providing a dictionary
of keys and graphs (or nodes) which represents the options that could be evaluated and then provide
the switch operator a key time-series that will select the graph to instantiate.

For example:

::

    from hgraph import TS, graph, switch_, add_, sub_

    @graph
    def graph_switch(selector: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return switch_(selector, {
            "add": add_,
            "sub": sub_,
        }, lhs, rhs)

In this example we have two potential options, when the ``selector`` is set to 'add' then the ``add_``
node is instantiated, the ``lhs`` and ``rhs`` are wired in, these are provided by reference, so if they have
values, they will tick on construction. Each time the selector ticks the previous graph will be stopped (if
there was one) and the new graph will be instantiated and started.

Sometimes we only want a new graph instantiated if the value of the selector changes, but other times we
may wish to cause the graph to be re-loaded if the selector changes, to do this use the ``reload_on_ticked``
kwarg and set it to ``True`` (the default is ``False``).

An example of how this can be useful is for things like order management where a collection of order requests are
collected, then using a combination of ``map_`` and ``switch_`` the orders can be split up and then using the switch
the correct graph for the type of order can be instantiated and the order can be correctly handled.

Mesh
----

This is the most complex of the dynamic graph building tools. This allows for the dynamic construction of computational
nodes.

This bears some similarities to the ``mesh_`` operator. This takes as inputs the function (graph, node or lambda), then
it is possible to provide multiplexed inputs (as with ``map_``) or to set the ``__key_set__`` to instantiate the graph
instances.

Up to this point there is no difference between ``map_`` and mesh, where the difference comes in is that the is possible
to dynamically construct new requests without having them fed into the key set of requests. This is typically done
from logic within the graph that is instantiated in the main ``mesh_`` call.

Below is a simple example:

::

    from hgraph import graph, TS, switch_, const, mesh_

    @graph
    def f(k: TS[str]) -> TS[float]:
        return switch_(
            k,
            {
                'a': lambda : const(1.0),
                'b': lambda : const(2.0),
                'a+b': lambda: mesh_('f')['a'] + mesh_('f')['b']
            },
        )

    @graph
    def compute_a_plus_b() -> TS[float]:
        return mesh_(f,
                     __key_arg__ = 'k',
                     __key_set__ = const(frozenset({'a+b'}), TSS[str]),
                     __name__='f')


In the main graph ``compute_a_plus_b``, the top level ``mesh_`` operator is called. We use the ``__key_set__`` here to
keep this very simple. Notice we also name this mesh instance using the ``__name__`` kwarg. Now, when the ``f`` graph is
instantiated, it can recursively call the mesh dynamically, in this case using the dynamic call signature ``mesh_('f')``
where 'f' is the name we gave the mesh instance and ``['a']`` and ``['b']`` are the new keys we wish to have
instantiated.

.. note:: It is not possible to add new entries to the mesh other than in the initial call, so if you use multiplexed
          arguments it will likely limited the utility of mesh, so as a general rule either use ``__key_set__`` for
          constructing graph instance and ``no_key`` for the multiplexed inputs to ensure that there are the keys
          required, but instances are only created on demand.


