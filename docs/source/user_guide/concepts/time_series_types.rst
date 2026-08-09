Time Series Types
=================

.. testsetup::

    # Quieten the engine's debug logging so the examples below show only their own output.
    from logging import INFO
    from hgraph import GraphConfiguration
    GraphConfiguration(default_log_level=INFO)

The time-series types define the ports of the node that allow the graph to be connected.
A node in the graph is allowed at most one output port. This port can be connected to
zero or more input ports. The flow of information is from output to input.

.. mermaid::

    graph LR
        subgraph node
            direction LR
            ts_1(["ts_1"])
            ts_i(["ts_i"])
            ts_n(["ts_n"])
            out(["out"])
        end


Non-time-series inputs are supported as inputs, these define the configurable properties
of the node and do not change over time. We refer to these as scalar properties to
indicate that they do not have a time-dimension. A scalar is bound once, when the node is
wired, and holds that value for the life of the node; a time-series is a live connection
whose value changes as the graph is evaluated. Only time-series types form edges in the
graph, and only a time-series can be an output.

We distinguish input and output time-series types. Input time-series properties can be
connected to output's. The output time-series type holds the value of the time-series
property, the input time-series type references the value in the output type.

Another way of thinking about the time-series properties is to think about them in terms
of the observer pattern, here the output can be viewed as the observable and the input
the observer. The observable holds the source of information and the observer is
associated to the observable, in this case (as with property observers) it can see
the value and will be notified when the property changes.

The other way to think about this is using the pub-sub pattern, the output is the publisher
and the input is the subscriber. As with classical pub-sub, there is a single publisher
for a topic (or property) and zero or more subscribers to the property.

An output time-series value can be set, but an input time-series value can only be read.

Using the HGraph model, input time-series' are declared as function arguments and output
time-series values are declared as return values.

Thus, when calling an HGraph function, (if it has a return type defined) the value returned
(when using the graph decorator) is a reference to the output port of the graph (or the
time-series output).

Let's consider the following code:

::

    @graph
    def my_example_graph():
        c = const("world")
        debug_print("hello", c)

In this trivial example, ``c`` represent the output time-series of the const node.
``"world"`` is a scalar input defining the configuration defining the value that the
node will tick with. ``debug_print`` is connected to the ``const`` node by passing ``c``
to the time-series input of the node. Creating the graph:

.. mermaid::

    graph LR
        subgraph const
            out(["out"])
        end
        subgraph debug_print
            ts(["ts"])
        end
        out --> ts

Time-series properties
----------------------

The time-series types have the following properties:

.. mermaid::

    classDiagram
        class TimeSeries {
            +Node owning_node
            +Graph owning_graph
            +value
            +delta_value
            +bool modified
            +bool valid
            +bool all_valid
            +datetime last_modified_time
        }
        class TimeSeriesInput {
            +bool active
            +make_active()
            +make_passive()
        }
        class TimeSeriesOutput
        TimeSeries <|-- TimeSeriesInput
        TimeSeries <|-- TimeSeriesOutput

The properties above are common to both inputs and outputs. Inputs add the notion of
being ``active``, covered under node activation, since only an input can subscribe a
node to changes.

The time-series type is aware of the node it is bound to. This can be extracted
from the ``owning_node`` property. This is most useful when debugging the
graph, but generally is used more for framework code (as is the owning_graph).

The ``owning_graph`` property declares the graph the node belongs to, this is
the runtime graph, not the wiring graph.

All inputs are capable of presenting their current value state as a Python object.
This is accessed through the ``value`` property, the ``delta_value`` property
is also a Python representation of the time-series, in this case it represents
the change in value. This is only really useful on complex types, such as a
time-series collection class, where the delta represents the elements that
were modified in this engine cycle. Whereas the ``value`` property represents
the current valid values of the time-series, which include results that have
previously been modified / set.

For a ``TS``, the two are the same, since the whole value changes each time it
ticks. The distinction only becomes visible on a collection type. The example
below prints both for a two-field bundle:

.. testcode::

    from hgraph import compute_node, TSB, TS, TimeSeriesSchema
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from frozendict import frozendict as fd

    @dataclass
    class Quote(TimeSeriesSchema):
        bid: TS[float]
        ask: TS[float]

    @compute_node
    def probe(q: TSB[Quote]) -> TS[str]:
        return f"{dict(sorted(q.value.items()))} | {dict(sorted(q.delta_value.items()))}"

    assert eval_node(probe, [fd(bid=100.0, ask=101.0), fd(ask=102.0), fd(bid=99.0)]) == [
        "{'ask': 101.0, 'bid': 100.0} | {'ask': 101.0, 'bid': 100.0}",
        "{'ask': 102.0, 'bid': 100.0} | {'ask': 102.0}",
        "{'ask': 102.0, 'bid': 99.0} | {'bid': 99.0}",
    ]

Laid out as a table, with the input on the left and the two properties on the right:

+----------+-----------------------+--------------------------------+------------------+
| **tick** | **input**             | ``value``                      | ``delta_value``  |
+==========+=======================+================================+==================+
|     1    | ``bid=100, ask=101``  | ``{ask: 101.0, bid: 100.0}``   | ``{ask, bid}``   |
+----------+-----------------------+--------------------------------+------------------+
|     2    | ``ask=102``           | ``{ask: 102.0, bid: 100.0}``   | ``{ask}``        |
+----------+-----------------------+--------------------------------+------------------+
|     3    | ``bid=99``            | ``{ask: 102.0, bid: 99.0}``    | ``{bid}``        |
+----------+-----------------------+--------------------------------+------------------+

Note that in the second cycle only ``ask`` ticked, so the delta contains only ``ask``,
but ``value`` still reports ``bid`` at its last set value of ``100.0``. The time-series
does not forget a value simply because it did not change; that is the "infinite forward
fill" behaviour of the graph. Use ``value`` when you need the current state of the world,
use ``delta_value`` when you need to know what just happened.

There are two useful flags associated to the time-series, ``modified`` and
``valid``. Where ``modified`` is ``True`` if the time-series type was modified
in the current engine cycle and ``False`` otherwise. The ``valid`` flag is
``True`` when the value has been set at least once, or in other words, has
a valid value associated to it. Note, there are circumstances where a value
can transition from valid to invalid, so the naive statement of at least set
once is not 100% true.

These two answer different questions, and confusing them is a common source of
surprise. ``valid`` asks "does this input have a value I can read?"; ``modified``
asks "did this input change in this engine cycle?". An input can be valid without
being modified, which is exactly the case for a passive input that ticked earlier:

+----------+----------------+-----------------+----------------+-----------------+
| **tick** | ``a.value``    | ``a.modified``  | ``b.value``    | ``b.modified``  |
+==========+================+=================+================+=================+
|     1    |                | ``False``       |       3        | ``True``        |
+----------+----------------+-----------------+----------------+-----------------+
|     2    |       1        | ``True``        |       3        | ``False``       |
+----------+----------------+-----------------+----------------+-----------------+
|     3    |       1        | ``False``       |       4        | ``True``        |
+----------+----------------+-----------------+----------------+-----------------+

In tick 1 ``a`` is not yet valid at all (``a.valid`` is ``False``, and reading
``a.value`` is not meaningful). From tick 2 onwards ``a`` is valid for the rest of
the run, but it is only ``modified`` in the cycle it actually ticked.

The ``all_valid`` flag is ``True`` when all the elements of a collection
type are valid, for example in a TSL (time-series list), it is possible that
only some of the elements in the list could be valid and others not yet
valid. The ``all_valid`` property ensures that each element is valid. This
is a stronger requirement then ``valid`` which becomes true as soon as at
least one element becomes valid.

Where ``all_valid`` differs from ``valid``
..........................................

The distinction only exists for the types that are made up of independently
ticking elements, which is to say ``TSL`` and ``TSB``. Everywhere else
``all_valid`` is defined as ``valid``, and asking for it buys you nothing:

.. list-table::
    :header-rows: 1
    :widths: 20 80

    * - Type
      - ``all_valid``
    * - ``TS``
      - Same as ``valid``. A single value is either set or it is not.
    * - ``TSL``, ``TSB``
      - ``valid`` **and** every element valid. This is the case worth using.
    * - ``TSD``
      - Same as ``valid``. A key only exists once it has a value, so there is
        no partially populated state to detect.
    * - ``TSS``
      - Same as ``valid``. The set holds scalars, not time-series.
    * - ``TSW``
      - ``valid`` **and** the buffer has reached its ``min_size``. Useful, but
        it means something different to the collection case.

It follows that ``all_valid`` on a ``TS``, ``TSD`` or ``TSS`` input is not a
stricter guard, it is the same guard written the long way:

.. testcode::

    from hgraph import compute_node, TS, TSD, TSS
    from hgraph.test import eval_node
    from frozendict import frozendict as fd

    @compute_node(valid=tuple())
    def same(ts: TS[int], tsd: TSD[str, TS[int]], tss: TSS[int]) -> TS[bool]:
        return all(x.valid == x.all_valid for x in (ts, tsd, tss))

    assert eval_node(same, [1], [fd(k=1)], [frozenset({1})]) == [True]

.. warning:: The check is one level deep, it does **not** recurse. A collection
             asks each of its elements for ``valid``, not for ``all_valid``, so
             a partially populated collection nested inside another does not
             make the outer one ``all_valid``-false. This surprises people, as
             the name suggests otherwise.

The example below demonstrates this. Each inner list has had only its first
element set, so each is ``valid`` but not ``all_valid``; the outer list is
nevertheless ``all_valid`` because it only asks its elements for ``valid``:

.. testcode::

    from hgraph import compute_node, TSL, TS, Size
    from hgraph.test import eval_node

    @compute_node(valid=tuple())
    def nested(x: TSL[TSL[TS[int], Size[2]], Size[2]]) -> TS[str]:
        return f"outer={x.all_valid} inner0={x[0].all_valid} inner1={x[1].all_valid}"

    assert eval_node(nested, [{0: {0: 1}, 1: {0: 2}}]) == [
        "outer=True inner0=False inner1=False",
    ]

A direct element that is not valid at all does still make the outer collection
``all_valid``-false — that is the one level the check does look at:

.. testcode::

    assert eval_node(nested, [{0: {0: 1}}]) == [
        "outer=False inner0=False inner1=False",
    ]

If you need the nested guarantee, check it explicitly in the body.

The cost
........

``all_valid`` is not cached and it is not a one-off gate. When declared as a
node pre-condition it is re-evaluated on **every** evaluation of that node, for
the life of the graph, including long after the condition has been satisfied
and can no longer become false. Each evaluation walks the collection's elements,
so the cost is proportional to the size of the collection, paid per engine cycle
in which the node is scheduled.

For a ``TSL[..., Size[2]]`` that is irrelevant. For a large ``TSB``, on a node
that ticks frequently, it is not. Only ask for ``all_valid`` when the constraint
is actually required; where a node simply needs to wait for a collection to fill
up once, it is usually cheaper to check in the body and make the input passive,
or to gate the subgraph, than to pay the scan on every cycle.

To make the difference concrete, take a ``TSL[TS[int], Size[2]]`` where only the
first element has ever ticked. The list as a whole is ``valid``, because one of its
elements is, but it is not ``all_valid``, because the second element has never been
set:

.. testcode::

    from hgraph import compute_node, TSL, TS, Size
    from hgraph.test import eval_node

    @compute_node
    def probe(tsl: TSL[TS[int], Size[2]]) -> TS[str]:
        return f"valid={tsl.valid} all_valid={tsl.all_valid}"

    assert eval_node(probe, [{0: 1}, {1: 2}]) == [
        "valid=True all_valid=False",
        "valid=True all_valid=True",
    ]

Note that the node is called in the first engine cycle even though it uses the default
validity check, which requires its inputs to be valid. That is exactly the point: one element
of the list has ticked, which is enough to make the list itself ``valid``, so the node runs.

Asking for ``all_valid`` instead is the stronger constraint, and it does suppress that first
cycle:

.. testcode::

    from hgraph import compute_node, TSL, TS, Size
    from hgraph.test import eval_node

    @compute_node(all_valid=("tsl",))
    def probe(tsl: TSL[TS[int], Size[2]]) -> TS[str]:
        return f"valid={tsl.valid} all_valid={tsl.all_valid}"

    assert eval_node(probe, [{0: 1}, {1: 2}]) == [
        None,
        "valid=True all_valid=True",
    ]

The node is not evaluated until every element of the list has been set, which is why the
first entry is ``None``.

Finally, the ``last_modified_time`` represents the time this time-series
value was last modified. This can be useful for a number of reasons, but
a simple use-case is to deal with staleness checking of a value.

