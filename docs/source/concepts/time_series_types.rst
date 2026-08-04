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

.. plantuml::

    @startuml
    state node {
        state ts_1 <<inputPin>>
        state ts_i <<inputPin>>
        state ts_n <<inputPin>>
        state out <<outputPin>>
    }
    @enduml


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

.. plantuml::

    @startuml
    state const {
        state out <<outputPin>>
    }
    state debug_print {
        state ts <<inputPin>>
    }
    out --> ts
    @enduml

Time-series properties
----------------------

The time-series types have the following properties:

.. plantuml::

    @startuml
    class TimeSeries {
        owning_node: Node
        owning_graph: Graph
        value
        delta_value
        modified: bool
        valid: bool
        all_valid: bool
        last_modified_time: datetime
    }

    class TimeSeriesInput {
        active: bool
        make_active()
        make_passive()
    }

    TimeSeries <|-- TimeSeriesInput
    TimeSeries <|-- TimeSeriesOutput
    @enduml

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

The ``all_valid`` flag is ``True`` when all inputs / outputs of a collection
type are valid, for example in a TSL (time-series list), it is possible that
only some of the elements in the list could be valid and others not yet
valid. The ``all_valid`` property ensures that each input is valid. This
is a stronger requirement then ``valid`` which becomes true as soon as at
least one element becomes valid. Checking for ``all_valid`` is potentially
a very expensive operation and as such should only be used when this
constraint is actually required to be enforced.

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

