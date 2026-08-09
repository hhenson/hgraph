Code Organisation
=================

When approaching the design of your new project here are some things to consider:

* Think about the problem from the view point of information flow.
* Prioritise the use of ``graph`` elements.
* Nodes should be small and preferably general purpose
* Focus on extensive testing at the node / component level.
* Decouple the logic from the deployment.

Describing the problems / solution
----------------------------------

Think about the problem you are trying to solve as a flow of information, the old flow graph
model is a good modeling tool.

For example: You want to extract a signal from a set of market and related data.

**Step 1: Identify the data sources**

In this scenario lets assume we are interested in instrument closing prices and
another measure, lets call it sentiment.
Here we may find the data is present in a database as well as being available from a
subscription to a live stream of the information.

**Step 2: What are the outputs**

We need to produce a time-series of floating point numbers representing the signal for
a set of instruments.

Thus we may start with a stub for the solution that looks a bit like this:

::

    from hgraph import graph, TSD, TS

    @graph
    def generate_signal(market_data: TSD[str, TS[float]], sentiment: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        """
        Extracts a signal from the sentiment and market_data provided.
        The inputs and outputs are keyed by the instruments symbol represented as a string.
        The market data is in USD and the sentiment is a value between -1. and 1. with
        1. being the most positive sentiment and -1 being the most negative sentiment.
        """

Here we have defined the problem in terms of a graph with the inputs and outputs defined
and a description of what is expected from this node.

We can now start to go through the details of how this is done, to keep this simple we
will not make this too complicated, so lets assume the idea is to combine the sentiment
with average returns to rank the instruments. The ranking will be an even ranking
over the set of symbols.

So now we can extend the description of the problem as follows:

::

    @graph
    def generate_signal(market_data: TSD[str, TS[float]], sentiment: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        """
        ...
        """
        returns = compute_returns(market_data)
        combined = combine_sentiment_and_returns(returns, sentiment)
        return rank(combined)

    @graph
    def compute_returns(market_data: TSD[str, TS[float]) -> TSD[str, TS[float]]:
        ...

    @graph
    def combine_sentiment_and_returns(returns: TSD[str, TS[float]], sentiment: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        ...

    @graph
    def rank(raw_signal: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        ...

Here we just try and create stubs for the key concepts of the behaviour to try and
get a feel of code required.

Move from top down to bottom up
-------------------------------

Now we can start looking at the requirements of each of the steps in isolation of the overall
problem.

The ``rank`` graph looks like it could be a useful re-usable component. So the next step
may be to expand this component.

Always start using a ``graph`` decorator, then write out the pseudo code assuming
that each line of code exists (using those items that do when possible). Below is
an example:

::

    @graph
    def rank(raw_signal: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        """
        Takes a raw_signal that needs to be evenly normalised over the range [-1.0,1.0].
        """
        sz = len_(raw_signal)
        keys = sort(raw_signal)
        values = range_(-1.0, step=2.0/sz)
        return combine[TSD[str, TS[float]](keys, values)

We can now review how hard this would be to implement. We know for example that ``len_``
exists in the standard library, but ``sort`` and ``range_`` may not, and ``combine``
probably exists, will need to test.

There is an option to write this as a node, always think carefully about this as nodes
require more testing and debugging. Additionally, these will not benefit from performance improvements
made when the runtime is converted to C++ / Rust.

That said, let's consider implementing this as a compute node to further this example. The code can
then be converted as follows:

::

    @compute_node
    def rank(raw_signal: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        """
        Takes a raw_signal that needs to be evenly normalised over the range [-1.0,1.0].
        """
        sz = len(raw_signal)
        keys = (k for _, k in sorted((v, k) for k, v in raw_signal.value.items()))
        return {k: -1.0 + i * 2.0 / sz for k, i in zip(keys, range(sz))}

Now we have the rank defined, we need to write a test pack for the node.

.. note:: The testing approach is suitable for node's as well as graph components.

For node testing, HGrpah provides a simple testing wrapper called ``eval_node``.

To use this with pytest, do the following:

::

    import pytest
    from hgraph.test import eval_node
    from frozendict import frozendict as fd

    @pytest.mark.parametrize(
    ["raw_signal", "expected"],
    [
       [[fd(a=0.1, b=0.3, c=-3.0)], [fd(c=-1.0, a=0.0, b=1.0)]],
       ...
    ])
    def test_rank(raw_signal, expected):
        assert eval_node(rank, raw_signal) == expected

Running this test will cause the rank node to be evaluated with a time-series input
of raw_signal for the first tick and then evaluate it's response. In this case expecting
a first tick response of expected.

When we run this code should get a failure as we had a bug in the rank value calculation.

::

    >>>  Expected :[frozendict.frozendict({'c': -1.0, 'a': 0.0, 'b': 1.0})]
    >>>  Actual   :[frozendict.frozendict({'c': -1.0, 'a': -0.33333333333333337, 'b': 0.33333333333333326})]


This allows us to cycle and fix, try and find a good number of examples that will touch
normal as well as boundary conditions.

In this case if we correct the node as follows we get a correct result:

::

    @compute_node
    def rank(raw_signal: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        """
        Takes a raw_signal that needs to be evenly normalised over the range [-1.0,1.0].
        """
        sz = len(raw_signal)
        keys = (k for _, k in sorted((v, k) for k, v in raw_signal.value))
        return {k: (-1.0 + i*2.0/(sz-1.0)) for k, i in zip(keys, range(sz))}


NOTE the adjustment of the divisor ``(sz-1.0)`` to get the correct offset alignment.

Integration Testing / Runtime Wiring
------------------------------------

Once the bottom end components are coded and tested, move up the stack. With a final
set of tests that validate the overall behaviour. Depending on the complexity of the
application, you may require some integration tests that run up more of the stack for
testing. Keep these separate from the unit tests.

In this case integration testing will involve creating a main wiring class with
real data sources and sinks.

This could be the same as the final wiring class. The idea here is to separate
the logic (in this case the signal generation logic) from the physical sources of
data used to drive the logic. This allows for using back-test data separately from
real time data sources.

For example, perhaps the backtest data is retrieved from a database, but the real-time
data is sourced from a messaging bus. In this case it is easy to create a main wiring
class using data-base sourced information and a main wiring class using the messaging
bus data sources.

An example of this may look something similar to the code below:

::

    @graph
    def back_test_main_graph():
        market_data = db_reader("select * from market_data_tbl")
        sentiment = db_reader("select * from sentiment_tbl")
        signal = generate_signal(market_data, sentiment)
        db_writer("signal_tbl", signal)

This assumes that there are components: ``db_reader`` and ``db_writer`` that
can read and write from/to a database.

This would allow us to run this graph in simulation mode.

The other main wiring class may look more like the graph below:

::

    @graph
    def real_time_main_graph():
        market_data = subscribe_kafka("market_data")
        sentiment = subscribe_kafka("sentiment")
        signal = generate_signal(market_data, sentiment)
        publish_kafka("signal", signal)

In this scenario we assume there are publish and subscribe components that
we can use to access a stream of data and to publish the data to.

This approach allows for great flexibility and re-use of the code.

When constructing back-test or integration testing it is possible to collect
multiple graphs into a single process, and in production split the processes up
into multiple processes. This approach insures deployment is decoupled from
the business logic implementation.



