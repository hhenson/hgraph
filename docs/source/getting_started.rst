Getting Started
===============

HGraph is a Python package that can be installed with pip. It requires Python 3.12 or greater,
and ships as a pre-built wheel per platform (Linux ``manylinux_2_28``, macOS arm64, Windows) using
the CPython stable ABI, so no compiler is needed to install it.

To install:

.. code-block:: bash

    pip install hgraph

The runtime underneath is C++; the ``hgraph`` package is the Python authoring surface over it. Most
adaptors are behind packaging extras — ``web``, ``sql``, ``snowflake``, ``delta``, ``perspective``,
``dataframe`` — so a plain install stays small:

.. code-block:: bash

    pip install "hgraph[web]"

Kafka is not an extra. It is a separate distribution built against the hgraph SDK, so install it
alongside:

.. code-block:: bash

    pip install hgraph-kafka

If you would rather author graphs natively, with no Python involved at all, start at
:doc:`user_guide/cpp/quick_start` instead.

To use HGraph, you need to:

.. code-block:: Python

    import hgraph as hg

Then you can create your master wiring graph, for example:

.. code-block:: Python

    @hg.graph
    def hello_world():
        hg.debug_print("Hello", hg.const("World"))

Note that ``debug_print`` takes a scalar label (``"Hello"``) and a time-series to print. The
``const`` operator turns the plain Python string ``"World"`` into a ``TS[str]`` that ticks once,
at the start of the run. Writing ``hg.debug_print("Hello", "World")`` also works, because the
wiring layer lifts a scalar into a ``const`` for you when it is passed to a time-series input.
The explicit ``const`` is spelled out here to make the distinction between the two kinds of
argument visible; the first is configuration, the second is data flowing through the graph.

Finally you setup the main run-loop and start the graph running.

.. code-block:: Python

    if __name__  == '__main__':
        config = hg.GraphConfiguration()
        hg.evaluate_graph(hello_world, config)

Putting it together, the complete program is:

.. testcode::

    import hgraph as hg
    from logging import INFO

    @hg.graph
    def hello_world():
        hg.debug_print("Hello", hg.const("World"))

    hg.evaluate_graph(hello_world, hg.GraphConfiguration(default_log_level=INFO))

which prints:

.. testoutput::

    Hello: World

From here, :doc:`user_guide/python/quick_start/index` walks through the run loop, graphs and nodes, and
:doc:`user_guide/python/tutorial/index` covers the same ground in more detail. For the ideas underneath
the syntax, read :doc:`user_guide/concepts/index`.

