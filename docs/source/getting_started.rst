Getting Started
===============

HGraph is a python package that can be installed using the pip. This package requires Python 3.11 or
greater (currently tested with Python 3.11 and 3.12).

To install:

.. code-block:: bash

    pip install hgraph

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

    ... Hello: World

From here, :doc:`quick_start/index` walks through the run loop, graphs and nodes, and
:doc:`tutorial/index` covers the same ground in more detail.



