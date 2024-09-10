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

Then you can create you master wiring graph, for example:

.. code-block:: Python

    @hg.graph
    def main():
        hg.debug_print("Hello", "World")

Finally you setup the main run-loop and start the graph running.

.. code-block:: Python

    if __name__  == '__main__':
        config = hg.GraphConfig()
        hg.evaluate_graph(main, config)



