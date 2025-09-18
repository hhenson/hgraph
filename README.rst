HGraph
======

A functional reactive programming engine with a Python front-end.

This provides a Domain Specific Language (DSL) and runtime to support the computation of results over time, featuring
a graph based directed acyclic dependency graph and the concept of time-series properties.
The language is function based, and promotes composition to extend behaviour.

Here is a simple example:

.. testcode::

    from hgraph import graph, evaluate_graph, GraphConfiguration, const, debug_print
    from logging import INFO

    @graph
    def main():
        a = const(1)
        c = a + 2
        debug_print("a + 2", c)

    evaluate_graph(main, GraphConfiguration(default_log_level=INFO))

Results in:

.. testoutput::

    [1970-01-01 00:00:00...][1970-01-01 00:00:00.000001] a + 2: 3



Development
-----------

The project is currently configured to make use of `uv <https://github.com/astral-sh/uv>`_ for dependency management.
Take a look at the website to see how best to install the tool.

Here are some useful commands:

First, create a virtual environment in the project directory:

.. code-block:: bash

    uv venv

Then use the following command to install the project and its dependencies:

.. code-block:: bash

    # Install the project with all dependencies
    uv pip install -e .

    # Install with optional dependencies
    uv pip install -e ".[docs,web,notebook]"

    # Install with all optional dependencies
    uv pip install -e ".[docs,web,notebook,test]"

PyCharm can make use of the virtual environment created by uv to ``setup`` the project.

Run Tests
.........

.. code-block:: bash

    # No Coverage
    python -m pytest

.. code-block:: bash

    # Generate Coverage Report
    python -m pytest --cov=hgraph --cov-report=xml


Indexing with Context7 MCP
--------------------------

This repository includes a baseline configuration for Context7 MCP to improve code search and retrieval quality.

- See ``docs/context7_indexing.md`` for guidance.
- The root-level ``context7.yaml`` config sets sensible include/exclude rules, priorities, and summarization hints.
