Authoring in Python
===================

The ``hgraph`` package is the Python authoring surface over the C++ runtime.
Graphs described with the Python DSL are wired, type-checked and then executed
by the native runtime; Python-authored nodes run inside it.

Install it with ``pip install hgraph`` and work through
:doc:`../../getting_started` first.

:doc:`quick_start/index`
    The impatient path. Enough to run a graph and understand what you just ran.

:doc:`tutorial/index`
    The same ground at working depth: program structure, testing, compute
    nodes, injectables, typing, generics, operators and graphs.

:doc:`tasks/index`
    Recipes for running, testing, diagnosing, working with collections, and
    connecting services or adaptors.

:doc:`programming_model/index`
    How to *think* in HGraph, and how to organise a codebase once a program
    outgrows a single file.

For what the Python surface does and does not expose relative to the native
runtime, see :doc:`../python_compatibility`.

.. toctree::
    :maxdepth: 2

    quick_start/index
    tasks/index
    tutorial/index
    programming_model/index
