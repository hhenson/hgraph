Advanced native C++ authoring
=============================

Graphs and nodes can be authored, wired, tested, and executed entirely in C++
against the native runtime API. This surface is intended primarily for library
authors, native extensions and applications whose hot path requires maximum
performance. Most end users should start with the Python package.

Start with :doc:`quick_start` to go from a clean checkout to a first tested
node. The authoring and testing pages are the full reference for the C++ API.

.. note::

   The native C++ authoring API is source- and binary-provisional while it is
   refined by production use. The Python API is the compatibility commitment
   for the 0.8 release line (see :doc:`../python_compatibility`).

.. toctree::
   :maxdepth: 2

   quick_start
   authoring_nodes
   authoring_graphs
   testing_graphs
