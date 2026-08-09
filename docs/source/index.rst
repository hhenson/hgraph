hgraph
======

HGraph is a functional reactive programming engine for computing results over
time. Programs are described as forward propagation graphs over time-series
values, and the same description runs in simulation or against a live event
stream.

Version 0.8 is a C++ runtime with two authoring surfaces. The ``hgraph`` Python
package provides the DSL and is the compatibility commitment for the release
line; the native C++ API authors graphs with no Python involved at all. Both
describe the same graphs and obey the same evaluation semantics.

.. code-block:: bash

    pip install hgraph

.. code-block:: python

    import hgraph as hg

    @hg.graph
    def hello_world():
        hg.debug_print("Hello", hg.const("World"))

    hg.evaluate_graph(hello_world, hg.GraphConfiguration())

:doc:`getting_started` expands that into a working program.

The documentation is organised into four tracks:

User Guide
    The programming model, the concepts the runtime implements, and how to
    author, wire and test graphs in Python or C++.

Specification
    A precise, language-neutral definition of HGraph semantics, written so a
    conforming runtime could be implemented from it.

Developer Guide
    The authoritative internal design record: runtime, memory model, data
    structures, schemas, the wiring boundary and the Python integration.

RFCs
    Proposed and accepted changes to public types, runtime, operator model and
    extension surface.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   getting_started
   user_guide/index
   specification/index
   developer_guide/index
   rfc/index
   papers/index
   references
