hgraph
======

HGraph is a functional reactive programming engine for computing results over
time. Programs are described as forward propagation graphs over time-series
values, and the same description runs in simulation or against a live event
stream.

Version 0.8 uses a C++ runtime with Python as the primary end-user authoring
interface. The ``hgraph`` package provides the supported DSL and compatibility
commitment for the release line. A native C++ authoring API is also available
for library authors and applications that need maximum performance; both paths
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

The documentation is organised into five tracks:

User Guide
    The programming model and how to author, wire, test and diagnose graphs in
    Python. Advanced native C++ authoring is included as a separate section.

Python API Reference
    The supported types, decorators, operators, modules and generated public
    inventory.

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
   reference/index
   specification/index
   developer_guide/index
   rfc/index
   papers/index
   references
