hgraph
======

hgraph is a C++ first implementation of the HGraph runtime model. It preserves the current Python ecosystem where that ecosystem is strongest: wiring, graph authoring compatibility, and user-authored Python nodes. The runtime, system nodes, graph execution, and native graph/node APIs are implemented in C++ as the primary source of truth.

This documentation is split into two tracks:

User Guide
    The programming model, core concepts, and expected user-facing behavior.

Developer Guide
    Internal design notes for implementing the runtime, memory model, data structures, schema system, wiring boundary, and Python integration.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   user_guide/index
   developer_guide/index
   rfc/index
