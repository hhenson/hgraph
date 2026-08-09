Specification
=============

The HGraph Language Design Specification is a precise, language-neutral
definition of HGraph semantics: the type system, the wiring model, the runtime
execution model, the time-series types, the node kinds, and the operator
contracts. It is written so that a conforming HGraph runtime could be
implemented from it.

.. note::

   **Reference implementation.** As of 0.8 the C++ runtime in this repository
   is the reference implementation, and the ``hgraph`` Python package is an
   authoring surface over it. Where this specification and the C++ runtime
   disagree, the runtime is correct and the specification has a bug — report
   it. Behaviour that intentionally differs from the earlier Python-first
   implementation on ``release/0.5`` is recorded in
   :doc:`../developer_guide/parity_matrix`, which is the authoritative list of
   accepted deviations.

   These documents were written against the Python-first implementation and
   carry a 1.0-Draft status. They are being reconciled section by section
   against the runtime and the parity matrix; treat an unreconciled claim as
   provisional rather than normative.

How this differs from the other tracks
--------------------------------------

The :doc:`../user_guide/index` teaches the model. The
:doc:`../developer_guide/index` records how *this* runtime implements it —
memory layout, ops tables, plans and schemas — and is authoritative for
implementation questions. This specification sits between them: it states what
any implementation must do, without committing to how.

.. toctree::
    :maxdepth: 2
    :caption: Specification Documents

    00_INDEX
    01_OVERVIEW
    02_TYPE_SYSTEM
    03_WIRING_SYSTEM
    04_RUNTIME_SYSTEM
    05_TIME_SERIES_TYPES
    06_NODE_TYPES
    07_OPERATORS
    08_ADVANCED_CONCEPTS
    09_CONTROL_FLOW
    10_DATA_SOURCES
