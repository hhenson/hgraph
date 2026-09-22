Specification
=============

The current concept-first model is the
:doc:`runtime behaviour specification <../runtime_spec/index>`. It combines
runtime chapters, explicit conformance cases and source evidence, and replaces
the unmerged runtime-specification experiments.

The chapters below are the earlier Python-era draft. They remain reference
material for wiring, operators and other areas outside the new runtime core;
they are not a second authoritative runtime model. Their unreconciled claims
remain provisional.

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
