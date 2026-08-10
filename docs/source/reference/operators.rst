Operators
=========

Operators are public module attributes backed by the native overload registry.
They accept wiring ports, lift compatible scalar values when appropriate, and
return a new wiring port (or no port for a sink). A trailing underscore is part
of several public names—``add_``, ``filter_`` and ``print_`` are not private.

Use ordinary Python syntax where it is clearer. ``lhs + rhs`` wires ``add_``;
comparisons, indexing and attribute projection similarly delegate to the
registry.

Common families
---------------

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Family
     - Representative operators
   * - Arithmetic and logic
     - ``add_``, ``sub_``, ``mul_``, ``div_``, ``and_``, ``or_``, ``not_``
   * - Validity and flow
     - ``valid``, ``modified``, ``sample``, ``filter_``, ``default``, ``dedup``
   * - Collections
     - ``combine``, ``collect``, ``emit``, ``keys_``, ``values_``, ``merge``
   * - Dynamic graphs
     - ``map_``, ``reduce``, ``mesh_``, ``switch_``, ``dispatch_``
   * - Windows and statistics
     - ``to_window``, ``rolling_average``, ``mean``, ``std``, ``quantile``
   * - Temporal
     - ``at_zone``, ``resolve_civil``, ``temporal_floor``, ``range_union``
   * - Frames and JSON
     - ``to_data_frame``, ``with_columns``, ``to_json``, ``from_json``
   * - I/O and diagnostics
     - ``debug_print``, ``log_``, ``print_``, ``record``, ``replay``

Subscripted resolution
----------------------

Many operators accept a type-selection subscript when their output cannot be
inferred from inputs:

.. testcode::

   from hgraph import TS, const, graph
   from hgraph.test import eval_node

   @graph
   def typed_constant() -> TS[int]:
       return const[TS[int]](1)

   assert eval_node(typed_constant) == [1]

The selected overload, scalar options and input schemas determine the final
signature. Browse every accepted native overload, along with its semantic
description, in the :doc:`operator_catalogue`. The compact
:doc:`python_api_inventory` summarizes how those names are exposed.
