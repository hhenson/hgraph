Work with collections and deltas
================================

Choose the topology from how the collection changes, not merely from the
Python container you happen to hold.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Type
     - Choose it when
   * - ``TS[tuple[...]]``
     - The entire tuple is one scalar value and changes atomically.
   * - ``TSL[V, Size[N]]``
     - The number of independently ticking elements is fixed.
   * - ``TSS[T]``
     - Membership changes and additions/removals matter.
   * - ``TSD[K, V]``
     - Keys appear and disappear and each value ticks independently.
   * - ``TSB[Schema]``
     - A fixed set of named fields ticks independently.

Return an explicit set delta
----------------------------

.. testcode::

   from hgraph import TSS, compute_node, set_delta
   from hgraph.test import eval_node

   @compute_node
   def only_even_changes(values: TSS[int]) -> TSS[int]:
       return set_delta(
           added={value for value in values.added() if value % 2 == 0},
           removed={value for value in values.removed() if value % 2 == 0},
       )

   assert eval_node(only_even_changes, [
       {1, 2},
       set_delta(added={3, 4}, removed={2}),
   ]) == [
       set_delta(added={2}),
       set_delta(added={4}, removed={2}),
   ]

For a ``TSD`` output, return a delta dictionary. Associate ``REMOVE`` with a
key to remove it; use ``REMOVE_IF_EXISTS`` when absence should not be an error.

Frames and arrays
-----------------

Use ``Frame[Rows]`` for Arrow-native tables and ``Array[T, Size[...]]`` for
shaped numeric scalar values. Python receives ``pyarrow.Table`` and
``numpy.ndarray`` boundary values, while registered operators execute in the
native runtime. See :doc:`../../data_and_analytics` for metadata, sources,
windows and numerical operators.
