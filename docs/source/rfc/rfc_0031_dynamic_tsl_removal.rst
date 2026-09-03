RFC 0031: Dynamic TSL Added and Removed
=======================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-09-02
:Target: ``TSL<C, 0>`` delta contract, ``DynamicTSLStorage``, TSL views, Python bridge
:Related: RFC 0017 (binary value codec), RFC 0019 (native table recording)

Summary
-------

Give a dynamic ``TSL`` (``TSL<C, 0>``) a structural removal surface so it can
shrink as well as grow, and expose the resulting per-cycle structure change as
``added`` and ``removed`` index ranges on the TSData, input, and output views.

Removal is **tail truncation**: a dynamic ``TSL`` remains a true list. Its live
size grows and shrinks, only trailing indices are ever removed, and
``values()`` / ``items()`` stay dense. There are no holes and no sparse index
set; ``size()`` remains an element count, never an extent.

The canonical delta for a dynamic ``TSL`` changes from ``Map<int, delta(C)>``
to ``Bundle{removed: Set<int>, modified: Map<int, delta(C)>}``, matching the
``TSD`` delta shape. A fixed-size ``TSL`` is unaffected and keeps
``Map<int, delta(C)>``.

Motivation
----------

Dynamic ``TSL`` was introduced grow-only, and the reason was representational
rather than semantic:

    "Because that delta schema has no removal surface, dynamic ``TSL`` TSData
    is currently grow-only; copying a shorter list is rejected."
    -- ``data_structures/plans_and_ops/time_series.rst``

That restriction leaks into every producer of a dynamic list:

* ``apply_current_value`` and ``reconcile_current_state`` reject a shorter
  current value;
* ``copy_value_from`` / ``move_value_from`` reject a shorter source list;
* the Python bridge rejects a shorter sequence; and
* ``map_`` over dynamic ``TSL`` documents "no keyed remove/erase protocol
  because a dynamic TSL does not remove positions", so a child graph created
  for an index can never be retired.

A dynamic ``TSL`` is the natural target for a variable-length source: a
``tuple`` that shrinks, a fan-out whose arity falls, a windowed slice. Every
such source currently either throws or has to be modelled as ``TSD[int, C]``,
which gives up ordering, density, and the cheap positional child addressing
that motivated the dynamic list in the first place.

Non-goals
---------

* **Sparse removal.** Removing an interior index, leaving a hole, is
  explicitly rejected. That shape is ``TSD[int, C]`` and already exists.
* **Fixed ``TSL`` changes.** A fixed-size ``TSL`` has no structural delta and
  keeps its ``Map<int, delta(C)>`` delta schema unchanged.
* **Index re-labelling.** Truncation does not renumber surviving elements.
  Index ``i`` always addresses the same stable child while it is live.

Ownership boundary
------------------

The removal primitive, the delta shape, and the ``added`` / ``removed`` view
surfaces are core. Policy deciding *when* a list shrinks stays with the
producing node or graph.

Semantics
---------

Live size and the delta window
..............................

A dynamic ``TSL`` storage tracks:

``live_size``
   The number of elements currently in the list. ``size()`` reports this.

``previous_size``
   The live size when the current delta window opened.

``delta_time``
   The evaluation time the current delta window describes.

The delta window is opened lazily by the first mutation at a newer time, using
the same monotonic rule as ``TSD`` (``ts_data_slot_ops.cpp::prepare_delta``): a
mutation carrying a time older than or equal to the open window joins it and
never rebases it.

For an open window at ``evaluation_time``:

.. code-block:: text

   added_indices   = [previous_size, live_size)   when live_size > previous_size
   removed_indices = [live_size, previous_size)   when previous_size > live_size

At most one of the two is non-empty, and both are contiguous. When the window
does not describe ``evaluation_time``, both are empty.

Retention and resurrection
..........................

Truncated elements follow the ``TSD`` slot contract:

* ``stop_owned_ts_data_tree`` runs on each truncated child at removal time;
* the child's storage is **retained** for the rest of the delta window so
  ``removed_values()`` and ``removed_items()`` can read its last value;
* growing back into a retained index within the same window **resurrects** the
  same child with its payload intact, exactly as ``TSD`` reinsertion does; and
* the next window roll invalidates observers
  (``invalidate_owned_ts_data_tree``) and only then destroys the retained
  tail.

The physical element extent is therefore ``max(live_size, previous_size)``, and
the storage owns every element in it.

Delta contract
..............

For ``TSL<C, 0>``:

.. code-block:: text

   delta_value_schema    = Bundle{removed: Set<int>, modified: Map<int, delta(C)>}
   authored_delta_schema = Bundle{removed: Set<int>, modified: Map<int, authored_delta(C)>}

As with ``TSD``, ``removed`` is a statement of fact about the source: those
indices existed and no longer do. Unlike ``TSD`` there is no ``removed_strict``
field — a list truncation is total and cannot fail against an absent index, so
the ``REMOVE`` / ``REMOVE_IF_EXISTS`` distinction has nothing to express.

**Applying a delta** is lenient and order-defined:

1. If ``removed`` is non-empty, truncate the target to
   ``min(current_size, min(removed))``. Indices in ``removed`` above that point
   are implied by the truncation and need not be present.
2. Apply ``modified``, which may re-grow the list.

This ordering makes a captured delta round-trip exactly, including the
shrink-then-grow case within one cycle: a list going ``5 -> 3 -> 4`` captures
``removed = {4}``, ``modified = {3: d}`` and replays to size 4.

A fixed ``TSL`` continues to use ``Map<int, delta(C)>`` and has no removal.

C++ contract
------------

Ops table
.........

``IndexedTSDataOps`` gains optional hooks, following the established
nullable-hook idiom already used by ``modified_index_count_impl``. Null means
"this indexed shape has no structural delta", which is the fixed ``TSL`` /
``TSB`` answer:

.. code-block:: cpp

   /** Live-size window describing the open structural delta, if any. */
   struct IndexedStructuralDelta
   {
       DateTime    time{MIN_DT};      ///< Window time; MIN_DT when no window is open.
       std::size_t previous_size{0};  ///< Live size when the window opened.
       std::size_t size{0};           ///< Live size now.
   };

   struct IndexedTSDataOps : TSDataOps
   {
       // ... existing members ...

       /** Optional dynamic-list surface; null on fixed indexed shapes. */
       IndexedStructuralDelta (*structural_delta_impl)(
           const void *context, const void *memory) = nullptr;
       /** Element memory for a retained (removed-this-cycle) index. */
       const void *(*retained_element_memory_impl)(
           const void *context, const void *memory, std::size_t index) = nullptr;
       /** Grow or truncate the live list. */
       void (*resize_impl)(const void *context, void *memory,
                           std::size_t size, DateTime modified_time) = nullptr;
   };

Views
.....

``IndexedTSDataView`` gains, with the fixed shapes answering empty:

.. code-block:: cpp

   [[nodiscard]] bool supports_resize() const noexcept;
   [[nodiscard]] std::size_t previous_size(DateTime evaluation_time) const;
   [[nodiscard]] Range<std::size_t> added_indices(DateTime evaluation_time) const;
   [[nodiscard]] Range<std::size_t> removed_indices(DateTime evaluation_time) const;
   [[nodiscard]] Range<TSDataView> added_values(DateTime evaluation_time) const;
   [[nodiscard]] Range<TSDataView> removed_values(DateTime evaluation_time) const;
   [[nodiscard]] KeyValueRange<std::size_t, TSDataView> added_items(DateTime) const;
   [[nodiscard]] KeyValueRange<std::size_t, TSDataView> removed_items(DateTime) const;

``TSLInputView`` and ``TSLOutputView`` mirror those without the explicit time,
matching how ``TSDInputView`` / ``TSDOutputView`` present ``added_keys()`` and
``removed_keys()``:

.. code-block:: cpp

   Range<std::size_t> added_indices() const;
   Range<std::size_t> removed_indices() const;
   Range<TSInputView /* TSOutputView */> added_values() const;
   Range<TSInputView /* TSOutputView */> removed_values() const;
   KeyValueRange<std::size_t, TSInputView /* TSOutputView */> added_items() const;
   KeyValueRange<std::size_t, TSInputView /* TSOutputView */> removed_items() const;

A ``removed_*`` range projects the retained child for the rest of the cycle. A
fixed ``TSL`` returns empty ranges rather than throwing, so generic code over
``TSLInputView`` stays uniform.

Mutation
........

Dynamic ``TSL`` growth is already implicit in ``TSLOutputView::at(index)``.
Shrink is explicit and keeps that same "mutating accessor on the output view"
pattern rather than introducing a new mutation-view class:

.. code-block:: cpp

   /** Set the live list length, growing or truncating. Dynamic TSL only. */
   void TSLOutputView::resize(std::size_t size) const;

``resize`` on a fixed ``TSL`` throws ``std::logic_error``. Growing through
``resize`` is equivalent to touching ``at(size - 1)``.

Python contract
---------------

The friendly Python delta shape needs no new code: ``_simplify_delta`` already
rewrites ``{"removed": ..., "modified": ...}`` into ``{index: delta, removed_index:
REMOVE}``. A dynamic ``TSL`` delta therefore reads in Python exactly as a
``TSD[int, C]`` delta does, and a fixed ``TSL`` keeps its plain ``{index:
delta}`` dict.

Authoring from Python:

* A **sequence** result sets the whole list and **resizes** to its length;
  a shorter sequence truncates. ``None`` at a position still means "this
  element does not tick".
* A **mapping** result is a sparse update. A value of ``REMOVE`` (or
  ``REMOVE_IF_EXISTS``) truncates the list to the lowest removed index. The
  two sentinels behave identically, as removal cannot fail.
* Mixing an index above the truncation point with a removal is resolved by the
  same order as ``apply_delta``: truncate, then apply the modifications.

``PyTimeSeries`` / ``PyOutput`` gain ``added`` and ``removed`` accessors for a
dynamic ``TSL``, returning index tuples.

Serialization and compatibility
-------------------------------

* **Delta schema change.** Recordings and wire payloads carrying a dynamic
  ``TSL`` delta produced before this change use the bare ``Map<int, delta(C)>``
  shape and are not readable as the new bundle. Recording versions
  (RFC 0021) gate the change; the binary codec (RFC 0017) and the table
  protocol are schema-driven and need no format-specific work beyond the new
  shape flowing through.
* **JSON.** For a dynamic ``TSL`` the array form now *resizes* the list to the
  array length (a shorter array truncates), and the object form is the
  canonical delta bundle ``{"removed": [..], "modified": {..}}``. Fixed ``TSL``
  JSON is unchanged: the array form must fit, and the object form is the index
  map.
* **ABI.** ``IndexedTSDataOps`` grows three trailing function pointers.
  Extensions that construct the struct with designated initialisers are source
  compatible; the SDK ABI version is bumped.

Performance and memory
----------------------

The structural delta is three words (``time``, ``previous_size``,
``live_size``) rather than ``TSD``'s two dynamic bitsets, because truncation is
contiguous. ``added_indices`` / ``removed_indices`` are computed ranges with no
allocation and no per-element scan. The retained tail costs the same memory the
elements already occupied, for at most one delta window. Per-tick cost of the
existing grow and modify paths is unchanged; ``prepare_delta`` adds one
``DateTime`` compare to the mutation path that ``record_child_modified``
already performed inline.

Alternatives considered
-----------------------

``Bundle{size: int, modified: Map<int, delta(C)>}``
   A single new size field is the most compact encoding of a truncation. It
   was rejected because it makes the dynamic ``TSL`` delta unlike every other
   collection delta in the system: consumers, ``_simplify_delta``, the JSON
   object form, and the table protocol all already understand
   ``{removed, modified}``, and a bare ``size`` would need bespoke handling in
   each.

Sparse index removal
   Rejected as a non-goal above: it duplicates ``TSD[int, C]`` while
   destroying list density.

Keeping the delta a map with a removal sentinel value
   Encoding removal as a reserved value inside ``Map<int, delta(C)>`` avoids a
   schema change but makes the delta value schema depend on ``C`` admitting a
   sentinel, which it does not in general.

Acceptance criteria
-------------------

* A dynamic ``TSL`` output shrinks through ``resize`` and reports
  ``removed_indices`` / ``removed_items`` for the cycle, with retained child
  values readable.
* Growth reports ``added_indices`` / ``added_items``.
* Shrink-then-grow inside one cycle resurrects the retained child and reports
  the net structure change only.
* The next cycle's first mutation invalidates and destroys the retained tail;
  ASan/UBSan clean.
* ``capture_delta`` / ``apply_delta`` round-trip a shrink, a grow, and a
  combined shrink-plus-modify.
* ``apply_current_value``, ``reconcile_current_state``, ``copy_value_from``,
  ``move_value_from``, and the Python sequence path all accept a shorter
  source.
* Python ``.delta_value`` for a dynamic ``TSL`` reports ``{index: value,
  removed_index: REMOVE}``; a Python node returning a shorter tuple truncates.
* ``map_`` over a shrinking dynamic ``TSL`` stops and destroys the child graphs
  for removed indices and truncates its output.
* Fixed ``TSL`` behaviour, delta schema, and JSON form are unchanged.

Implementation status
---------------------

Implemented. Differences from the proposal as first written:

* **Growth became a timed operation.** Attributing a new index to the delta
  window needs the evaluation time, so ``TSDataView`` gained
  ``ensure_indexed_child_at(index, modified_time)`` and the dynamic list's
  ``mutable_element_memory_impl`` no longer grows. The untimed overload throws
  for a shape with a ``resize_impl``. Without this, an index added before the
  first timed write of a cycle was silently missing from ``added_indices``, and
  an untimed re-grow could resurrect a child from the *previous* cycle's
  retained tail.
* **The window time reuses the modified-ring header.** Storing a separate
  ``DateTime`` cost 8 bytes and could disagree with the ring; the storage grew
  by two lengths only, from 96 to 112 bytes on macOS/arm64.
* **Structural-delta ranges bind to the ops table and storage memory**, not to
  the view object. The typed list views forward them from a temporary
  ``TSLDataView``, so a ``this``-bound range dangled (it segfaulted the first
  test that read one through ``TSLOutputView``).
* **A retained child needs a detached input view.**
  ``TSInputView::child_from_target`` re-resolves the target path on every read,
  which no longer reaches a truncated index, so
  ``TSInputView::child_from_retained`` was added: it holds the retained TSData
  directly and never re-resolves.
* **A full reconciliation resizes instead of invalidating the surplus.**
  ``reconcile_indexed_impl`` used to invalidate children past the source length;
  for a resizable target that call re-grew the list straight after the
  truncation, so the surplus-invalidation loop is now skipped for it.
* **No ``removed_strict``.** The proposal already noted truncation cannot fail;
  the implementation confirms both Python sentinels behave identically.
* ``dynamic_list_delta`` was added beside ``list_delta`` in the test-authoring
  API (``static_node.h``), because those builders are parameterised on the
  element schema and cannot tell a fixed list from a dynamic one.

Conformance tests: ``tests/cpp/test_plan_factories.cpp`` (storage, retention,
same-cycle resurrection), ``tests/cpp/test_ts_input.cpp`` (input-side
added/removed and retained values), ``tests/cpp/test_ts_delta.cpp``
(capture/apply round trip, reconciliation), ``tests/cpp/test_map.cpp`` and
``tests/cpp/test_reduce.cpp`` (child retirement, leaf retirement),
``tests/cpp/test_json.cpp`` (array length, delta round trip), and
``python/tests/test_python_authoring.py`` (tuple/REMOVE truncation, added and
removed indices, ``map_``).
