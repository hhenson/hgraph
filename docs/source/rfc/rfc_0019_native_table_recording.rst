RFC 0019: Native Table Recording for Partitioned Time-Series
============================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-12
:Target: Table codec, record/replay data-frame backend, and the Python data-frame adaptor

Summary
-------

Make the native recorder able to record everything the Python data-frame
adaptor records — ``TSD`` partitions, removals, multi-row frames — so that
recording a data frame is one implementation rather than two, and so the
per-tick path never enters Python.

The missing piece is smaller and better-shaped than it first appears. C++
already flattens a partitioned time-series into table rows, and C++ already
appends rows into Arrow builders. What does not exist is a recorder driven by
the *table layout*: the Arrow recorder is built from a plain value schema and
has never known about partition levels.

This RFC adds that recorder, fuses row emission with column appending so no row
tuples are materialised on either side of the boundary, and gives recording an
explicit options surface — as-of tracking, removal tracking, column naming,
record mode, flush policy — rather than the ad-hoc dictionary the adaptor
carries today.

Motivation
----------

Recording a ``TSD`` to a data frame currently fails natively:

.. code-block:: text

   RuntimeError: node[1 'g.record'] start failed:
     table codec: unsupported value kind for 'Map[str,float]'
     (atomics and depth-1 bundles in v1)

So the Python adaptor implements recording itself: it takes rows from
``to_table``, pulls every cell into Python, builds Arrow, and writes through
``DataFrameStorage``. That is the only route for the shapes people actually
record, and it has already drifted once from the columnar-builder design
``record_replay_table.rst`` mandates — it was building a one-row Arrow table
per tick and concatenating, which cost 12.3s and 380MB peak where the batched
form costs 0.77s and 226MB on the same 20 000-tick sparse ``TSD``.

Fixing that drift did not remove the reason it happened. Two implementations of
one capability, one of them in Python on the per-tick path, will drift again.

**A measurement that shapes this RFC.** On a flat ``TS[int]``, the native
recorder and a batched Python recorder measure the same — 0.23s vs 0.21s,
0.48MB retained each, over 20 000 ticks. The per-tick Python boundary is
therefore *not* the dominant cost at that shape, and this RFC should not be
sold on speed alone. What it buys is a single implementation, native support
for the shapes that currently have none, and the removal of a row-materialising
step that both paths pay today.

Current state
-------------

.. list-table::
   :header-rows: 1
   :widths: 26 32 42

   * - Piece
     - Where
     - What it does / does not do
   * - ``to_table`` row flattening
     - ``table_impl.cpp``, ``TsTableLayout``
     - **Handles everything**: nested ``TSD`` levels, per-level key columns and
       removed flags, multi-row frame leaves, Tick/Sample/Snap modes. But
       ``emit_partition_rows`` materialises a ``Value`` row per row into a
       ``std::vector<Value>``.
   * - ``TableConverter`` / ``FrameRecorder``
     - ``table_codec.cpp``
     - Appends **straight into Arrow builders**, no row tuples — the shape the
       design record asks for. Built from a *value schema*; atomic leaves and
       depth-1 bundles only. Knows nothing of partition levels.
   * - ``record`` (``DATA_FRAME`` model)
     - ``record_replay_frame_impl.h``
     - Uses ``FrameRecorder``, writes the finished frame to the RFC 0016 frame
       store. Fails at ``start`` for any partitioned type.
   * - Python data-frame adaptor
     - ``adaptors/data_frame``
     - Consumes ``to_table`` rows in Python, builds Arrow, writes through
       ``DataFrameStorage``. Carries the override surface: ``track_as_of``,
       ``track_removes``, partition-key and removed-key renames.

The gap is one component: **an Arrow recorder driven by ``TsTableLayout``
rather than by a value schema.** Everything either side of it exists.

Design
------

Record the layout, not the value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``TsTableLayout`` already computes what an Arrow schema needs: ``keys`` (all
column names in row order), ``col_metas`` (per-column leaf metadata),
``levels`` (per ``TSD`` level: key columns and the removed-flag column),
``value_cols``, and the ``is_multi_row`` frame case.

A ``TableRecorder`` is constructed from a layout plus options, holds one
``arrow::ArrayBuilder`` per column, and exposes the same two verbs the existing
recorder does — append a row, finish a frame. The existing ``FrameRecorder``
becomes the degenerate case of it (no levels), which keeps one code path rather
than two that look alike.

Fuse emission with appending
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``emit_partition_rows`` currently walks the levels and pushes a materialised
``Value`` row per row. Recording then reads those rows back. The design record
(*I5*) already rejects that shape: "materialising a row value per tick, then
accumulating rows into a frame in the recorder, costs two copies".

The walk is therefore parameterised by a row sink. ``to_table`` keeps the
existing sink that materialises rows, because its output *is* a row-valued
time-series. Recording supplies a sink that appends cells directly into the
builders, so no row value is ever built. This removes a copy from the native
path that exists today even for the shapes it already supports.

Options
~~~~~~~

The adaptor's overrides become an explicit, validated structure rather than a
dictionary read at three call sites. Recording configuration is per recordable
key, so a graph can record two series with different policies:

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Option
     - Meaning
   * - ``as_of``
     - ``Track`` (default), ``Omit`` — no as-of column at all — or ``Fixed``
       with a supplied time. The column name comes from ``Config::as_of_key``
       and may be overridden per key.
   * - ``removes``
     - ``Track`` (default) or ``Omit``. When omitted the removed-flag column is
       dropped from every level and removal rows are not emitted. Per-level
       column names may be supplied, which is what
       ``remove_partition_keys`` does today.
   * - ``partition_names``
     - Per-level replacement names for the key columns, defaulting to the
       layout's. Today's ``partition_keys`` override.
   * - ``date_key``
     - The value-time column name; defaults to ``Config::date_key``.
   * - ``mode``
     - ``Tick`` (default), ``Sample``, ``Snap`` — the existing
       ``ToTableMode``, which the recorder must honour because it changes which
       rows exist.
   * - ``flush``
     - Rows per chunk and an optional wall-clock interval. Bounds retained
       memory and decides how often a partial recording becomes visible.
   * - ``write``
     - ``Overwrite`` (default) or ``Extend`` an existing recording, with schema
       compatibility checked on extend.

Defaults reproduce today's behaviour exactly, so an existing recording keeps
its columns and column names.

Storage
~~~~~~~

Recording writes through ``record_replay::FrameStoreOps`` — the seam RFC 0016
already defines — so the native recorder does not learn about files, object
stores, or Python.

``DataFrameStorage`` is then exposed as a frame store implementation rather
than as a parallel recording stack. The Python adaptor keeps its public surface
(``MemoryDataFrameStorage``, ``FileBasedDataFrameStorage``, subclass hooks,
``set_data_frame_overrides``) and loses only the part that was reimplementing
the recorder.

The flush policy matters here: a store write per flush, not per tick.

Replay
~~~~~~

Replay reads back through the same layout, which is what makes the round trip
verifiable rather than merely plausible. It needs the row grouping the Python
side does today — rows sharing a value time form one tick — plus ``start_time``
/ ``end_time`` / ``as_of`` filtering, and the ``replay_const`` first-group
semantics.

Reading a recording whose options differ from the reader's — a missing as-of
column, renamed partition columns — is resolved by name against the stored
schema, and a genuine mismatch is an error rather than a silent reinterpretation.

Staging
-------

Each step is independently useful and independently verifiable, and the order
is chosen so nothing is rewritten twice:

1. **Row sink.** Parameterise ``emit_partition_rows`` by a sink; ``to_table``
   keeps materialising. No behaviour change; pure refactor with the existing
   tests as the guard.
2. **``TableRecorder`` over a layout.** Arrow schema from the layout, builder
   per column, appending sink. ``FrameRecorder`` re-expressed as the
   no-levels case.
3. **Options.** The structure above, with defaults reproducing current
   behaviour, plumbed through ``record_replay::Config`` per recordable key.
4. **Native ``record`` for partitioned types.** Remove the ``start``-time
   rejection; the ``DATA_FRAME`` model now records ``TSD`` and multi-row
   shapes.
5. **Replay parity.** Grouping, filtering, ``replay_const``.
6. **Adaptor migration.** ``DataFrameStorage`` as a frame store; the Python
   recorder deleted, its public surface unchanged.

Steps 1–2 are where the performance is; steps 3–5 are where the parity is.

Compatibility
-------------

Stored frames keep their current column layout and names, so recordings written
before this RFC replay after it. The Python adaptor's public API does not
change. ``ToTableMode`` semantics are unchanged.

Two things do change and need calling out. A recording becomes visible to a
reader at flush boundaries rather than per tick, which is already true after
the batching fix. And the native path becomes the one that runs for the
``DATA_FRAME`` model on partitioned types, where previously it refused — so a
graph that relied on the refusal to fall through to the Python model must
select the model explicitly.

Alternatives considered
-----------------------

**Extend the table codec to ``Map`` kinds.** The obvious reading of the error
message, and wrong: it would give ``TableConverter`` a second, parallel notion
of partitioning next to ``TsTableLayout``, which already models levels, removed
flags and multi-row leaves correctly. The codec's value-schema orientation is
right for what it does; recording simply needs a different driver.

**Bind ``FrameRecorder`` to Python and append from the adaptor.** Smaller, but
it keeps the per-tick Python boundary and leaves two recorders. It also cannot
help the shapes that matter, since the recorder still could not describe a
partitioned layout.

**Leave the Python recorder in place.** Defensible on measurement alone —
after batching it matches the native path on flat values. Rejected because the
split is the defect: the drift that cost 12.3s and 380MB happened *because*
recording was implemented twice, and nothing prevents a recurrence.

Unresolved questions
--------------------

* **Where per-key options live.** ``record_replay::Config`` is currently one
  global struct with the model and the bitemporal key names. Per-recordable-key
  options need either a map on that config or a separate registry; the latter
  keeps ``Config`` small but adds a lookup.
* **Removal rows under ``Omit``.** Dropping the removed column is
  unambiguous; whether a removal should still emit a row with null values, or
  no row at all, is a semantic choice the current override does not state.
* **Multi-row leaves inside partitions.** ``TsTableLayout`` supports both, but
  the combination — a frame-valued leaf under a ``TSD`` level — has no test
  today. Whether it is supported or explicitly refused should be decided
  before it is implemented.
* **Schema evolution on extend.** Appending to a recording whose stored schema
  differs — an added bundle field — could be refused, or widened with nulls.
  Refusal is the safe default; widening is what users will eventually ask for.

Acceptance criteria
-------------------

* A ``TSD[str, TS[float]]`` records and replays natively under the
  ``DATA_FRAME`` model, round-tripping tick-for-tick, including removals.
* Nested ``TSD`` levels produce one key column per level, in layout order.
* ``as_of`` omitted, tracked, and fixed each produce the stated column layout;
  the same for ``removes``.
* Renamed partition and removed columns appear under the supplied names and
  replay by name.
* Tick, Sample and Snap produce the same rows natively as through
  ``to_table`` today.
* No ``Value`` row is materialised on the recording path — asserted by covering
  the appending sink directly, not inferred from timing.
* Retained Arrow memory tracks payload rather than tick count, at both one row
  per tick and five rows per tick over 20 000 ticks.
* The Python adaptor's existing tests pass unchanged against the migrated
  implementation.
* A recording written by the current Python adaptor replays through the native
  reader.

References
----------

* ``docs/source/developer_guide/record_replay_table.rst`` — the columnar-builder
  ruling (*I5*) this RFC finishes applying.
* RFC 0016 — object-store frame persistence; ``FrameStoreOps`` is the storage
  seam used here.
* ``include/hgraph/lib/std/operators/impl/table_impl.h`` — ``TsTableLayout``.
* ``include/hgraph/types/value/table_codec.h`` — ``TableConverter``,
  ``FrameRecorder``.
* ``python/hgraph/adaptors/data_frame/_data_frame_record_replay.py`` — the
  adaptor whose recording half this RFC removes.
