Persistence package migration
=============================

Durable record and replay — the frame stores, the Arrow record/replay
backend, and the 0.5 ``DataFrameStorage`` surface — moved from core
``hgraph`` to the separately installed ``hgraph-persistence`` distribution
in 0.8. Core keeps the *participation contracts*: the ``record``,
``replay`` and ``compare`` operator markers, record/replay modes,
recordable identity, and the in-memory reference backends. Storage policy
— durable stores, formats, compression, retention, Parquet and S3 — lives
in the extension (:doc:`../rfc/rfc_0025_hgraph_persistence`).

Install it alongside core, either directly or through the extra that
depends on it:

.. code-block:: bash

   pip install hgraph hgraph-persistence
   # or, which pulls hgraph-persistence in as a dependency:
   pip install "hgraph[dataframe]"

Durable names are then reached from the new distribution:

.. code-block:: python

   import hgraph as hg
   from hgraph_persistence import RecordAsOf, RecordRemoves

   @hg.graph
   def record_positions(positions: hg.TSD[str, hg.TS[float]]):
       hg.record(
           positions,
           key="positions",
           recordable_id="book",
           model="hgraph.persistence.frame",
           removes=RecordRemoves.TRACK,
       )

**Nothing needs to move at once.** Released import paths keep working:
``hgraph.adaptors.data_frame`` re-exports the durable surface lazily from
the extension, so ``import hgraph`` never pulls the distribution in and a
core-only install still imports the module. Using a durable name without
the extension installed raises a pointed install error naming
``hgraph-persistence`` — it fails at *use*, not at import.

Python name changes
-------------------

.. list-table::
   :header-rows: 1
   :widths: 48 52

   * - Former core name
     - New persistence name
   * - ``hgraph.RecordAsOf``
     - ``hgraph_persistence.RecordAsOf``
   * - ``hgraph.RecordRemoves``
     - ``hgraph_persistence.RecordRemoves``
   * - ``hgraph.frame_store_contains``
     - ``hgraph_persistence.frame_store_contains``
   * - ``hgraph.frame_store_read``
     - ``hgraph_persistence.frame_store_read``
   * - ``hgraph.adaptors.data_frame.WriteMode``
     - ``hgraph_persistence.compat.WriteMode``
   * - ``hgraph.adaptors.data_frame.DataFrameStorage``
     - ``hgraph_persistence.compat.DataFrameStorage``
   * - ``hgraph.adaptors.data_frame.BaseDataFrameStorage``
     - ``hgraph_persistence.compat.BaseDataFrameStorage``
   * - ``hgraph.adaptors.data_frame.FileBasedDataFrameStorage``
     - ``hgraph_persistence.compat.FileBasedDataFrameStorage``
   * - ``hgraph.adaptors.data_frame.MemoryDataFrameStorage``
     - ``hgraph_persistence.compat.MemoryDataFrameStorage``
   * - ``hgraph.adaptors.data_frame.set_data_frame_record_path``
     - ``hgraph_persistence.compat.set_data_frame_record_path``
   * - ``hgraph.adaptors.data_frame.set_data_frame_overrides``
     - ``hgraph_persistence.compat.set_data_frame_overrides``
   * - ``hgraph.adaptors.data_frame._data_frame_record_replay``
       ``.get_data_frame_record_overrides``
     - ``hgraph_persistence.compat.get_data_frame_record_overrides``

The ``hgraph.adaptors.data_frame`` spellings above continue to resolve; the
table records where the implementation now lives so new code can import it
directly. One name is an exception, and the table gives its real path:
``get_data_frame_record_overrides`` was never re-exported at package level and
is reachable only through the private ``_data_frame_record_replay`` module —
as are ``DATA_FRAME_RECORD_REPLAY_PATH`` and ``DATA_FRAME_RECORD_OVERRIDES``.

None of this survives 1.0. Per :doc:`../rfc/rfc_0005_hgraph_1_0_api`, the
``hgraph.adaptors`` namespace exists only as warning shims in the pre-1.0
bridge release and does not exist in 1.0, so treat every row in this table as
work to do before then rather than a permanent alias.

Backend and model names
~~~~~~~~~~~~~~~~~~~~~~~

Record/replay selects a backend by string id rather than by model constant.
The 0.5 constants are still accepted and translated:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - 0.5 model constant
     - Backend id
   * - ``InMemory``
     - ``"memory"`` — core, the interactive in-memory backend
   * - ``InMemoryDense``
     - ``"testing"`` — core, the cycle-aligned test backend
   * - ``DataFrame``
     - ``"hgraph.persistence.frame"`` — the extension's Arrow backend

Only the third requires ``hgraph-persistence``; the first two are core and
need no extension. ``set_record_replay_model`` remains as an alias for
``set_record_replay_config``.

C++ name changes
----------------

Native applications take the extension's headers, link
``hgraph::persistence``, and register the backend after the core standard
operators:

.. code-block:: cpp

   #include <hgraph/lib/std/std_operators.h>          // register_standard_operators
   #include <hgraph/persistence/recording_store.h>     // register_frame_backend
   #include <hgraph/persistence/frame_store.h>         // store::FrameStore
   #include <hgraph/persistence/recording_options.h>   // RecordAsOf, RecordRemoves

   namespace hgp = hgraph::persistence;

   hgraph::stdlib::register_standard_operators();
   hgp::register_frame_backend();

Header and symbol moves:

.. list-table::
   :header-rows: 1
   :widths: 48 52

   * - Former core name
     - New persistence name
   * - ``hgraph/types/frame_store.h``
     - ``hgraph/persistence/frame_store.h``
   * - ``hgraph::store`` (namespace)
     - ``hgraph::persistence::store`` — the whole store surface moved with the
       header, so ``FrameStore``, ``FrameStoreOps``, ``FrameStoreConfig``,
       ``make_frame_store`` and ``Compression`` all requalify. Changing the
       include alone leaves every former ``hgraph::store::`` reference
       uncompilable.
   * - ``hgraph::stdlib::RecordAsOf``
     - ``hgraph::persistence::RecordAsOf``
   * - ``hgraph::stdlib::RecordRemoves``
     - ``hgraph::persistence::RecordRemoves``
   * - ``HGRAPH_WITH_PARQUET``
     - ``HGRAPH_PERSISTENCE_WITH_PARQUET``
   * - ``HGRAPH_WITH_S3``
     - ``HGRAPH_PERSISTENCE_WITH_S3``

Core keeps **no forwarding stubs** for these: a core-only build must not
name ``FrameStore``, and the C++ constants were replaced outright rather
than aliased. That is deliberate and allowed pre-1.0 — the C++ surface
follows the extension policy's source-compatibility rules, not an ABI
promise. Only the Python spellings keep deprecated aliases.

Build and packaging
-------------------

``HGRAPH_BUILD_PERSISTENCE_EXTENSION=ON`` includes the extension in a
repository build; it is off by default. Standalone consumers find
``hgraph-persistence`` and link ``hgraph::persistence``.

Durable-store *build* policy moved with the code. Core no longer detects
Parquet or S3, no longer exports ``HGRAPH_WITH_PARQUET``/``HGRAPH_WITH_S3``
on its public interface, and no longer emits ``find_dependency(Parquet)``
into its installed CMake package. A downstream project that relied on
inheriting those answers from core must detect them itself or link the
extension, whose own ``HGRAPH_PERSISTENCE_WITH_*`` macros carry them.

Scope
-----

This migration covers durable record/replay only. Core still owns, and
needs no extension for:

* the ``record``, ``replay``, ``replay_const`` and ``compare`` operator
  contracts, record/replay modes, and recordable identity;
* the ``"memory"`` and ``"testing"`` backends, which serve interactive use
  and the testing harness;
* the ``Frame`` value kind, the ``convert``-to-frame operator family, the
  Arrow data sources in ``hgraph.adaptors.data_frame``, and
  ``stdlib::lower``;
* the ``lib/std/operators/table_rows.h`` table seam a durable backend
  consumes; and
* graph state capture and restore mechanics.
