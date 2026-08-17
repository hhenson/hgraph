RFC 0025: The hgraph-persistence Extension
==========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-17
:Target: Core record/replay and checkpoint contracts; a new
         ``hgraph-persistence`` extension owning durable storage and
         recovery policy

Summary
-------

Create a separately built, C++-first ``hgraph-persistence`` extension and
move durable data storage and retrieval, frame-backed record/replay,
checkpoint persistence, journalling, versioning, retention, and recovery
policy into it.

Core hgraph retains the semantic record/replay API and in-memory reference
backends, so core remains useful and fully testable without the extension.
Core also retains the runtime mechanics required to capture and restore
graph state safely — and nothing else of persistence.

This is an **extraction and dependency-boundary change, not a second
implementation**.  At the end of the migration:

.. code-block:: text

    application
        |
        +--> hgraph core
        |      - graph/runtime/type/operator systems
        |      - record/replay/compare operator contracts
        |      - component recordable identity and modes
        |      - dense + sparse in-memory implementations
        |      - exact state capture/import and restore lifecycle
        |
        +--> hgraph-persistence
               - durable record/replay overloads
               - FrameStore and memory/local/S3 store strategies
               - Arrow IPC/Parquet persistence
               - segments, versions, journals and manifests
               - checkpoint publication, recovery and retention policy
               - Python durable-storage compatibility surface

The tracking issue is `#498 <https://github.com/hhenson/hgraph/issues/498>`_;
its checkpoint plan is normative for sequencing and is restated under
`Implementation plan`_.

Motivation
----------

Durable persistence grew inside core because its first consumers did
(RFC 0016 deliberately placed ``FrameStore`` and the Arrow-backed stores in
core; RFC 0019 built native table recording on top; RFC 0023 assigned core
the durable checkpoint-store contract).  The consequences now outweigh the
convenience:

* core's public surface carries S3 credentials, Parquet formats, retention
  vocabulary, and recording-segment protocols that most graphs never use;
* the closed backend vocabulary (``InMemory`` / ``InMemoryDense`` /
  ``DataFrame``) hard-codes one durable implementation into core headers,
  where an open ecosystem needs extensions to register their own;
* generic table conversion reads record/replay configuration, coupling two
  independent facilities;
* the kafka and web extensions established the working pattern — a
  separately versioned wheel and CMake package over the installed SDK,
  registering overloads of core operator contracts — and persistence is the
  remaining large facility that predates it.

Ownership boundary
------------------

Core keeps
~~~~~~~~~~

* ``record``, ``replay``, ``replay_const``, and ``compare`` operator
  markers, modes, and component boundary composition;
* stable recordable/component ids and fully-qualified key construction;
* the **dense** cycle-aligned in-memory implementation used by
  ``eval_node`` and graph tests, and the **sparse** time-indexed in-memory
  implementation used by ordinary in-memory component record/replay;
* an in-memory comparison implementation and the core-neutral comparison
  summary publication (below);
* ``RecordableState`` as runtime semantic state;
* graph execution, scheduling, dynamic topology, REF ownership, and the
  restore mechanics required to participate safely in state capture and
  restore: exact TS state capture, validation and quiet import; node
  semantic-state capture/import and derived-state rebuild; root-cycle
  consistency cuts; dynamic child/slot reconstruction; scheduler state and
  internal REF restoration; fresh-versus-restored start mode; gated
  ingress during restore; and boundary source/sink participation
  contracts.  Core may own a versioned in-memory graph state image
  required for exact restoration.

The test harness must eventually use explicitly testing-owned
source/capture nodes rather than depending accidentally on a durable
persistence implementation, but it remains available in a core-only build.

hgraph-persistence owns
~~~~~~~~~~~~~~~~~~~~~~~

* the complete ``FrameStore`` surface: the owning erased handle and
  operations table, key validation, memory/local-filesystem/S3 locations,
  credentials, Arrow IPC and Parquet formats, compression policy, backend
  construction, S3 lifecycle, and Python store adaptation;
* frame-backed ``record`` / ``replay`` / ``replay_const`` and durable
  ``compare`` overloads, recording projection, segmentation,
  immutable-key and partitioned/frame-valued recording policy;
* checkpoint/object stores, durable encodings surrounding the core state
  image, manifests and checksums, input journals and run lineage,
  conditional publication, recording/checkpoint versions, recovery
  selection, compaction, retention and garbage collection, and concrete
  external cursor/acknowledgement/transaction policy;
* the Python durable-storage compatibility surface.

Core must not own filesystem/S3 persistence or retention policy; the
extension must not contain an independent graph runtime or a Python
implementation of native semantics.

This reverses recorded ownership statements in RFCs 0016, 0019, 0021 and
0023; each carries an ownership-revision note referring here, and the
downstream :doc:`../developer_guide/extension_policy` records the general
principle: **core owns runtime participation contracts; persistence
implementations and storage policy live in extensions**.

C++ and Python contract
-----------------------

Open backend selection
~~~~~~~~~~~~~~~~~~~~~~

The closed model vocabulary is replaced by a minimal core configuration:

.. code-block:: cpp

    struct RecordReplayConfig
    {
        std::string backend{"memory"};
    };

Core recognises exactly its own backend identifiers:

* ``"memory"`` — the sparse, time-indexed reference implementation;
* ``"testing"`` — the dense, cycle-aligned harness implementation.

``hgraph-persistence`` defines ``"hgraph.persistence.frame"`` and
registers overloads of the existing core operator markers.  Local
filesystem versus S3 is *configuration of that durable backend*, not a
distinct operator overload.

A per-call ``backend`` scalar continues to override the graph default.
**Every overload guard resolves through the same effective-backend
function** (the existing ``call_model`` contract, renamed to effective
backend selection), which is what keeps overloads mutually exclusive: if
one guard consulted a local override and another did not, a call
supplying one would match both overloads or neither.

Core must not define ``DATA_FRAME``, local, S3, Parquet, or other
extension-owned identifiers.  Selecting a backend that no installed
implementation recognises is an explicit wiring-time error naming the
backend and, when the identifier is namespaced
(``hgraph.persistence.*``), the distribution expected to provide it.

Activation: selecting the backend is the load point
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Installing a wheel executes no code, so "durable overloads arrive by
installing the extension" needs a defined load point.  The contract:

* **Native registration entry point.**  ``_hgraph_persistence`` exposes
  and calls on import an idempotent
  ``hgraph::persistence::register_frame_backend()`` that registers the
  durable operator overloads and claims the
  ``"hgraph.persistence.frame"`` identifier.  Registration goes through
  the same installer mechanism core's standard-operator setup uses, so a
  registry reset-and-rebuild (the testing path through
  ``record_replay::reset()``) replays extension registration exactly as
  it replays core's — the extension does not strand on reset.
* **Python load trigger.**  Durable behaviour is only ever *requested*
  through backend selection.  The single backend-normalisation choke
  point in ``hgraph._wiring._state`` (graph default and per-call
  override alike), on seeing a ``hgraph.persistence.*`` identifier,
  lazily imports ``hgraph_persistence`` — importing the native module
  runs the registration above — and raises the pointed install error if
  the import fails.  An explicit ``import hgraph_persistence`` activates
  identically and remains supported.

User imports of the operator surface (``hgraph.record`` et al.) are
therefore genuinely unchanged: activation rides the configuration change
users already make to choose a durable backend, never a new import.

Extension configuration is separate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Core ``RecordReplayConfig`` contains only cross-implementation selection
required by core wiring.  The persistence extension owns its own
configuration:

.. code-block:: cpp

    struct FrameRecordingConfig
    {
        std::string date_key{"__date_time__"};
        std::string as_of_key{"__as_of__"};
        std::optional<DateTime> as_of{};
        FrameStore store{};
    };

Per-recording policies remain explicit wiring-time arguments on the
persistence overloads: as-of tracking, removal tracking, partition and
removal column names, frame prefix, Tick/Sample/Snap mode, and row/time
flush thresholds.

Generic core table operators (``to_table`` / ``from_table``) must not
read persistence configuration; they receive their own explicit table
options and defaults.  The bitemporal vocabulary — the date/as-of column
keys and the optional fixed as-of override — is **generic table
configuration and stays core**, owned by those options: converting a
time series to bitemporal rows requires an as-of stamp whether or not
anything durable consumes the rows.  ``FrameRecordingConfig`` above
carries the extension's *own* copies of these fields (sharing the
defaults is a vocabulary convenience, not a read of core table state).

Core-neutral comparison summaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The detailed comparison representation is implementation-specific (the
memory backend may compare directly; the persistence extension may store
full comparison rows in Arrow frames).  The *summary* is not.  Core
provides:

.. code-block:: cpp

    struct ComparisonSummary
    {
        std::size_t compared{0};
        std::size_t mismatches{0};
    };

    void publish_comparison_summary(
        GlobalStateView, std::string_view fq_key, ComparisonSummary);

    std::optional<ComparisonSummary> comparison_summary(
        GlobalStateView, std::string_view fq_key);

The default implementation stores this small value under a private
core-owned ``GlobalState`` key.  Core's memory compare publishes it
directly; durable implementations publish the same summary at completion
while retaining any detailed result in their own store.  The public query
therefore has no dependency on ``Frame``, Arrow, ``FrameStore``, or any
specific backend.  (The current core query decodes an Arrow frame and
throws when absent; the replacement is total and neutral — a recorded
compatibility change.)

Checkpoint/restore split
~~~~~~~~~~~~~~~~~~~~~~~~

Core exposes the mechanics (``TSCheckpointOps``, ``NodeCheckpointOps``,
the executor root-cycle checkpoint hook, the owned immutable state image,
``Fresh``/``Restore`` start context, gated ingress); the extension owns
every durable consequence (stores, manifests, journals, fencing,
lineage-based recovery selection, incremental images, compaction,
retention, GC).  The detailed mechanics remain specified by RFC 0023,
re-read under this RFC's ownership boundary.

Packaging, namespaces, and identifiers
--------------------------------------

Decisions (checkpoint 1 of the tracking issue):

* repository location ``extensions/persistence``, following the kafka/web
  template (own CMake package over the installed SDK, own wheel, CI
  wiring, distribution audit);
* CMake package ``hgraph-persistence``; exported target
  ``hgraph::persistence``; root option
  ``HGRAPH_BUILD_PERSISTENCE_EXTENSION``;
* wheel ``hgraph-persistence`` (cp312-abi3), versioned with core; Python
  package ``hgraph_persistence`` with stable-ABI native module
  ``_hgraph_persistence``;
* C++ namespace ``hgraph::persistence`` (stores under
  ``hgraph::persistence::store``);
* backend identifier ``"hgraph.persistence.frame"``; core identifiers
  ``"memory"`` and ``"testing"``.

Compatibility and migration
---------------------------

* **Backend names.** The legacy model constants map
  ``InMemory → "memory"``, ``InMemoryDense → "testing"``,
  ``DataFrame → "hgraph.persistence.frame"``.  The Python bridge accepts
  its released vocabulary and translates during a deprecation window; the
  C++ constants are replaced outright at checkpoint 2 (pre-1.0, and the
  C++ surface follows the extension policy's source-compatibility rules,
  not an ABI promise).
* **C++ headers.** ``hgraph/types/frame_store.h`` moves to the extension
  (``hgraph/persistence/frame_store.h``); core does not keep a forwarding
  stub — a core-only build must not name ``FrameStore``.
  ``hgraph/types/record_replay.h`` remains core but sheds the store
  registration, segmented-recording protocol, and durable identifiers.
* **Python imports.** ``hgraph.adaptors.data_frame`` keeps its released
  import paths: the durable record/replay surface becomes a guarded lazy
  re-export of ``hgraph_persistence`` (the ``hgraph.adaptors.tornado`` →
  ``hgraph_web.compat`` precedent), raising a pointed install error only
  when a durable name is used without the extension installed, and the
  relevant extra installs ``hgraph-persistence``.
* **Symbol dispositions.** Every public C++ and Python
  record/replay/storage symbol is classified core / extension /
  compatibility shim / removal in `Appendix: symbol and migration
  inventory`_ — the agreed migration table the checkpoints execute
  against.
* **Registry behaviour.** Installed extensions register their overloads
  against the core operator markers; registry reset (test isolation)
  must define re-registration behaviour for installed extensions —
  decided at checkpoint 3 and recorded here when implemented.

Performance and memory
----------------------

The extraction itself must not change hot-path behaviour: overload
resolution continues through the same wiring-time guard, and the memory
backends are moved-by-classification, not rewritten.  Checkpoint work
(RFC 0023 mechanics) is confined to declared boundary inputs and
requested cold-path capture; ordinary evaluation stays free of per-node
checkpoint branches.  The acceptance criteria include a no-regression
gate on the repository's performance suites.

Alternatives considered
-----------------------

* **Leave persistence in core** — rejected: the motivation section's
  coupling costs are already realised, and RFC 0023's durable half would
  grow them substantially.
* **A second implementation in the extension** (leaving core's durable
  code as a deprecated twin) — rejected outright: one implementation,
  moved, with compatibility shims only at import boundaries.
* **Moving the ``Frame`` scalar and the generic Arrow/value/table codecs
  along with the stores** — explicitly a non-goal: ``Frame`` is a core
  value type with consumers unrelated to persistence; a larger codec
  extraction needs its own evidence and RFC.
* **A plugin API instead of operator overloads** — rejected: the operator
  registry *is* the extension seam (RFC 0019's table-ops precedent);
  inventing a parallel plugin registry would violate the one-runtime-model
  principle.

Implementation plan
-------------------

Issue #498's eight checkpoints are the plan of record, each independently
reviewable and behaviour-preserving unless it documents a compatibility
transition:

1. this RFC, ownership revisions to RFCs 0016/0019/0021/0023, the
   extension-policy update, and the symbol inventory (no behaviour
   change);
2. minimal ``RecordReplayConfig``, effective-backend rename, explicit
   table options for generic table operators, and the core-neutral
   comparison summary;
3. independently consumable extension seams: public table
   layout/traversal header, extension overload registration, registry
   reset semantics, split Python bindings, and an installed-SDK probe
   backend;
4. scaffold ``hgraph-persistence`` and move ``FrameStore`` and all
   backends;
5. move the durable record/replay overloads and their tests; explicit
   missing-extension diagnostics;
6. core checkpoint/restore mechanics (RFC 0023's core half);
7. durable checkpoints and recovery in the extension (RFC 0023's durable
   half);
8. compatibility cleanup and coordinated minimum versions.

Acceptance criteria
-------------------

* A core-only C++ and Python installation supports record/replay/compare
  using the memory and testing backends, and core's complete native and
  Python compatibility suites pass without ``hgraph-persistence``.
* ``hgraph-persistence`` supplies durable overloads for the same core
  operator contracts; backend selection is open and mutually exclusive;
  core contains no extension backend vocabulary.
* Generic table conversion has no dependency on persistence
  configuration.
* ``comparison_summary`` returns the same core-neutral result for memory
  and durable implementations.
* Core contains no filesystem, S3, Parquet, recording-segment, retention
  or durable-publication policy; the extension contains no independent
  graph runtime or Python implementation of native semantics.
* Separately built C++ and Python installed-SDK consumers exercise
  registration, record/replay, storage and reset behaviour.
* Hot-path performance does not regress; checkpoint work is confined to
  declared boundary inputs and requested cold-path capture.
* Core changes pass the complete native suite and the Python 3.14
  non-WIP compatibility suite; runtime/lifecycle work also passes the
  Linux acceptance environment with ASan where ownership is involved.
  GitHub CI is not a substitute for the local acceptance gates.

Implementation status
---------------------

Checkpoint 1 (this RFC, the ownership revisions, and the symbol
inventory) landed as documentation only; no runtime behaviour changed.

Checkpoint 2 is implemented on the extraction branch: core
``RecordReplayConfig{backend}`` with the ``normalize_backend`` choke
point and ``effective_backend`` resolution; the generic table options
extracted to ``table::TableConfig`` (``types/table_config.h``) and read
by the generic table operators, the transitional frame backend, and the
data-frame adaptor's raw replay; the core-neutral ``ComparisonSummary``
published by both compare implementations (memory per tick before its
failing throw, frame at stop beside its detailed rows) with the total
``optional`` query.  Public Python behaviour is unchanged: the module
attributes carry the new backend ids, legacy names translate at every
entry point, and the Python summary query keeps its raise-on-absent
contract.

Appendix: symbol and migration inventory
----------------------------------------

Classification key: **core** (stays, possibly renamed at the recorded
checkpoint), **extension** (moves to ``hgraph-persistence``), **shim**
(a compatibility re-export retained during the deprecation window),
**removal** (deleted at the recorded checkpoint, with its replacement
named).

C++ — ``hgraph/types/record_replay.h`` (namespace
``hgraph::record_replay``):

.. list-table::
   :header-rows: 1
   :widths: 44 12 12 32

   * - Symbol (current location)
     - Class
     - Checkpoint
     - Disposition
   * - ``Mode``, ``has_mode``, mode operators
     - core
     - —
     - Unchanged; the operator-marker mode set.
   * - ``ScopeState``, ``current_scope``, ``scope``
     - core
     - —
     - Unchanged; wiring-time mode scope.
   * - ``RECORDABLE_ID_TRAIT``, ``has_recordable_id``,
       ``fq_recordable_id`` (both forms)
     - core
     - —
     - Unchanged; recordable identity.
   * - ``Config`` (model/date_key/as_of_key/as_of)
     - removal
     - 2
     - Replaced by ``RecordReplayConfig{backend}``; table/as-of keys move
       to the extension's ``FrameRecordingConfig``.
   * - ``IN_MEMORY`` (``"InMemory"``)
     - removal
     - 2
     - Becomes backend id ``"memory"``; Python bridge translates the
       legacy name during deprecation.
   * - ``IN_MEMORY_DENSE`` (``"InMemoryDense"``)
     - removal
     - 2
     - Becomes backend id ``"testing"``; same translation.
   * - ``DATA_FRAME`` (``"DataFrame"``)
     - removal
     - 2/5
     - Extension-owned vocabulary; becomes
       ``"hgraph.persistence.frame"`` defined by the extension.
   * - ``set_config`` / ``config`` / ``model_is``
     - core
     - 2
     - Re-shaped over ``RecordReplayConfig``; ``model_is`` renamed to a
       backend guard.
   * - ``call_model`` / ``call_model_is``
     - core
     - 2
     - Renamed to effective-backend selection; remains the ONE resolution
       function every overload guard uses.
   * - ``set_frame_store`` / ``frame_store`` /
       ``clear_frame_store`` (process and state scoped)
     - extension
     - 4
     - Move with ``FrameStore``; the process-global fallback store is
       removed outright (state-scoped configuration only).
   * - ``store_write`` / ``store_read`` / ``store_contains``
     - extension
     - 4
     - Convenience wrappers move with the store.
   * - ``segmented_recording_marker`` / ``..._manifest`` /
       ``is_segmented_recording`` / ``segment_key`` /
       ``completion_key``
     - extension
     - 5
     - RFC 0019's segmented-recording protocol moves with the durable
       recorder.
   * - ``reset``
     - core
     - 3
     - Stays (scope reset); its store-reset half moves; extension
       re-registration semantics defined at checkpoint 3.
   * - ``replay_const_value``
     - core
     - 2/5
     - Core contract; memory implementation stays, durable
       implementation moves behind backend dispatch.
   * - ``recorded_seed_resolver``
     - core
     - 2/5
     - Core contract (``component`` RECOVER seed); dispatches per
       effective backend.
   * - ``ComparisonSummary`` / ``comparison_summary``
     - core
     - 2
     - Re-based on the core-neutral ``GlobalState`` publication; gains
       ``publish_comparison_summary``; the query becomes total
       (``optional``) and Arrow-free.

C++ — ``hgraph/types/frame_store.h`` (namespace ``hgraph::store``): every
symbol — ``Format``, ``Compression``, ``MemoryLocation``,
``LocalLocation``, ``Credentials``, ``S3Location``, ``Location``,
``FrameStoreConfig``, ``FrameStoreOps``, ``validate_key``,
``require_valid_key``, ``finalize_s3``, ``parquet_available``,
``FrameStore``, ``make_frame_store`` — classifies **extension**
(checkpoint 4), relocating to ``hgraph/persistence/frame_store.h`` under
``hgraph::persistence::store``.  No core forwarding header remains: a
core-only build must not name ``FrameStore``.

C++ — operator implementations and seams:

.. list-table::
   :header-rows: 1
   :widths: 44 12 12 32

   * - Symbol (current location)
     - Class
     - Checkpoint
     - Disposition
   * - ``lib/std/component.h`` (``component<G>``,
       ``recovering_pass_through``, key/wrap helpers)
     - core
     - —
     - Unchanged; composes the operator markers and mode scope only.
   * - ``impl/record_replay_memory_impl.h``
       (``dense_record_impl``, ``sparse_record_impl``, ``replay_impl``,
       memory compare)
     - core
     - 2
     - Stays; guards re-express over backend ids ``"memory"`` /
       ``"testing"``.
   * - ``impl/record_replay_frame_impl.h`` (frame-backed
       record/replay/compare, recording options fold)
     - extension
     - 5
     - Moves wholesale; registers its overloads from the extension.
   * - ``impl/table_impl.h`` — ``table_ts_detail`` layout/traversal
       (``recording_columns`` et al., ``TableRecordingOptions``)
     - core
     - 3
     - Promoted from the ``impl`` header to a supported public core
       header so the extension consumes it without private includes.
   * - ``impl/table_impl.h`` — reads of ``record_replay::config`` inside
       generic ``to_table`` / ``from_table``
     - removal
     - 2
     - Generic table operators receive their own explicit table
       options/defaults.
   * - ``register_standard_operators()`` — frame record/replay
       registration lines
     - removal
     - 5
     - Durable overloads register from ``hgraph-persistence``; core
       registers only memory/testing.

C++ — testing harness (``lib/testing/record_replay.h``,
``record_replay_buffer.h``, the dense buffer/seed API): **core** — the
harness data layer stays in a core-only build (checkpoint 3 renames it
explicitly testing-owned).  ``standard_scalar_bindings.cpp``'s
``ReplayCursorState`` binding follows the memory backend: core.  The
``extensions/analytics`` test that includes the memory ``impl`` header
directly is re-pointed at the public seam at checkpoint 3.

Python bindings — ``python/py_state_services.cpp`` (split at
checkpoint 3 into core state/services and extension persistence units):

.. list-table::
   :header-rows: 1
   :widths: 44 12 12 32

   * - Binding (python-visible name)
     - Class
     - Checkpoint
     - Disposition
   * - ``MODE_*`` constants, ``RecordReplayScope``,
       ``record_replay_scope``, ``current_record_replay_mode``
     - core
     - —
     - Mode scope bindings; unchanged.
   * - ``_set_record_replay_config``
     - core
     - 2
     - Re-shaped over ``RecordReplayConfig{backend}``.
   * - ``_set_as_of``, ``_set_table_schema_date_key`` /
       ``_set_table_schema_as_of_key`` / ``_table_schema_keys``
     - core
     - 2
     - Re-pointed at the generic table operators' OWN options (core
       state: bitemporal keys + fixed as-of); they no longer write
       record/replay configuration.  The durable recorder keeps its own
       ``FrameRecordingConfig`` copies (checkpoint 5).
   * - ``IN_MEMORY`` / ``IN_MEMORY_DENSE`` module attrs
     - shim
     - 2→8
     - Bridge translation to ``"memory"`` / ``"testing"`` during the
       deprecation window; removed at checkpoint 8.
   * - ``DATA_FRAME`` module attr
     - shim
     - 5→8
     - Translates to ``"hgraph.persistence.frame"``; selecting it
       without the extension raises the pointed install error.
   * - ``_comparison_summary``
     - core
     - 2
     - Re-based on the core-neutral summary (total, Arrow-free).
   * - ``_GlobalState._set_memory_recording_entry``
     - core
     - —
     - Memory-backend seeding; unchanged.
   * - ``_frame_store_contains`` / ``_frame_store_read`` /
       ``_set_python_frame_store`` / ``_restore_python_frame_store``
     - extension
     - 4
     - Move to ``_hgraph_persistence`` with the store; core keeps no
       frame-store bindings.
   * - ``_set_record_as_of_enum`` / ``_set_record_removes_enum``
     - extension
     - 5
     - Recording-option enum slots move with the durable recorder.
   * - ``_set_to_table_mode_enum``, ``_set_cmp_result_enum``
     - core
     - —
     - Generic table / compare enum slots; unchanged.
   * - ``RecordableStateView``, ``Node.recordable_state``
     - core
     - —
     - Runtime semantic state; unchanged.
   * - ``ArrowStream``, ``ArrowSeriesArray``
     - core
     - —
     - ``Frame`` value transport; ``Frame`` itself is a non-goal.

Python package surface:

.. list-table::
   :header-rows: 1
   :widths: 44 12 12 32

   * - Name (current location)
     - Class
     - Checkpoint
     - Disposition
   * - ``hgraph.record`` / ``replay`` / ``replay_const`` / ``compare``
       (lazy operator exports)
     - core
     - —
     - Operator contracts; durable overloads arrive by installing the
       extension, with no import-path change — activated at backend
       selection per *Activation: selecting the backend is the load
       point*.
   * - ``RecordReplayEnum``, ``RecordReplayContext``,
       ``record_replay_scope``, ``component``, ``RECORDABLE_STATE``
     - core
     - —
     - Unchanged.
   * - ``set_record_replay_model`` / ``set_record_replay_config`` /
       ``set_as_of`` (``hgraph._wiring._state``)
     - core
     - 2
     - Accept released vocabulary, translating legacy model names to
       backend ids during deprecation; ``set_as_of`` writes the generic
       table options (core state) — the durable recorder reads its own
       configuration.
   * - ``IN_MEMORY`` / ``IN_MEMORY_DENSE`` / ``DATA_FRAME``
       (``hgraph.__all__``)
     - shim
     - 2→8
     - Deprecated aliases of the backend ids.
   * - ``frame_store_contains`` / ``frame_store_read``
       (``hgraph.__all__``)
     - shim
     - 4
     - Lazy re-exports of the extension; pointed install error without
       ``hgraph-persistence``.
   * - ``get_recorded_value``, ``get/set_recorder_api``,
       ``get/set_recording_label``
     - core
     - 3
     - Testing-harness surface; stays core-only.
   * - ``RecordAsOf`` / ``RecordRemoves`` (``hgraph._compat``)
     - shim
     - 5
     - Recording options owned by the extension; core keeps deprecated
       aliases through the window.
   * - ``ToTableMode``, ``TableSchema``, ``make_table_schema``,
       ``table_schema``, ``TABLE``, table date/as-of key accessors
     - core
     - 2
     - Generic table surface; re-based on the table operators' own
       options.
   * - ``hgraph.adaptors.data_frame._data_frame_record_replay`` — all
       nine ``__all__`` names (``DATA_FRAME_RECORD_REPLAY``,
       ``set_data_frame_record_path``, ``set_data_frame_overrides``,
       ``replay_data_frame``, ``WriteMode``, ``DataFrameStorage``,
       ``BaseDataFrameStorage``, ``FileBasedDataFrameStorage``,
       ``MemoryDataFrameStorage``)
     - shim
     - 4/5
     - The module becomes a guarded lazy re-export of
       ``hgraph_persistence`` (storage classes at checkpoint 4, operator
       surface at checkpoint 5), preserving import paths.
   * - ``hgraph._wiring._core`` lazy import of
       ``_legacy_record_replay_kwargs`` (the core→adaptor back-edge,
       triggered when the model is ``DATA_FRAME``)
     - removal
     - 5
     - The extension's own overloads carry their argument adaptation;
       core wiring must not import adaptor modules.

Tests (moved with what they test; every remaining core test passes in a
core-only build):

* stay core — ``test_record_replay.cpp``, ``test_record_replay_config.cpp``,
  ``test_component.cpp``, ``test_const_eval.cpp``, ``test_table.cpp``,
  ``test_table_recorder.cpp``, ``test_table_round_trip.cpp``,
  ``python/tests/test_global_state_wiring.py``, harness/inventory suites;
* move at checkpoint 4 — ``test_frame_store.cpp``,
  ``python/tests/test_python_frame_store.py``, and the frame-store half of
  ``tests/install_consumer`` (an equivalent installed-SDK consumer moves
  to the extension; core's consumer drops ``FrameStore``);
* move at checkpoint 5 — ``test_record_replay_frame.cpp``,
  ``test_record_replay_partitioned.cpp``, ``test_recording_columns.cpp``,
  ``test_data_frame_operators.cpp`` (its recording halves),
  ``python/tests/test_native_table_recording.py``,
  ``test_data_frame_replay_window.py``, and the
  ``adaptors/data_frame`` + ported data-frame record/replay suites.

Private ``GlobalState`` and metadata keys:

* core-private — ``__hgraph.record_replay.config__``, the ``:memory:``
  recording-buffer prefix, ``__record_replay_model__`` (bridge mirror;
  renamed with the config re-shape), ``__recorder_api__`` /
  ``__recorder_api__label__`` (testing harness), the new comparison
  summary key introduced at checkpoint 2;
* extension-bound — ``__hgraph.record_replay.frame_store__``,
  ``__hgraph.python.frame_store_stack__``, the ``:data_frame:`` key
  family, the ``hgraph.recording.layout`` / ``.version`` / ``.segments``
  Arrow-metadata keys and the ``__hgraph_recording_marker__`` field, and
  the ``<fq>.__compare__`` store key (superseded by the neutral summary
  for the public query; the extension may keep detailed rows under its
  own keys);
* shared vocabulary that stays core — the ``__date_time__`` /
  ``__as_of__`` column-name defaults (they belong to the generic table
  codec; the extension's ``FrameRecordingConfig`` carries its own copy).

Symbols established above do not change classification without amending
this RFC.

References
----------

* Issue #498 — the agreed architectural decisions and checkpoint plan.
* :doc:`rfc_0000` — process and required structure.
* :doc:`rfc_0016_object_store_frame_persistence` — the store contract
  this RFC relocates (ownership revised).
* :doc:`rfc_0019_native_table_recording` — native table recording
  (recording policy relocates; the table seam becomes a public core
  header).
* :doc:`rfc_0021_recording_versions` — recording versions (extension
  scope under this RFC).
* :doc:`rfc_0022_serializable_graph_manifest` — the manifest contract
  restore verification builds on.
* :doc:`rfc_0023_graph_checkpoint_recovery` — checkpoint/recovery
  mechanics (core half) and durable policy (extension half; ownership
  revised).
* :doc:`rfc_0015_kafka_extension_api` and
  :doc:`rfc_0024_web_extension_api` — the extension packaging template.
* :doc:`../developer_guide/extension_policy` — the core-versus-extension
  ownership principle this RFC instantiates.
