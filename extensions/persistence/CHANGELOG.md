# Changelog

## Unreleased

- Persistence wheels now publish a shared native SDK carrying the pinned curl
  implementation. Downstream extension wheels can consume S3 persistence
  without relinking against an older or ABI-incompatible system CURL/TLS SDK.

- Added the reusable C++ `ObjectStore` contract required by RFC 0023 and RFC
  0026: atomic immutable objects, typed absence and backend failures, ordered
  prefix paging, and conditional references across memory, local, and S3
  strategies. Immutable local/S3 `FrameStore` publication now uses the same
  atomic backend path.

- Extracted from hgraph core (RFC 0025, checkpoint 4): FrameStore and
  all store backends, the frame-backed record/replay/compare overloads,
  the segmented-recording protocol, the durable replay_const read, and
  the DataFrameStorage Python compatibility surface.
- Checkpoint 5: registered the durable overloads from the extension's own
  keyed installer; added the missing-extension diagnostics; the `dataframe`
  extra now installs this distribution; the 0.5 override-registry
  translation moved here behind core's wiring-adapter seam.
- Checkpoint 5 completion (audit 2026-08-19):
  - `RecordAsOf` / `RecordRemoves` are now extension-owned. The C++ enums
    live in `hgraph/persistence/recording_options.h`; the Python spellings
    are exported from `hgraph_persistence` and associated with the native
    scalars through the keyed installer. `hgraph.RecordAsOf` /
    `hgraph.RecordRemoves` remain deprecated aliases until checkpoint 8.
  - The `:data_frame:` recording path and override registry
    (`set_data_frame_record_path`, `set_data_frame_overrides`,
    `get_data_frame_record_overrides`) moved here from core, beside the
    translation that reads them. `hgraph.adaptors.data_frame` re-exports
    them lazily, so released import sites are unchanged.
  - Parquet and S3 detection, linkage and feature macros are this
    extension's alone: it no longer inherits a parent build's answers, and
    its macros are `HGRAPH_PERSISTENCE_WITH_PARQUET` / `_WITH_S3`.
