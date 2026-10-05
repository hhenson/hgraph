HGL optional atomic publications
================================

Finite concrete structs with optional fields are complete atomic payloads.
A field declared with a null default may remain unset; zero, an empty string,
and an empty container are present payloads. A struct whose optional fields
are all unset is still a present publication, including equal repetitions.
Only the harness silence marker omits a publication.

Ordinary construction and owning retention preserve each field's presence.
Applying a complete atomic snapshot replaces every field, so an unset field
removes its previous payload. The existing bundle storage and equality rules
preserve this distinction through nested values, replay and recording.

Eval preflight consults the source struct contract for presence requirements.
Those requirements are kept alongside materialized types in the compiler
bridge; the runtime bundle schema describes the shared field layout. Missing
required fields remain errors, while absent optional fields require no value
construction. Registry resets discard and rebuild this source-contract cache.

This admission adds no optional-field read or clear operation and does not
interpret null as a sparse structural delta. The normative contract is
``optional-atomic-publications.md`` in the pinned specification.
