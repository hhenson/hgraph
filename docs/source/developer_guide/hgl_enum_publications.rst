HGL enum publications
=====================

Declared enums are nominal scalar leaves in the publication profile specified
by ``external/hgraph_spec/language/docs/design/enum-publications.md``. Their
identity is the defining module and declaration, and each member retains its
assigned signed 64-bit number. Neither an integer nor another enum substitutes
for that identity. Scalar ``atomic`` and ``delta`` normalization applies to the
enum itself and recursively inside admitted finite shapes.

The syntax tree owns enum declarations and member expressions. Resolution binds
qualified member references to that declaration; typed IR checks numbering,
duplicates and representability before backend lowering. HGraph IR carries a
layout-independent enum contract and typed member constants. The direct backend
uses the core's existing named enum metadata and ordinary owning Value plans.
The generated backend publishes a distinct C++ schema marker per declaration
and binds it to the same named enum contract. Its member values use owning
``Value`` storage with the core enum plan; no C++ enum object is aliased as an
integer. Generated hooks use prepared plans without registry lookups. Runtime publications use existing
value and delta operations, with no conversion to an integer schema.

This implementation stage covers declared members, automatic numbering,
qualified lookup, exact identity, defaults, ordinary captures, generic
pass-through, replay and record. Source-module enum export/import, native enum
imports, checked construction from
numbers or names, enum enumeration helpers and enum switch dispatch remain
separate compiler capabilities. Their absence is not permission to erase enum
identity or introduce unchecked member construction into publications.

Shared enum publication fixtures cover signed endpoints, equal ticks, silence,
empty horizons, recursive structural and atomic payloads, defaults and retention.
Native compiler fixtures cover generated C++ wiring; negative frontend tests
cover invalid numbering, duplicate names/numbers and wrong nominal types.
