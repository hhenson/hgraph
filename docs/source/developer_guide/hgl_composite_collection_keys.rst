HGL finite composite collection keys
====================================

Complete positional tuples and concrete struct values can be set members and
map keys. Every component must recursively admit equality and hashing under the
collection-key profile. Equality includes tuple positions, exact nominal types,
optional presence and every present field. Hashing uses those same values;
ordering is unnecessary. Nested NaN values are rejected before publication.

Sparse recipes retain each key once in written order. Immutable aliases of
constant tuple or struct recipes reuse the retained value, including provider
results. Mutable aliases and temporal inputs do not become constant keys.
Duplicate keys and added/removed overlap fail before graph startup. Existing
canonical transition requirements remain: additions address absent members and
removals address present members. Redundant additions are not normalized by the
harness.

Ordinary atomic set/map snapshots own their complete keys and values. Mutating
a source binding cannot change membership or a previously captured delta.
Collection key plans use canonical owning runtime storage, including nested
tuple and struct components. This keeps hashed lookup and equality consistent
between generated ordinary recipes and native runtime publications.
Collections inside keys, recursive keys, abstract families, opaque native keys
and references remain outside this admission. The normative contract is
``composite-collection-keys.md`` in the pinned specification.

The local CTest matrix includes all shared ``hgraph.std`` test parts, including
older scalar, structural, atomic and replay fixtures. Native-module tests use
the existing native source inventory. The parity discovery tool independently
checks the complete shared corpus for missing test results.
