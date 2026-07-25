RFC 0001: Typed Frame Metadata
==============================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-07-23
:Target: Next ``hg_cpp`` minor release

Summary
-------

Extend ``Frame[Rows]`` to ``Frame[Rows, Metadata]`` while retaining one Arrow
table as the complete value and interchange representation. Frame-level
metadata is encoded field by field in the Arrow schema metadata; it is not a
second side object and is not repeated as table rows or columns.

Motivation
----------

Versioned tabular datasets need identity, immutable revision, as-of time,
provenance, and schema version to apply to a complete table. Repeating those
values on each row wastes space and permits row-level disagreement. Keeping
them outside Arrow loses information at the C++/Python and persistence
boundaries.

Type contract
-------------

The public forms are:

.. code-block:: text

   C++:    FrameOf<RowSchema, MetadataSchema>
   Python: Frame[RowSchema, MetadataSchema]

The existing one-argument form remains metadata-free. ``RowSchema`` describes
the Arrow columns. ``MetadataSchema`` is a named Bundle/CompoundScalar schema
describing one immutable logical value for the whole frame.

Arrow representation
--------------------

Reserved byte-string entries in ``arrow::Schema::metadata`` are:

``hgraph.metadata.schema``
   Optional qualified Bundle name. It supports discovery and fast validation,
   but is not required for conformance.

``hgraph.metadata.version``
   Field-encoding version.

``hgraph.metadata.field.<name>``
   One entry for each populated metadata field.

Strings, integers, floating-point values, booleans, enums, dates, datetimes,
times, and durations use their deterministic UTF-8 field representation.
Nested Bundles, tuples, lists, sets, and maps use the schema-directed JSON
codec. Opaque Python objects, callables, and values without a deterministic
codec are rejected. Unrelated Arrow metadata is preserved.

The declared ``MetadataSchema`` is authoritative. When the optional type marker
is present it must match. When absent, successful schema-directed decoding of
the field entries establishes conformance. Reflective decoding may use a
registered marker; markerless decoding requires the caller or typed Frame
boundary to supply the schema.

Public operations
-----------------

``with_frame_metadata``
   Return a Frame/table with a schema-directed metadata value encoded.

``frame_metadata``
   Decode with an explicit schema, or reflectively when the optional marker is
   present and registered.

``has_frame_metadata``
   Detect reserved hgraph metadata entries.

``without_frame_metadata``
   Remove only reserved hgraph entries as an explicit lossy conversion.

Operator semantics
------------------

Row filtering, sorting, slicing, and row-preserving projection preserve
metadata. Column replacement and schema-changing projection preserve it unless
the output type explicitly changes the metadata schema. Concatenation requires
equal hgraph metadata by default. A join, group, split, or merge whose correct
metadata is not structurally determined requires an explicit output policy.

Compatibility and performance
-----------------------------

``Frame[Rows]`` and existing metadata-free Arrow tables remain valid. The Frame
still stores one shared Arrow table handle, so the change adds no side
allocation to the native value. Encoding cost is proportional to populated
metadata fields and is paid when metadata is attached or decoded, not for each
row.

Acceptance criteria
-------------------

* C++ and Python type resolution and round trips use the same Arrow table.
* Marker-bearing and markerless schema-directed decoding are tested.
* Missing required fields, incompatible markers, unsupported values, and
  implicit metadata loss are rejected.
* Structural table transforms follow the propagation rules above.
* Direct Arrow C-stream and FrameStore paths preserve schema metadata.
* Installed-SDK tests prove downstream C++ use.

Implementation status
---------------------

The accepted implementation covers the
two-parameter C++ and Python Frame type, field-wise Arrow schema codec,
marker-bearing and markerless decoding, Python conversion validation,
metadata-aware table overloads, installed-SDK use, and C++/Python conformance
tests.

Versioned-dataset proof
-----------------------

The proving fixture stores dataset identity, immutable revision, as-of time,
source, and schema version as frame metadata. Each row contains independently
filterable domain data. The fixture proves that row operations do not duplicate
or discard the dataset-level fields.
