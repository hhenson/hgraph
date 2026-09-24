Open parity questions
=====================

Questions raised while triaging parity issues that neither the runtime
specification nor an earlier ruling decides. Each names the issue, the
observations, the options and a recommendation; the owner's answer moves it
into :doc:`parity_matrix` (accepted) or into a fix.

A native ``table_schema``: what it takes (issue #821, PR #1638)
---------------------------------------------------------------

Owner direction (2026-09-24): ``table_schema`` should be C++; find the
issues first, to choose how. A spike on ``main`` @ ``a629ad27b`` (branch
``spike/native-table-schema``) built it. Native ctest passed 2252/2252 with
every extension. The Python suite passed 3964; its one failure is the missing
HGL catalogue disposition for the new operator.

**What the spike needed, and what it proved**

1. *The operator.* A native ``hgraph::TableSchema`` bundle (``tp: type``,
   ``types: tuple[type, ...]``, the rest as released) and a const-evaluable
   compose ``table_schema(tp) -> TS[TableSchema]`` over
   ``ts_table_layout``. It is about 70 lines, and C++ graphs wire it like any
   operator.
2. *A type as a runtime value.* ``TypeCarrier`` (RFC 0033) is already a
   standard scalar with equality, hashing and text; it lacked a Python
   conversion. A hook pair (the ``WiredFn`` mechanism) sends it to Python as
   what a type argument crosses as (a ``TsType``, the scalar's Python class,
   a size) and accepts whatever a type-argument slot accepts. With it,
   ``getattr_`` of ``keys``, ``types`` and ``tp`` and whole-value reads all
   work. ``types`` holds real Python classes, closer to released hgraph than
   today's ``object`` for every non-leaf column.
3. *Binding the Python class to the native schema.* Python's ``type`` and
   ``type[T]`` deliberately map to the Python-object scalar, so a Python
   ``TableSchema`` cannot describe ``TypeCarrier`` fields through its
   annotations. The spike lets the class name its native schema
   (``__native_schema__ = "hgraph::TableSchema"``). The class keeps released
   hgraph's annotations, and the native schema decides the storage. Graph
   outputs arrive as ``TableSchema`` instances equal to ``.value``, and
   user-built values convert, including an unregistered class in ``types``.
   All 239 table and data-frame tests pass.

**Issues to resolve**

A. *The binding is a new mechanism.* "A Python class is the face of a named
   native schema" needs a design record. It must check that the Python fields
   match the native fields by name and order, and define how it meets
   generics and inheritance. It is useful beyond ``TableSchema``: the Kafka
   extension's classes mirror their native schemas by annotation today. The
   alternative is a public Python annotation meaning "an hgraph type".
B. *No wire form.* The binary codec refuses ``type`` (``BinaryWireFormError``).
   That is correct, since a carrier holds interned pointers. So a
   ``TS[TableSchema]`` cannot be checkpointed, recorded on a durable backend
   or sent across ``dmap_``/``spawn``, and ``to_json`` fails with
   ``unsupported atomic scalar 'type'``. A wire form needs a portable
   schema descriptor, written on encode and resolved back to the interned
   schema on decode (the manifest's schema descriptor is the obvious base).
   The parity case, a constant inside a graph, does not need it.
C. *Per-tick cost.* Converting a type-valued field calls into Python
   (``_carrier_value`` → ``_value_type``) and the reverse binding. A Python
   node producing a ``TableSchema`` every tick measured **+12 type-system
   lock acquisitions per tick**, against the per-tick registry-free rule
   (``CLAUDE.md`` §7). The conversions need a lock-free cache (class →
   carrier, schema → class) in the bridge.
D. *Eager ``.value`` outside a graph.* ``evaluate_const`` needs an active
   ``GlobalState`` for the table configuration. Today's
   ``table_schema(tp).value`` works at module level, so the eager path needs
   the default configuration as a fallback. This is small.
E. *Text.* ``str_`` of a type field writes ``type[int]``, where released
   hgraph writes Python's repr (``<class 'int'>``). The text form of a type
   value is a Text-rule decision (VAL-8).
F. *Registration during conversion.* Converting an unregistered class
   registers an opaque value type. That is harmless at wiring; per tick it is
   issue C again, with the same fix.
G. ``to_table`` of a ``TS[TableSchema]`` has no overload, because a type has
   no column form. Low priority.
H. The new operator needs an HGL catalogue disposition. This is trivial.

**Proposed approach**

- *Phase 1, in process (makes ``table_schema`` native; closes the review
  finding):* the operator; ``TypeCarrier`` Python conversion with cached,
  lock-free conversions (C, F); the native-schema binding (A), with its
  design record; the eager fallback (D); a decision on the text form (E); the
  catalogue entry (H). RFC-level, because it makes a type a first-class
  runtime value: one RFC, "types as values".
- *Phase 2, only when a schema must be persisted or cross a process:* wire
  forms by schema descriptor (B), then ``to_table`` (G).

Questions for the owner: is phase 1 the right scope; the binding (A) or a
public "hgraph type" annotation; and the text of a type value (E).
