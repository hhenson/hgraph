RFC 0042: Types as Values
=========================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-09-24
:Target: ``TypeCarrier``, the Python bridge, ``CompoundScalar`` binding,
         ``table_schema``

Summary
-------

Let a type be an ordinary runtime value. RFC 0033's ``TypeCarrier``, which
today exists only while wiring, gains a Python conversion, a text form (its
name) and a wire form (also its name, parsed back through the registry). A
Python ``CompoundScalar`` that declares a ``namespace`` binds to a native
schema of the same qualified name, validated field by field, so the native
schema decides storage even where a Python annotation cannot say it. With
these, ``table_schema`` becomes a native, const-evaluable C++ operator that
publishes released hgraph's ``TableSchema``, and the column types of a table
schema get their Arrow names, the vocabulary of the frame a schema describes.

Motivation
----------

Issue #821 made ``table_schema(tp)`` a constant ``TS[TableSchema]`` inside a
graph, as in released hgraph 0.5.41. PR #1638 did it in Python: the layout is
computed in C++, but the ``TableSchema`` value was assembled in Python and
wired with ``const``. Review of #1638 cited ``AGENTS.md``: a feature exposed to
Python needs an equivalent, first-class C++ wiring path. The owner ruled that
``table_schema`` should be C++ once the obstacles were known.

The obstacle is the value, not the operator. Released hgraph's
``TableSchema`` holds types: ``tp`` is the time-series type and ``types`` the
Python class of each column. A type had no runtime value in this runtime:

- ``TypeCarrier`` (RFC 0033) is registered as a standard scalar, with
  equality, hashing and text, but it had no Python conversion, no JSON form
  and no binary wire form. It crossed only as a wiring-time argument.
- Python spells a type ``type`` (or ``type[T]``), and that annotation
  deliberately maps to the Python-object scalar: a user's ``type`` field may
  hold any class, including classes hgraph never registers. A Python class
  therefore cannot describe a native type-valued field through its
  annotations.

A spike (branch ``spike/native-table-schema``) built the whole feature to
find the real issues; this RFC is what it found, with the owner's rulings.

Rulings recorded (owner, 2026-09-24)
------------------------------------

- ``table_schema`` is C++.
- The text of a type is its name: ``str_`` of ``int`` is ``int``, of a
  time-series type ``TS[int]``.
- Types serialise to and from their name.
- A table schema's column types use the Arrow vocabulary, because a schema's
  target is an Arrow frame; the representation is whichever is more
  efficient and extensible (this RFC chooses type values, below).
- A Python class binds to its native schema by name, once the idea is
  validated (the validation is in *Evidence*).

Design
------

1. A type is a value
~~~~~~~~~~~~~~~~~~~~

``TypeCarrier`` keeps its RFC 0033 form: a closed sum of an interned
time-series schema, an interned value schema, or a size. It is one pointer or
size, so copying and comparing a type value is as cheap as a pointer.

- **Equality and hashing** are by the interned schema, as today.
- **Text** is the type's name (``int``, ``datetime``, ``TS[int]``,
  ``TSD[str, TS[int]]``, a qualified name for a named bundle or enum). Today it
  renders ``type[int]``.
- **Python conversion.** A hook pair, the mechanism ``WiredFn`` uses. To
  Python, a type is what a type argument already crosses as: a ``TsType``
  for a time-series type, the Python class of a scalar type (through the
  reverse binding), or an ``int`` for a size. From Python, it accepts what a
  type-argument slot accepts: a ``TS[...]`` expression, a class, a schema or
  a size.
- **No per-tick registry cost.** A type is resolved once, to an interned
  schema, and referenced after that. Measured with the output dropped
  (``null_sink``), a Python node producing a new value with type fields every
  tick, and a Python reader converting them, take no type-system locks during
  evaluation (``CLAUDE.md`` §7).

2. A type serialises as its name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The wire form and the JSON form of a type value are its canonical name,
tagged with its kind (``scalar:int``, ``ts:TS[int]``, ``size:3``; see
*Differences from the proposal*). Decoding parses the name and resolves it through the
registry. The parse is a cold-path operation, cached by name, so replaying a
recording that repeats a type costs one lookup per distinct name.

A named type (a registered bundle or enum) must be registered in the decoding
process before decoding, as the binary codec already requires of a named
bundle. Until this lands, the binary codec refuses a type value
(``BinaryWireFormError``), which is correct: a carrier holds interned
pointers, which must never reach the wire.

The core had no type-name parser, and RFC 0022's schema descriptor is
one-way (identity bytes, no decode). This RFC adds the parser
(``types/metadata/type_names.h``). Its grammar is the names the registry
prints: a registered name (a scalar, a named bundle or enum, a named TSB, an
alias) resolves by lookup; ``Tuple``, ``VariadicTuple``, ``NullableTuple``,
``List``, ``MutableList``, ``Set``, ``MutableSet``, ``Map``, ``MutableMap``,
``CyclicBuffer``, ``Queue``, ``Array``, ``Owned``, ``Shared``, ``frame``,
``series`` and ``Bundle{...}`` on the value side, and ``TS``, ``TSS``, ``TSD``,
``TSL``, both ``TSW`` forms, ``REF`` and ``TSB{...}`` on the time-series side,
are built through the constructor that printed them.

3. A Python class binds to its native schema by name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``namespace=`` on a ``CompoundScalar`` already means "resolve the same
globally named schema as the public C++ one"; the Kafka and fabric
extensions use it. Today that works only when the class's annotations produce
exactly the native field list. A different field list is a hard registry
error, and no second type is ever created silently.

This RFC makes the binding explicit. When a class declares a ``namespace``
and ``<namespace>::<ClassName>`` is already a registered native schema, the
class binds to that schema instead of registering one from its annotations.
The binding is validated:

- the field names, in order, equal the native schema's;
- each annotation produces the native field's type, with one relaxation:
  Python spells a native type value ``type``, at any depth (``tuple[type, ...]``
  stands for ``tuple[TypeCarrier, ...]``);
- a generic class specialises under its own name (``TableSchema[int]``) and
  so never binds to the native schema.

One Python class is the face of one native schema: a second class of a
different reconstruction shape is refused by the existing class registration,
while an identical redefinition (a module reload) re-registers, as for any
``CompoundScalar``. A subclass of a bound class is an ordinary
derived bundle that inherits the native fields. The native schema must be
registered before its Python face is first used; the core's schemas register
when ``_hgraph`` is imported, and an extension's when its native module is.

Every class that binds today still binds, to the identical schema, because
today's rule (exact field-list equality) is stricter than the validation.

4. ``table_schema`` is native
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``hgraph::TableSchema`` is a native bundle with released hgraph's fields:
``tp: type``, ``keys: tuple[str, ...]``, ``types: tuple[type, ...]``,
``partition_keys``, ``removed_keys``, ``date_time_key``, ``as_of_key`` and
``is_multi_row``. ``table_schema(tp) -> TS[TableSchema]`` is a const-evaluable
compose over ``ts_table_layout``, under the graph's table configuration. Its
eager kernel answers ``table_schema(tp).value``; outside any graph the default
configuration applies, as the Python column-name accessors already do.

Python's ``TableSchema`` keeps released hgraph's annotations and binds to the
native schema (``namespace="hgraph"``), so graph outputs, eager values and
user-built values are one type, and ``types`` holds the Python class of
every column, as in released hgraph. (The Python-assembled value gave
``object`` for every column outside nine leaves.)

``evaluate_const`` applies type-argument roles and unwraps its arguments, as
wiring does. Without that, a ``TS[...]`` expression never reached a
const-evaluable type argument; this affects every such operator, not only
``table_schema``.

5. Column types in the Arrow vocabulary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A table's columns come from a closed set: ``HGRAPH_TABLE_ATOMIC_LEAVES`` in
``table_codec.cpp``, 17 leaves, and lists of them. Each maps to one Arrow
type. ``table_column_type_name(leaf)`` (and ``TableSchema.arrow_types`` in
Python) names a column type in that vocabulary:

- Arrow's own name where Arrow has a type of its own for the leaf:
  ``bool``, ``int64``, ``double``, ``string``, ``binary``, ``date32[day]``,
  ``timestamp[us, tz=UTC]``, ``duration[us]``, ``time64[us]``,
  ``timestamp[us]``, ``month_day_nano_interval``;
- the hgraph name where Arrow's type is structural or shared with an earlier
  leaf: ``zone_id`` (``utf8``, like ``str``), ``zoned_datetime``,
  ``instant_range``, ``civil_date_range``, ``instant_range_set``,
  ``civil_date_range_set``;
- ``list<element>`` for a sequence, recursively, so the shared-type rule
  applies inside it too.

The vocabulary is generated from the leaf list, so a new leaf is named where
it is added. It is extensible in the way the owner proposed: strings, with
names for the types Arrow lacks.

Differences from the proposal
-----------------------------

Recorded on acceptance (PRs #1643, #1644, #1645 and the serialisation PR):

- **The serialised form names the kind.** A named TSB and its value-side
  bundle share one name, so a bare name cannot say which kind a type value
  is. The wire and JSON forms are ``scalar:<name>``, ``ts:<name>`` or
  ``size:<n>``; the text stays the bare name.
- **The grammar** is the registry's own printed forms, listed in section 2,
  rather than the Python spellings (``tuple``, ``frozenset``, ``dict``) first
  written here.
- **One face per native schema**, refined: a second class of a different
  reconstruction shape is refused; an identical redefinition (a module
  reload) re-registers, as for any ``CompoundScalar``.
- **The ``evaluate_const`` fix** landed with ``table_schema`` (step 3), its
  first user, rather than with step 1.
- **The decode cache** is guarded by a counted ``TypeSystemMutex``. Decoding
  is a boundary operation (restore, transport), not evaluation, so the
  per-tick registry-free rule is unaffected.

Alternatives considered
-----------------------

- **Column types as strings in the value.** Rejected on the spike's
  evidence. Released hgraph's ``types`` holds classes, and
  ``make_table_schema`` takes them; 16 tests failed (nine ported from released
  hgraph, six data-frame converters, the #821 regression). A string is also
  larger and slower to compare than a type value, and ``tp`` needs a type
  value with a string form anyway, which the column types can share.
- **An Arrow schema as a new value kind.** A second value kind with its own
  codecs, text and Python conversion, where the Arrow vocabulary above gives
  the frame's column types without one.
- **A public "hgraph type" annotation.** A new Python API, where
  ``namespace=`` already expresses "this class is that native schema".
- **A new class attribute naming the native schema.** The spike's first form
  (``__native_schema__``). The same capability as ``namespace=`` under a second
  name.

Evidence
--------

The spike, on ``main`` @ ``a629ad27b``:

- the final form (``6c976ca66``) on Linux: native ctest 2252/2252 with every
  extension; the Python suite 3964 passed, its one failure the new
  operator's missing HGL catalogue disposition;
- locally, all 2116 table, data-frame, delta-adaptor and compound scalar
  tests pass; graph value, eager value (in and outside a graph),
  ``getattr_`` of every field, ``table_shape``, ``dataclasses.replace``,
  ``str_`` and a registry reset behave; the binding rejects a changed
  field order, a missing field and a field of the wrong type, each with a
  message naming the field;
- zero type-system locks per tick for type-valued fields in either direction.

Implementation plan
-------------------

1. **Type values.** ``TypeCarrier`` Python conversion hooks; text as the
   name; ``evaluate_const`` argument roles.
2. **Binding by namespace**, with validation, documented in
   ``python_bridge.rst``.
3. **Native ``table_schema``**: the bundle and operator, the Python wrapper,
   the Arrow vocabulary, native and Python tests, regenerated operator
   artifacts and the HGL catalogue disposition.
4. **Serialisation by name**: the core type-name parser, the binary atom
   form and the JSON form of a type value.

Each lands with its developer-guide change.
