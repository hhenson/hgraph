RFC 0040: A Total Binary Codec with Size and Speed Profiles
===========================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-18
:Target: Value serialization for persistence, checkpoints, and inter-process transport
:Related: RFC 0017 (binary value codec), RFC 0030 (typed value persistence),
          RFC 0035 (python-free type layer), RFC 0037 (distributed map),
          RFC 0038 (spawn pipelines), RFC 0039 (compact checkpoint images)

Summary
-------

JSON is a *representation*: it is used when a value has to be shown, exchanged
or stored **as JSON**. It is not a serialization format for hgraph state.
Checkpoints, stores, journals, recordings, and the payloads that cross a
``dmap_`` or ``spawn`` boundary use the binary codec.

Two things prevent that rule from holding today. The RFC 0017 codec is
**partial**: it refuses ``Any``, Python objects, and every atomic without a
hand-written wire form, so a caller that must not fail reaches for JSON. And it
has **one encoding**, which is neither as small as a disk format should be nor
as fast as a process-to-process format could be.

This RFC makes the codec **total** over every value schema the type system
admits, carries a native Python object as a pickle, and gives the codec two
**profiles** over one data model: ``Compact``, which minimises bytes, and
``Fast``, which minimises encode and decode time. It then retires JSON from
the remaining serialization paths.

Ruling
------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Binary codec (this RFC)
     - JSON
   * - checkpoint images (RFC 0039)
     - ``to_json`` / ``from_json`` operators
   * - ``ValueStore`` objects, Fabric metadata and revisions
     - the json adaptor and any external protocol that *is* JSON
   * - record/replay and input journals
     - diagnostics, manifests and logs meant to be read by a person
   * - ``dmap_`` / ``spawn`` boundary payloads
     - a store a user *explicitly* configures with the ``"json"`` codec

``python/tests/test_architecture_ratchets.py`` gains a ratchet over
``to_json_string``, ``from_json_string``, ``JsonConverter`` and
``JSON_VALUE_CODEC`` in ``src/hgraph/runtime``, ``src/hgraph/types/value``
(outside ``json_codec``), and ``extensions/*/src``. The count may only fall.

How JSON got in is recorded because it is the reason for the ratchet: the
ruling existed only verbally. RFC 0030 made JSON "the required baseline and the
initial default" of ``ValueStore`` and registered no other codec, and the first
component checkpoint wrote every value with ``to_json_string`` although
RFC 0023 says checkpoint images share the binary value codec.

What is refused today
---------------------

``binary_converter`` throws at synthesis for:

* ``ValueTypeKind::Any`` -- and therefore every ``opaque_python`` nominal type,
  which is a Python annotation over ``Any`` storage;
* an atomic that is not trivially copyable and is not one of the dozen
  hand-enumerated types (``Str``, ``Bytes``, ``Frame``, ``Series``, the zone and
  range types);
* a trivially copyable atomic that is not buffer compatible, an enum, or one of
  three named temporal types -- "no portable wire form";
* a buffer-compatible atomic wider than eight bytes; and
* a derived ``Bundle`` met through an unbound converter.

Separately, endpoint storage selected as ``PythonOnly`` refuses checkpointing
altogether (``ts_data_atomic_ops.cpp``).

Data model
----------

Unchanged from RFC 0017: schema-driven, no field names, presence bitmaps for
bundles and nullable sequences, little-endian. What changes is that encoding
becomes a **session** and that the byte-level form of each node of the schema
is chosen by a **profile**.

Session
~~~~~~~

``to_binary_string`` is stateless, so a polymorphic bundle writes its concrete
schema *name* with every value, and ``Any`` cannot name a schema at all. An
encode or decode call is now a session that owns two tables, emitted once:

* a **schema table** -- the recipes RFC 0039 introduced for checkpoint images
  (name, manifest descriptor, structural recipe over earlier entries, including
  ``Frame[Row, Meta]`` and ``Series[T]``), moved down into the value codec so
  that checkpoint images, the stream envelope and ``Any`` share one
  implementation; and
* a **string table** for repeated text that is not a value: polymorphic
  alternative names, enum member names when a profile writes them.

A value whose schema is known to the reader -- the common case -- adds nothing
to either table and costs nothing for them.

Totality
~~~~~~~~

``Any``
   A schema-table index followed by the value under that schema. An empty box
   is index zero. Nesting is ordinary recursion.

Python objects
   The type layer is Python-free (RFC 0035) and stays so. The bridge registers
   one more function-pointer table, beside the storage provider:

   .. code-block:: cpp

      struct PythonObjectCodecOps
      {
          void (*dumps)(const void *object, std::string &out);   // pickle, protocol 5
          Value (*loads)(ValueTypeRef binding, std::string_view bytes);
      };

   The wire form is a varint length and the pickle. The hook takes the GIL; the
   codec never includes a Python header. A process with no bridge cannot hold a
   Python object and so never encodes one; when it *decodes* one -- a native
   store, a forwarding stage -- it keeps the bytes in a ``PickledObject`` atom
   and writes them back unchanged. Unpickling executes code: it is permitted
   only for bytes this deployment wrote, which is already the ``dmap_`` trust
   boundary, and the decode limits gain ``allow_pickle`` (default true for
   checkpoints and boundaries, false for the descriptive stream reader).

Atomics
   Every atomic resolves, in order, to: a built-in form; a wire form registered
   with the scalar (``register_binary_atom``, the RFC 0003 registration's new
   optional argument); the raw image when it is trivially copyable *and* its
   registration declares it portable; and finally, when the scalar has a Python
   conversion, its pickle. The last step is what makes the codec total for
   every type a Python user can name. It is slow and it is reported: binding
   returns the chosen form, and ``RuntimeRegistrySnapshot`` counts atoms bound
   by fallback so a hot path that depends on one is visible.

``PythonOnly`` storage
   A typed endpoint whose value happens to be held as a Python object is
   converted through the bridge to its **native** value and encoded by schema.
   It is not pickled: its schema is a native one and a native reader must be
   able to load it.

What remains refused, by design, is what is not data: a
``TimeSeriesReference`` outside a checkpoint's locator context, and internal
handles such as wiring callables and recovery configuration.

Profiles
--------

.. code-block:: cpp

   enum class BinaryProfile : std::uint8_t { Compact = 0, Fast = 1 };

   BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *, BinaryProfile);

The profile is fixed when a converter is bound and recorded in the first byte
of whatever frames the bytes -- the RFC 0017 stream envelope, the RFC 0039
image header, a ``ValueStore`` object -- so a reader never guesses. A bare
``to_binary_string`` has no frame; its caller states the profile on both sides.

Both profiles are portable across builds, compilers and architectures. Neither
is a memory image.

``Fast`` -- minimise encode and decode time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For bytes that live for one cycle: ``dmap_`` and ``spawn`` payloads, and an
online snapshot handed to an asynchronous writer.

* Every numeric and temporal atom is fixed width. No varint touches a payload.
* A sequence of fixed-width atoms is a count, padding to eight bytes, and one
  contiguous little-endian block: one ``memcpy`` out and one in, with the
  reader building the container in place rather than one ``Value`` per
  element.
* A sequence of bundles is written **by column**: one validity bitmap and one
  block per fixed-width field. Source storage is by row, so encoding is a
  gather per field; it has no per-element dispatch and decodes to bulk copies.
* Text is length-prefixed and never deduplicated. Nothing is compressed.

``Compact`` -- minimise bytes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For bytes that are stored: checkpoints, ``ValueStore`` objects, recordings.

* Integers and enum ordinals are zig-zag varints. Booleans in a sequence are
  bit-packed.
* A sequence of fixed-width atoms is a **column** with a one-byte encoding
  chosen by the encoder from a single scan: ``raw``; ``varint``; ``delta``
  (zig-zag varint of successive differences, which is what sorted keys and
  timestamps want); ``constant``. Floating-point columns are ``raw`` or
  ``constant``; they are left to block compression.
* A sequence of bundles is columnar, as in ``Fast``, and each field column is
  encoded independently.
* A text column with few distinct values is a dictionary plus indices.
* The frame may be **block compressed** with zstd or LZ4 through
  ``arrow::util::Codec`` -- Arrow is already a core dependency, so this adds
  none -- above a size threshold and only when the codec is available. The
  frame records the compression, and a reader without it refuses by name.

Choosing
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 20 40

   * - Use
     - Default
     - Override
   * - checkpoint image, ``ValueStore``, recording, journal
     - ``Compact``
     - store / recovery configuration
   * - ``dmap_`` / ``spawn`` boundary, in-process worker
     - ``Fast``
     - ``WorkerPoolConfig`` / ``SpawnConfig``
   * - snapshot awaiting an asynchronous durable write
     - ``Fast`` capture, ``Compact`` on the writer thread
     - --

Transport path
--------------

Two defects in the ``dmap_`` / ``spawn`` exchange are fixed with the switch to
``Fast``, because they cost more than the encoding does:

* ``distributed_protocol.cpp`` binds a converter **per slot, per cycle, in both
  directions** -- ``to_binary_string`` on the way out and
  ``bind_binary_converter(schema).read`` on the way in. Binding takes the
  realization-snapshot and converter-registry mutexes and heap-allocates a
  converter tree. That is build-time machinery on the per-tick path, against
  the codec header's own rule and the single-threaded evaluation ruling.
  ``BoundarySlots`` binds each slot's converter once, when the slot is added.
* Each payload is encoded into a temporary string and then copied behind a
  length prefix, and a ``BoundaryTransfer`` payload is itself an opaque byte
  string that is encoded again -- two copies per hop. The session reserves the
  length and writes the payload in place.

Retiring JSON
-------------

1. ``hgraph-persistence`` registers ``"binary"`` (``Compact``) and
   ``"binary-fast"`` and makes ``"binary"`` the ``ValueStore`` default. Every
   object already records its codec, so existing JSON objects stay readable;
   ``"json"`` remains registered for a store that is *meant* to hold JSON.
   RFC 0030 is amended.
2. Fabric's ``metadata_codec`` and Kafka revision payloads default to
   ``"binary"``.
3. The version 1 checkpoint reader is the one remaining JSON decoder on a
   serialization path. It is read-only and exists for images that
   hgraph 0.8.25-0.8.27 published.
4. The ratchet above lands with step 1.

One existing use is a judgement call and is **not** changed here: RFC 0001
writes structured frame-metadata fields as JSON text, because Arrow schema
metadata is string-to-string and is read by Polars and pandas. That is a
property represented as JSON for third parties. If it should instead be
base64 of the binary form, that is a change to RFC 0001's contract.

Compatibility
-------------

The RFC 0017 field-wise encoding becomes profile ``Compact`` **version 1** only
where they coincide; they do not, so the existing encoding is kept readable as
``Legacy`` (profile byte absent) wherever bytes were stored: RFC 0039 version 2
images and nothing else -- ``dmap_`` and ``spawn`` payloads never outlive a run.
The image header gains the profile byte as format version 3; version 2 remains
readable.

``BinaryConverter``'s public struct gains no field. Profiles are separate
interned converters, and the session is a new type, so compiled extensions are
unaffected. ``register_binary_atom`` is additive.

Stages
------

1. Session, shared schema table, ``Any``, profile plumbing and the frame byte,
   with ``Compact`` initially identical to today's encoding.
2. ``Fast``: bulk atom blocks, columnar bundle sequences, in-place container
   construction on read. ``dmap_`` and ``spawn`` switch to it.
3. ``Compact``: integer and enum varints, adaptive columns, text dictionaries,
   optional block compression. Checkpoints and stores switch to it.
4. Python objects: the bridge hook, ``PickledObject`` pass-through,
   ``PythonOnly`` endpoint capture, atomic resolution order and its fallback
   counter.
5. JSON retirement: codec registration and defaults, Fabric, the ratchet,
   RFC 0030 amendment.

Acceptance
----------

* A property test over generated schemas -- every kind, nested, nullable,
  polymorphic, ``Any``, frames, Python objects -- round-trips under both
  profiles, and decoding one profile's bytes as the other is refused by the
  frame, never misread.
* No schema the wiring layer accepts for a time series fails to bind. The test
  enumerates the registry rather than a hand-written list.
* For the RFC 0039 benchmark endpoint and for a 100,000-row list of a
  six-field bundle: ``Compact`` is no larger than today's encoding on any case
  and at least 30% smaller on the integer- and timestamp-keyed ones; ``Fast``
  encodes and decodes at least 3x faster than today's encoding. The numbers are
  recorded with the raw benchmark output, per profile.
* A Python object crosses a ``dmap_`` boundary and survives a checkpoint; a
  C++-only process forwards one without being able to unpickle it.
* The ratchet is at its floor: the version 1 checkpoint reader and nothing else.

Alternatives considered
-----------------------

One encoding with a fast path (RFC 0017's ``trivial_layout``)
   RFC 0017 required the fast path to produce bytes identical to the field-wise
   path. That forbids exactly the choices that make a disk format small --
   varints, deltas, dictionaries -- so one encoding cannot serve both uses.

Raw storage images for ``Fast``
   Fastest of all between two processes of the same build, and ``dmap_``
   workers are. Rejected: padding, container layout and small-buffer storage
   are build properties, so a worker built separately, or a snapshot read by a
   newer build, would misread silently. Columnar fixed-width blocks keep almost
   all of the speed and none of that coupling.

Arrow as the value format
   Excellent for tables and already used for ``Frame``. Poor for the recursive,
   sparse, polymorphic values that make up node state, and it would make every
   small value pay Arrow's per-batch overhead.

Pickle for everything Python can see
   Total and trivial, but opaque to a native reader, large, slow, and it would
   make every store a code-execution surface. Pickle is confined to values that
   have no native schema.

Unresolved questions
--------------------

* Whether block compression is on by default for ``Compact`` checkpoints, and
  which codec.
* Whether ``allow_pickle`` should default to false for ``ValueStore`` reads.
* Whether RFC 0001's frame-metadata text should leave JSON (see above).
* Whether the pickle fallback for unregistered extension atoms should exist, or
  whether an atom with no wire form should fail wiring instead.
