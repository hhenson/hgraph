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
to either table and costs nothing for them. That includes the root: the reader
supplies it, as with ``from_binary_string``, so it is bound directly and is
never a table entry. A framed ``int`` is its eight bytes, a six-byte header and
two empty table counts.

The session is a schema table plus one bound converter per entry, bound the
first time an entry is used. RFC 0039's image codec already held exactly that
pair privately; it now uses the session
(``hgraph/types/value/binary_session.h``), so there is one implementation and
an ``Any`` inside a checkpoint payload names its schema in the image's own
table. Converters write through a ``BinaryWriter`` cursor -- the mirror of
``BinaryReader`` -- which is how the session reaches them; both cursors carry a
session pointer and a subreader inherits it.

Totality
~~~~~~~~

``Any``
   A schema-table index, one past it so that zero is the empty box, followed by
   the value under that schema. Nesting is ordinary recursion, which means the
   table has to be able to name ``Any`` itself: the structural recipes gain the
   unconstrained ``Any``, and the ring-buffer and queue kinds they also lacked.
   An empty box needs no table, so it can still be written with
   ``to_binary_string``; a full one outside a session is refused, naming
   ``encode_binary_frame`` and the session as the way to write it.

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
   and writes them back unchanged.

   A value that exists only as a Python object leaves no other choice, so
   pickling is always permitted and is never an error. It is, however, slow,
   large, opaque to every native reader, and a sign that a type could be given a
   schema. **Binding a converter that will pickle logs a warning**, once per
   schema per process, naming the schema and where it is bound, so that the
   author can consider a better design. Unpickling executes code; it is applied
   only to bytes this deployment wrote, which is already the ``dmap_`` trust
   boundary.

Atomics
   Every native atomic resolves, in order, to: a built-in form; a wire form
   registered with the scalar (``register_binary_atom``, the RFC 0003
   registration's new optional argument); or the raw image when it is trivially
   copyable *and* its registration declares it portable.

   A native atom with none of these **fails at wiring**. There is no fallback:
   a type that silently pickled, or silently refused at the first checkpoint
   hours into a run, would be worse than one that cannot be wired. The error
   names the scalar, the endpoint or boundary slot that needs it, and the fix:

   .. code-block:: text

      binary codec: scalar 'acme.Quote' has no wire form, required by
      dmap_ boundary slot 'quotes' (TSD[str, TS[acme.Quote]]).
      Register one with register_binary_atom<acme::Quote>(write, read), or
      declare the type portable if it is trivially copyable and has no
      pointers or padding-dependent layout.

   Wiring is where this is decidable: a ``dmap_`` / ``spawn`` boundary plan, a
   recoverable component's identity, and a store binding are all built then.
   Python-owned scalars (RFC 0004) are Python objects and take the pickle path
   above, with its warning.

``PythonOnly`` storage
   A typed endpoint whose value happens to be held as a Python object is
   converted through the bridge to its **native** value and encoded by schema.
   It is not pickled: its schema is a native one and a native reader must be
   able to load it.

What remains refused, by design, is what is not data: a
``TimeSeriesReference`` outside a checkpoint's locator context, and internal
handles such as wiring callables and recovery configuration.

One thing is refused that is not an encoding at all: an ``Any`` has no
``portable_hash``. The hash assigns a ``dmap_`` key to a worker, it runs with
no session to name a schema in, and resolving a converter per call would put a
registry lock on the per-tick path. An ``Any`` is not a usable partition key.

Profiles
--------

.. code-block:: cpp

   enum class BinaryProfile : std::uint8_t { Compact = 0, Fast = 1 };

   BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *, BinaryProfile);

The profile is fixed when a converter is bound and recorded in the first byte
of whatever frames the bytes -- the RFC 0017 stream envelope, the RFC 0039
image header, a ``ValueStore`` object -- so a reader never guesses. A bare
``to_binary_string`` has no frame; its caller states the profile on both sides.

The byte after it is the **revision** of that profile's encoding. A profile's
name is stable while its bytes change as the stages below land, and bytes
written between two stages must not be misread by the later one. Revision 0 of
*either* profile is the RFC 0017 field-wise encoding; ``Fast`` becomes
revision 1 at stage 2 and ``Compact`` at stage 3. A reader refuses a revision
it does not know, by number. ``Compact`` revisions stay readable, because they
are stored; an old ``Fast`` revision need not be, because it never outlives a
run.

The self-contained frame (``encode_binary_frame`` / ``decode_binary_frame``):

.. code-block:: text

   u8   profile
   u8   revision
   u32  payload length, little-endian
        payload
        session tables

The payload precedes its tables so that it is written in place: the tables are
only known once the payload has been encoded, and putting them first would
mean encoding into a scratch buffer and copying.

Both profiles are portable across builds, compilers and architectures. Neither
is a memory image.

``Fast`` -- minimise encode and decode time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For bytes that live for one cycle: ``dmap_`` and ``spawn`` payloads, and an
online snapshot handed to an asynchronous writer.

Revision 1, as built:

* Every numeric and temporal atom is fixed width, as it already was. Counts
  stay varints: there is one per sequence, and it is not what costs.
* A **sequence of fixed-width atoms** -- list, ring buffer, queue, set -- is a
  count and one contiguous little-endian block. These are the bytes the
  field-wise form already wrote; what changes is that the writer builds no
  ``ValueView`` per element and the reader no ``Value``: it hands the block to
  the container's storage, which copies it as one. A nullable list keeps the
  field-wise form.
* A **map of fixed-width keys and values** is two blocks, keys then values,
  with a flag byte saying whether a presence bitmap sits between them. An
  unset value keeps its place in the block, zeroed. Interleaved, as the
  field-wise form has it, there is nothing to copy in bulk.
* A **list of composite rows** is written **by column**: for each field a flag
  byte, a presence bitmap only if some row lacks the field, then that field for
  every row. A fixed-width field is a block addressed by row (an unset one
  zeroed), so it is a strided copy each way. Text is a block of four-byte
  lengths and then the characters. Any other field -- a nested list, row or
  map -- is written by its own converter for the rows that have it. The reader
  constructs every row once and fills them where they stand
  (``ListBuilder::append_default``, ``CompositeFieldLayout``). A polymorphic,
  indirect or wrapped row keeps the field-wise form.
* Nothing is deduplicated and nothing is compressed.

Two things the proposal had that revision 1 does not:

* **No padding to eight bytes.** Alignment only pays a reader that uses the
  block where it lies. This one copies into its own storage, and a payload
  written in place inside a message has no fixed origin to align to.
* **No new ops-table entry.** The clean way to read contiguous elements out of
  an arbitrary view is an ``IndexedValueOps`` entry returning its spans, which
  is an ABI change to every compiled extension. It was not needed to meet the
  bar: calling the existing ``element_at`` and copying the atom is 3x faster
  than the field-wise writer. It is the next step if encode ever matters more.

Measured (``hgraph_unit_tests '[codec-benchmark]'``, Release, Apple arm64),
field-wise against ``Fast`` revision 1:

.. list-table::
   :header-rows: 1
   :widths: 30 17 17 18 18

   * - shape
     - encode
     - decode
     - bytes before
     - bytes after
   * - ``list<int>`` x 1,000,000
     - 7.2 -> 2.4 ms
     - 25.1 -> 0.10 ms
     - 8,000,003
     - 8,000,003
   * - ``list<row6>`` x 100,000
     - 5.2 -> 1.6 ms
     - 33.5 -> 2.4 ms
     - 3,980,003
     - 4,180,009
   * - ``map<int,float>`` x 100,000
     - 1.0 -> 0.55 ms
     - 6.9 -> 0.61 ms
     - 1,700,003
     - 1,600,004

Three passes of raw output are in
``benchmarks/results/rfc0040-stage2-20260918-macos.md``.

``row6`` is the acceptance case: an int, two floats, a timestamp, a boolean and
a string. It is 5% larger under ``Fast`` because string lengths are four bytes;
that is the profile doing what it is for.

One finding was not about the codec. Every value builder ended in
``Value{binding, &storage}``, which *copies* the storage it has just built, so
every built container in the system was constructed twice. ``build()`` now
moves its storage into the ``Value`` (``Value::AdoptStorage``).

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
* The frame is **block compressed** through ``arrow::util::Codec`` -- Arrow is
  already a core dependency, so this adds none. Compression is **on by default
  for everything stored** (recordings, checkpoints, ``ValueStore`` objects):
  zstd when the Arrow build provides it, LZ4 otherwise, and uncompressed only
  when neither is available or the payload is below the threshold at which a
  frame header costs more than it saves. The frame records the codec, and a
  reader without it refuses by name. ``Fast`` never compresses.

The compression block (``hgraph/types/value/binary_compression.h``) -- **done**:

.. code-block:: text

   u8      codec           0 none, 1 zstd, 2 lz4 (frame format)
   varint  stored length
   varint  raw length      only when the codec is not none
           bytes

A block is stored as it is when it is under 256 bytes, when the codec is not in
this build, or when compressing it would not make it smaller, so compression
only ever shrinks. The raw length is the writer's claim, so a reader bounds it
before allocating for it; a caller that checksums its bytes does so first. An
uncompressed block is read in place, with no copy.

RFC 0039 checkpoint images use it as **format version 3**: profile, revision,
then one block holding the tables and the body, with the checksum over the
compressed bytes. On the RFC 0039 benchmark endpoint (100,000-key
``TSD[int, TS[float]]``) the stored image goes from 1,931,274 to **184,272
bytes** with zstd, for about 1.9 ms more to encode and 2.8 ms more to decode.
That endpoint's keys and values are synthetic and regular; real floating-point
state will compress less.

Choosing
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 20 40

   * - Use
     - Default
     - Override
   * - checkpoint image, ``ValueStore``, recording, journal
     - ``Compact``, compressed
     - store / recovery configuration
   * - ``dmap_`` / ``spawn`` boundary, in-process worker
     - ``Fast``, never compressed
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
  **Done.** One slot's bind was ten lock acquisitions; a cycle now takes none,
  and a test holds it there with the counted type-system mutex.
* Each payload is encoded into a temporary string and then copied behind a
  length prefix, and a ``BoundaryTransfer`` payload is itself an opaque byte
  string that is encoded again -- two copies per hop. The length is reserved
  as a fixed four bytes and the payload written in place behind it. **Done.**

Measured per cycle, all slots carrying the boundary transfer's byte string
(``hgraph_unit_tests '[protocol-benchmark]'``, Release, Apple arm64):

.. list-table::
   :header-rows: 1
   :widths: 24 19 19 19 19

   * - slots x bytes
     - encode before
     - encode after
     - decode before
     - decode after
   * - 1 x 64
     - 0.23 us
     - 0.05 us
     - 0.26 us
     - 0.10 us
   * - 32 x 64
     - 4.89 us
     - 0.72 us
     - 5.06 us
     - 1.52 us
   * - 32 x 4,096
     - 11.2 us
     - 5.8 us
     - 9.3 us
     - 6.0 us
   * - 32 x 262,144
     - 354 us
     - 226 us
     - 226 us
     - 228 us

The single-threaded figures understate the first defect: the locks are global,
so in-process workers on several threads contended for them every cycle. What
is left at large sizes is one copy each way -- into the message, and out of it
into the ``Bytes`` value that ``BoundaryTransfer::apply`` reads.

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

One existing use stays, by decision (2026-09-18): RFC 0001 writes structured
frame-metadata fields as JSON text. Arrow schema metadata is string-to-string
and is read by Polars, pandas and other tools that know nothing of hgraph, so
this is a property deliberately *represented as JSON* for compatibility with
non-hgraph users -- the case JSON is for -- and not a serialization path.

Compatibility
-------------

The RFC 0017 field-wise encoding is **revision 0** of both profiles, so nothing
already written changes meaning. It stays readable wherever bytes were stored,
which is RFC 0039 version 2 images and nothing else -- ``dmap_`` and ``spawn``
payloads never outlive a run. A version 2 image has no profile or revision in
its header and is read as ``Compact`` revision 0; the header gains both as
format version 3 at stage 3, and version 2 remains readable.

``BinaryConverter`` gains no field and keeps its size; its write function now
takes the ``BinaryWriter`` cursor rather than a bare string, and the string
overloads of ``write`` remain. ``BinaryReader`` gains the session pointer, so
code compiled against the old header that constructs one must be rebuilt --
in-tree that is the core and the Python bridge, and no extension does.
``bind_binary_converter(meta, profile)`` is an overload beside the existing
function, not a default argument, so the existing symbol is unchanged.
``register_binary_atom`` is additive.

Stages
------

1. Session, shared schema table, ``Any``, profile plumbing and the frame bytes,
   with both profiles at revision 0, today's encoding. **Done**: the image
   codec is on the session (byte-identical images, no change in encode or
   decode time), and sets and maps are written in place rather than through a
   scratch copy. The RFC 0039 image header is *not* changed by this stage: it
   gains the profile and revision at stage 3, when its payload bytes first
   change, so that no image ever claims an encoding it does not hold.
2. ``Fast``: bulk atom blocks, columnar bundle sequences, in-place container
   construction on read. ``dmap_`` and ``spawn`` switch to it. **Done**, as
   ``Fast`` revision 1: ``BoundarySlots`` and ``BoundaryTransfer`` bind for it.
3. ``Compact``: integer and enum varints, adaptive columns, text dictionaries,
   optional block compression. Checkpoints and stores switch to it.
4. Python objects: the bridge hook, the bind-time warning, ``PickledObject``
   pass-through, ``PythonOnly`` endpoint capture, ``register_binary_atom`` and
   the wiring-time refusal of a native atom with no wire form.
5. JSON retirement: codec registration and defaults, Fabric, the ratchet,
   RFC 0030 amendment.

Acceptance
----------

* A property test over generated schemas -- every kind, nested, nullable,
  polymorphic, ``Any``, frames, Python objects -- round-trips under both
  profiles, and decoding one profile's bytes as the other is refused by the
  frame, never misread.
* Every schema built from built-in kinds binds under both profiles; the test
  enumerates the registry rather than a hand-written list. A native atom with
  no wire form is refused **at wiring**, by name, with the registration hint;
  binding a Python-object schema logs exactly one warning per schema.
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

Decisions
---------

Recorded 2026-09-18.

* **Compression** is on for everything stored and off for everything that
  crosses a process boundary.
* **Pickle** is always permitted for a value that exists only as a Python
  object, and binding such a converter warns.
* **Frame metadata** stays JSON text (RFC 0001): compatibility with non-hgraph
  readers is the point of it.
* **A native atom with no wire form fails at wiring** with a message naming the
  scalar, where it is needed, and how to register one. There is no pickle
  fallback for native atoms.
