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

   * - Binary codec (this RFC): internal communication and state storage
     - An external format: JSON (and, where registered, Avro or protobuf)
   * - checkpoint images (RFC 0039)
     - ``to_json`` / ``from_json`` operators
   * - ``ValueStore`` objects; the metadata Fabric *stores* (revisions, as-of
       and latest indexes)
     - **what hgraph encodes onto Kafka** -- Fabric's revision notifications --
       because a topic is read by tools that rely on JSON, Avro or protobuf
   * - record/replay and input journals
     - the json adaptor and any external protocol that *is* JSON
   * - ``dmap_`` / ``spawn`` boundary payloads
     - diagnostics, manifests and logs meant to be read by a person
   * -
     - a store a user *explicitly* configures with the ``"json"`` codec

The line is between what is internal to hgraph and what is not. The binary
codecs never cross an external boundary when hgraph does the encoding; what a
user encodes into a Kafka record's ``Bytes`` is the user's business.

``python/tests/test_architecture_ratchets.py`` gains two ratchets over the JSON
codec's names: ``json-in-runtime`` (``src/hgraph/runtime``) and
``json-in-persistence-and-fabric`` (the persistence, Fabric and Kafka
extensions). A count may only fall.

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

Revision 1, as built:

* An integer, a duration and an enum -- whose payload is its member's assigned
  integer -- are zig-zag varints. An instant alone stays eight bytes, because
  microseconds since the epoch are never small; it is still an integer, because
  a column of instants is what delta encoding is for.
* Wherever there is a **run of one fixed-width atom** -- a list, ring buffer,
  queue or set, a map's keys or its values, one field of many rows, the keys of
  a checkpointed collection -- it is a **column**: an encoding byte chosen from
  one pass over the values, then the values. ``raw``; ``varint``; ``delta``
  (the first value, then each step from the one before, which is what sorted
  keys and timestamps want; differences wrap, so every pair has a step);
  ``constant``; and ``bits`` for booleans, eight to the byte. Floating point is
  ``raw`` or ``constant`` and is left to block compression.
* A list of composite rows is columnar, as in ``Fast``, with each field's
  column written for the rows that have it and encoded independently.
* A text column is a dictionary and one index per row when few of its strings
  are distinct; building it is abandoned as soon as it holds more than a
  quarter of the rows. Lengths and indices are themselves integer columns.
* A map with an unset value writes a presence bitmap and only the values that
  are set.

A format that writes values of one schema one after another asks for a **run**
(``BoundBinaryConverter::write_run`` / ``read_run``): a column where the
profile has one, each value in turn where it does not. RFC 0039 images write a
collection's keys that way, which is where most of their uncompressed saving
comes from.

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
only ever shrinks. An uncompressed block is read in place, with no copy.

The raw length is the writer's claim and it sizes an allocation, so the **owner
of the bytes bounds it**: ``read_compressed_block`` takes the most the block may
expand to, with a deliberately modest default of 1 GiB. A checksum anyone can
recompute detects damage and does not make the claim honest. A checkpoint image
is bounded by ``checkpoint_image_default_max_bytes`` (4 GiB) unless the reader
passes its own; a deployment with larger state says so.

The same reasoning applies to a list of rows read by column, which allocates
every row before it has read a field. The count is tested first: the work of
every field of every row is charged, and the bytes present must be at least
what those columns could occupy. The field-wise reader never needed this,
because it built a row only once it had read one.

RFC 0039 checkpoint images introduced it in **format version 3**
(and retain it in version 4): profile, revision,
then one block holding the tables and the body, with the checksum over the
compressed bytes.

Measured -- the RFC 0039 benchmark endpoint, a 100,000-key
``TSD[int, TS[float]]`` (``hgraph_unit_tests '[image-size]'``,
``hgraph_persistence_tests '[checkpoint-benchmark]'``):

.. list-table::
   :header-rows: 1
   :widths: 40 20 20 20

   * - image
     - bytes
     - bytes per key
     - encode / decode
   * - version 2 (field-wise, uncompressed)
     - 1,931,274
     - 19.3
     - 2.9 / 7.6 ms
   * - ``Compact`` 1, uncompressed
     - 1,100,208
     - 11.0
     -
   * - ``Compact`` 1, LZ4
     - 401,025
     - 4.0
     -
   * - ``Compact`` 1, zstd (the stored default)
     - 81,191
     - 0.81
     - 3.7 / 8.1 ms
   * - ``Fast`` 1 (transport)
     - 1,800,207
     - 18.0
     -
   * - hand-coded record
     - 2,400,008
     - 24.0
     - 2.2 / 8.5 ms

That endpoint's keys are consecutive and its values regular, which flatters
both the delta column and zstd; real floating-point state will compress less.
The uncompressed figure is the honest one for the encoding itself: 43% smaller
than version 2, all of it from the keys.

And the value codec alone, field-wise against ``Compact`` 1
(``hgraph_unit_tests '[codec-benchmark]'``):

.. list-table::
   :header-rows: 1
   :widths: 30 24 23 23

   * - shape
     - bytes
     - encode
     - decode
   * - ``list<int>`` x 1,000,000
     - 8,000,003 -> 1,000,004
     - 7.3 -> 7.5 ms
     - 24.0 -> 2.1 ms
   * - ``list<float>`` x 1,000,000
     - 8,000,003 -> 8,000,004
     - 7.5 -> 2.7 ms
     - 24.4 -> 0.21 ms
   * - ``list<row6>`` x 100,000
     - 3,980,003 -> 2,012,807
     - 6.0 -> 4.6 ms
     - 33.6 -> 3.3 ms
   * - ``map<int,float>`` x 100,000
     - 1,700,003 -> 900,006
     - 1.2 -> 1.4 ms
     - 7.0 -> 0.81 ms

The one case that is not smaller is a column of floating point, which costs its
encoding byte: one byte more than today, per column. Raw output for all of the
above is in ``benchmarks/results/rfc0040-stage3-20260918-macos.md``.

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

1. ``hgraph-persistence`` registers ``"binary"`` (``Compact``, in a
   compression block) and ``"binary-fast"`` (``Fast``, never compressed) and
   makes ``"binary"`` the ``ValueStore`` default. ``"json"`` remains registered
   for a store that is *meant* to hold JSON. RFC 0030 is amended. **Done.**

   A compressed object's stored size says nothing about what decoding it
   allocates, so a limit on the stored bytes -- fabric's 16 MiB of metadata --
   would be a limit on nothing. ``ValueCodecOps`` gains an optional
   ``decode_limited``, which a compressing codec must supply, and
   ``BoundValueCodec::decode(encoded, max_decoded_bytes)`` lets the owner of the
   bytes say how large the decoded object may be; fabric passes its limit.
   Without one, ``binary`` holds an object to 1 GiB, and refuses to *write* a
   larger one, so that what is written can always be read.

   This proposal first said that every object records its codec, so existing
   JSON objects would stay readable. That was wrong: a stored object is exactly
   its codec's bytes and says nothing about which codec that was. It does not
   matter -- no store of the old default exists (Howard, 2026-09-18) -- so
   there is nothing to migrate and ``"binary"`` reads binary and nothing else.
2. Fabric's ``metadata_codec`` defaults to the store default: metadata is
   state that is stored. **Done.**

   Its notification codec -- what fabric puts onto Kafka -- is **not** binary.
   This proposal said "Kafka revision payloads default to ``"binary"``", and
   that was wrong (Howard, 2026-09-18): **the binary codecs are for internal
   communication and state storage. Kafka is an external boundary, and what
   hgraph encodes onto it is JSON, Avro or protobuf**, because those are the
   formats the tools around a topic -- consoles, connectors, schema registries,
   other consumers -- rely on. JSON is the one this build provides; Avro and
   protobuf register as store codecs in the same way (RFC 0030). The Kafka
   extension and adaptor themselves carry ``Bytes``: what a user encodes is
   the user's business. A fabric test pins the rule.
3. The version 1 checkpoint reader is the one remaining JSON decoder on a
   serialization path. It is read-only and exists for images that
   hgraph 0.8.25-0.8.27 published.
4. The ratchet lands with step 1: ``json-in-runtime`` and
   ``json-in-persistence-and-fabric`` in ``test_architecture_ratchets.py``.
   The second one's floor names fabric's Kafka codec as a place JSON belongs.
   **Done.**

One existing use stays, by decision (2026-09-18): RFC 0001 writes structured
frame-metadata fields as JSON text. Arrow schema metadata is string-to-string
and is read by Polars, pandas and other tools that know nothing of hgraph, so
this is a property deliberately *represented as JSON* for compatibility with
non-hgraph users -- the case JSON is for -- and not a serialization path.

Compatibility
-------------

The RFC 0017 field-wise encoding is **revision 0** of both profiles, so nothing
already written changes meaning. ``Fast`` is at revision 1 (stage 2) and
``Compact`` at revision 1 (stage 3). ``bind_binary_converter(meta, profile,
revision)`` binds a stated revision, which is what a reader of stored bytes
needs; a revision later than the build writes is refused by number. Every
``Compact`` revision stays readable, because it was stored.

**The functions that name no profile are pinned to revision 0.**
``bind_binary_converter(meta)``, ``to_binary_string`` and ``from_binary_string``
have no frame to record a revision in, so their bytes are the ones they have
always produced and never move when a profile's do. Code that wants a profile's
current form names the profile. (Making them mean "``Compact``, current"
silently changed the bytes under every existing caller, and was caught by tests
that build field-wise bytes by hand.)

RFC 0039 images: version 2 has no profile or revision in its header and is read
as ``Compact`` revision 0; version 3 records both. Real version 2 bytes, written
by the last build that produced them, are pinned in
``tests/cpp/checkpoint_v2_fixture.h``.

``BinaryConverter`` gains one trailing pointer, ``atom_ops`` (stage 4): a
registered wire form has to be reachable from the converter without a lookup,
and a lookup would mean a lock per value. Existing members keep their offsets.
Nothing outside the codec constructs or copies one -- callers hold a
``BoundBinaryConverter``, which is a handle. Otherwise it keeps its shape; its write function now
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
   block compression. **Done** as ``Compact`` revision 1 and image format
   version 3; checkpoint images are written with it. ``ValueStore`` switches
   at stage 5, with the rest of the JSON retirement.
4. Python objects, registered wire forms and the wiring-time refusal. **Done**,
   except the two items marked deferred:

   * ``register_binary_atom(scalar, BinaryAtomOps{write, read, context, opaque})``
     and ``declare_portable_binary_atom(scalar)``. The codec resolves an atom
     in order: built in; registered; declared portable; plain numeric storage.
     A registered form is framed with its length, so one that reads too much
     or too little is caught at that value.
   * A scalar with none of these is a ``BinaryWireFormError`` raised when the
     converter is **bound**. ``BoundaryTransfer`` takes the name of what it is
     wiring (``"dmap_ input 2"``, ``"the output of a spawn_ stage"``) and the error
     carries it, with the whole time-series schema and the fix.
   * The bridge registers **pickle** (protocol 5) as the wire form of its
     object atom, marked ``opaque``, so the type layer stays Python-free. A
     Python error is described while the GIL is still held. Binding a schema
     that names a class used as a type warns, once per schema, naming the class.
   * A boundary whose schema can reach an ``Any`` carries a session: body,
     tables, then a four-byte trailer with the body's length, so the head of a
     payload reads as before. A boundary with a schema writes none of it.
     ``BoundBinaryConverter::needs_session()`` says which, when bound.
   * What is pickled is narrower than "anything behind ``object``". The bridge
     gives such a value a native form where it has one -- a tuple, dict, set or
     scalar -- and the codec writes that. A frozen dataclass has a schema and
     crosses natively with no warning. Only a value with no native form, such
     as an instance of an ordinary class, is pickled. A callable gets the
     native ``callable`` scalar, which has no wire form and is refused by name
     -- at run time, because ``object`` says nothing at wiring about what it
     will hold.
   * A class used as a type that sits *inside* another value -- a tuple or a
     dict of instances behind ``object`` -- puts that class's schema in the
     session's table, so the table has a recipe for it. The bridge names an
     annotation after its identity in the process that registered it
     (``python::module.Class@<id>``), so only that process, an in-process
     worker, has the exact schema; anywhere else the unconstrained ``Any``
     stands in, as does anything built over it, and the identity check that
     other entries get is skipped for those. Nothing a reader needs is lost:
     either is a box holding a Python object, the bytes are the same, and the
     object's pickle says what it is. An ordinary named schema that is missing
     is still an error.

     This does not make a *time-series* schema over such a class portable: a
     checkpoint of a ``TS[SomeClass]`` endpoint names a schema that the process
     recovering it, which registered the class afresh, does not have. That
     goes with the recovery work, and probably wants annotations named without
     a process identity.
   * **Known difference, not introduced here but newly reachable:** behind
     ``object``, a Python *list of numbers* reaches a ``map_`` child as a
     ``list`` and a ``dmap_`` child as a NumPy array. The bridge infers a native
     numeric list; locally the original object is served from the endpoint's
     Python cache, while across a boundary the value is rebuilt from native
     bytes, and a native numeric list converts to an array. Tuples, dicts, sets
     and scalars agree. Left for a decision: it is a property of the bridge's
     list conversion, not of the codec.
   * **Deferred -- ``PickledObject`` pass-through.** No process in the system
     forwards values whose schemas it cannot resolve: a worker is the calling
     program launched again, and a native ``ValueStore`` caller declares its
     own schemas. The length framing above is what would make it possible.
   * **Deferred -- ``PythonOnly`` endpoint capture.** Such an endpoint still
     reports itself ineligible for checkpointing. It belongs with the
     recoverable-component wiring (RFC 0039 stages 3 to 5), which is where a
     component's endpoints are decided.
5. JSON retirement: codec registration and defaults, Fabric, the ratchet,
   RFC 0030 amendment. **Done**, ahead of stage 4: the binary codec already
   covers every schema the JSON codec does, so nothing here waits on Python
   objects.

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
