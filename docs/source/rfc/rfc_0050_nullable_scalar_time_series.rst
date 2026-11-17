RFC 0050: Nullable scalar time series
=====================================

:Status: Proposed
:Created: 2026-11-17
:Target: Value types, the Python bridge and HGL publications

Summary and motivation
----------------------

A time series of a scalar ``T`` can tick only a ``T``. "No value" is said by
not ticking, and a reader sees it as validity. There is no tick whose value
is null: one that wakes its consumers, is valid, and reads as nothing.

HGL has that tick. A node whose result is ``i64?`` publishes a null with
``return null`` and stays silent with a bare ``return``, and the two are
different observations. Today ``TS[Optional[int]]`` is taken as ``TS[int]``:
the ``None`` of the union is dropped where the value type is made, a node
that returns ``None`` does not tick, and ``output.value = None`` invalidates.
Part 1 of issue #1797 asks for the missing tick.

This RFC adds one value type, ``Optional[T]``, and nothing to the
time-series layer.

Ownership
---------

``Optional[T]`` is a generic core value type with a first-class C++ contract
and matching Python exposure. The HGL compiler consumes it; core does not
depend on the compiler. There is no downstream implementation to retain or
migrate.

Contract
--------

``TypeRegistry::optional(payload)`` interns the value type ``Optional[T]``
for a payload value type ``T``. A value of it is always a live value: it
either holds one ``T`` or is unset. ``payload`` is any value type other than
``any``, which already holds nil, and another ``Optional``.

Equality, ordering and hashing are the payload's, with field validity's
existing rule for the unset state: unset equals unset and orders before set.
No arithmetic or other operator is defined on ``Optional[T]``. Operators that
take any scalar (pass-through, ``const``, equality, selection, collection of
values) take it as they take any other value.

In C++ a signature spells it ``Optional<T>``, beside ``Owned<T>`` and
``FixedTuple<...>`` in ``static_schema.h``:
``In<"x", TS<Optional<Int>>>`` and ``Out<TS<Optional<Int>>>``. A node reads
it through an optional view of the value, which says whether a payload is
held and gives it, and writes a held payload or the unset state. The exact
view and builder names follow the existing ``as_bundle`` and ``as_list``
family and are fixed by the implementation's first commit.

In Python, ``TS[Optional[T]]`` and ``TS[T | None]`` are the time series of
``Optional[T]``. A sentinel ``Null`` stands beside ``REMOVE`` in
``hgraph._wiring._sentinels``, is exported from ``hgraph``, and is handed to
the bridge at import as the existing sentinels are. It is a singleton
compared by identity.

.. code-block:: python

   @compute_node
   def cleared(command: TS[int]) -> TS[Optional[int]]:
       if command.value == 0:
           return Null            # a tick whose value is null
       if command.value < 0:
           return None            # no tick, as today
       return command.value       # a tick that holds the value

+-----------------------------------+---------------------------------------+
| Python                            | Meaning                               |
+===================================+=======================================+
| A node returns ``None``           | No tick, on every output, as today    |
+-----------------------------------+---------------------------------------+
| A node returns ``Null``           | On an ``Optional`` output, a tick     |
|                                   | whose value is unset. On any other    |
|                                   | output, ``TypeError``                 |
+-----------------------------------+---------------------------------------+
| A node returns a ``T``            | On an ``Optional[T]`` output, a tick  |
|                                   | that holds it, with no wrapper        |
+-----------------------------------+---------------------------------------+
| ``input.value``                   | ``None`` for a tick whose value is    |
|                                   | unset, with ``input.valid`` true. The |
|                                   | payload otherwise                     |
+-----------------------------------+---------------------------------------+
| ``input.delta_value``             | ``Null`` for a tick whose value is    |
|                                   | unset, the payload for one that holds |
|                                   | it, ``None`` when not modified        |
+-----------------------------------+---------------------------------------+
| ``eval_node`` vectors and results | ``None`` is no tick, as today.        |
|                                   | ``Null`` is a tick whose value is     |
|                                   | unset                                 |
+-----------------------------------+---------------------------------------+
| ``output.value = Null``           | Publishes the unset value.            |
|                                   | ``output.value = None`` invalidates,  |
|                                   | as today                              |
+-----------------------------------+---------------------------------------+

A ``TS[T]`` source bound to a ``TS[Optional[T]]`` input is accepted: binding
adds the adaptation that publishes each tick as a held payload. The reverse
is a wiring error.

Only the scalar directly under ``TS[...]`` changes. An ``Optional[X]`` field
of a ``CompoundScalar`` stays a field of ``X`` that may be unset, and an
``Optional`` inside a scalar container keeps its present reading.

Representation
--------------

``Optional[T]`` is a composite of one payload with the validity word a
bundle or a fixed tuple already carries for its fields: the payload's
storage, followed by the word. It is marked by a value-type flag and named
``Optional[T]``, which joins the type-name grammar of RFC 0042 and the wire
descriptor of RFC 0017.

"Null is a state, not a schema or type" still holds. The unset state is the
payload's validity bit inside a live value, as an unset bundle field is. So
nothing in the time-series layer changes:

* validity stays the last modified time, and a tick whose value is unset is
  valid like any other;
* the atomic copy and move paths receive a live source;
* a typed-null delta still means no effect, an unset element of a replay or
  record buffer still means no tick, and an empty ``any`` still holds nil.

Compatibility and serialization
-------------------------------

``TS[Optional[T]]`` stops being ``TS[T]``. No test in the repository pins the
present reading. A node that declares one keeps accepting ``TS[T]`` sources
through the binding adaptation, and reads ``None`` only for a tick whose
value is unset, which no source produces until one returns ``Null``.

RFC 0004's rule stands: ``None`` does not form a value tick.

The binary codec writes a presence tag and then the payload, as it does for
an indirect value. JSON writes ``null`` for the unset state and the payload's
form otherwise. The time-series JSON operators keep their present reading of
``null`` in a ``TSL`` (no tick) and a ``TSD`` (a key erased).

Table recording reads an Arrow null cell as no tick. Recording an
``Optional`` leaf therefore needs the presence column of RFC 0019 to say that
the row ticked, with a null payload for the unset state. That is left to a
later change; until then a table of an ``Optional`` leaf is refused at wiring
with a named error.

The runtime specification gains the value kind and its cases in a linked
change to ``hgraph_spec``.

Performance
-----------

No existing path changes. A tick of ``Optional[T]`` costs the payload's copy
and one validity-word write, with no allocation for a payload that needs
none. The Python fast compute path stays for the atomic kinds it covers
today; an ``Optional`` output uses the erased apply.

Alternatives considered
-----------------------

* A "valid but null" state in atomic time-series data. It touches every
  atomic read and write, and collides with the meanings a missing value
  already has: a typed-null delta is no effect, an unset buffer element is
  no tick, an Arrow null cell is no tick.
* ``TS[any]`` holding nil. The runtime specification gives it a nil tick
  (SCALAR-NIL), and it loses the payload's type.
* ``Owned[T]``. It is shaped for bundles and allocates.
* A new spelling, ``TS[Nullable[T]]``, leaving ``Optional`` dropped. Issue
  #1797 chose ``Optional``: it is what a reader expects it to mean.

Unresolved questions
--------------------

1. Binding ``TS[T]`` to a ``TS[Optional[T]]`` input: the adaptation above, or
   a wiring error that asks for an explicit conversion. Proposed: the
   adaptation.
2. The payload: any value type but ``any`` and ``Optional``, or leaf scalars
   only to begin with. Proposed: any value type.
3. Table recording of an ``Optional`` leaf: in this RFC's implementation, or
   the later change above. Proposed: later.

Acceptance criteria and test plan
---------------------------------

* C++: the registry interns and names ``Optional[T]`` and reads the name
  back; the value compares, orders, hashes and copies in both states; a
  native node publishes a held payload, the unset state and no tick, and
  ``eval_node`` tells the three apart; the binary and JSON codecs round-trip
  both states; an installed-SDK consumer builds a node that uses
  ``Optional<T>``.
* Python: the node above; ``Null`` refused on an output that is not
  ``Optional``; ``value`` and ``delta_value`` in the three cases;
  ``eval_node`` vectors and results; a ``TS[T]`` source bound to an
  ``Optional`` input; ``CompoundScalar`` optional fields unchanged.
* The developer guide's nullability, value-kind and bridge pages, and the
  user guide's statement that returning ``None`` does not tick.
* The complete C++ and Python gates.
