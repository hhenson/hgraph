RFC 0045: Zoned wall-clock time scalar
======================================

:Status: Proposed, implementation under review
:Created: 2026-10-05
:Target: Native scalar identity and HGL publications

Contract and ownership
----------------------

``ZonedTime`` is an immutable passive value containing ``CivilTime`` and
``ZoneId``. It names a wall-clock time in a named zone, with no date, instant,
offset or fold/gap policy. Its registered scalar name is ``zoned_time``.
This extends RFC 0002's domain-independent temporal vocabulary for ordinary
scalar transport. The HGL compiler consumes this native scalar; core does not
depend on the compiler. There is no downstream runtime implementation to retain
or migrate.

The public C++ constructor ``ZonedTime(CivilTime, ZoneId)`` checks that the time
is in the half-open interval from midnight to 24 hours and that the zone handle
is valid. ``time()`` and ``zone()`` return the two components. Equality and
hashing include both components and preserve exact zone spelling, including
links. No ordering, arithmetic or date-resolution operator is introduced.
Default construction creates only the empty-zone storage sentinel, as with
``ZonedDateTime``; it does not represent a valid public temporal literal.

Python exposes the same native value as ``hgraph.ZonedTime(time, zone)`` with
read-only ``time`` and ``zone`` properties, equality, hashing and representation.
``time`` uses the existing naive ``datetime.time`` conversion and ``zone`` uses
``ZoneId``. Python does not implement another temporal runtime.

Construction and transport
--------------------------

HGL literal construction validates exact provider membership at its cold
materialization boundary. It neither resolves a civil date nor requests an
offset. Every supplied eval expression is validated in written order before
target start. Provider-dependent defaults and generated runtime hooks follow
the existing temporal literal restrictions. Retention, replay, record and
ordinary comparison copy the prepared scalar without provider work.

The JSON representation is a string containing civil time followed by the
bracketed exact zone name. Strict decoding validates time, zone syntax and
provider membership without resolution. The internal binary codec transports
microseconds from midnight and the exact name, never a process-local handle;
decode re-interns the name without re-resolving it. Other temporal wire formats
retain their existing identifiers and bytes. This additive scalar does not
change existing C++ layouts or semantics. Separately built consumers compile
against the updated installed SDK.

HGL admits this leaf at top level and recursively through its existing finite
structural and atomic publication profile. ``delta<zoned_time>`` and
``atomic<zoned_time>`` normalize to ``zoned_time``. Existing collection key,
set-element, optional-field and recursive-shape restrictions remain intact.

Representation and validation
-----------------------------

The value is a trivially copyable, standard-layout pair of at most sixteen
bytes. Publication adds no allocation, locking, registry lookup or provider
dispatch. Native wiring tests verify equal repeated publications and owning
retention; compiler tests exercise direct and generated paths, invalid cold
names, defaults and recursive payloads. Python tests prove wrapper and wiring
parity; binary/JSON tests prove exact identity. Installed-consumer coverage
proves the first-class public C++ contract. Complete native and Python
compatibility gates remain required.

Storing an offset was rejected because it is undefined without a date. Reusing
``ZonedDateTime`` would introduce an invented date and instant. Date resolution
and broader zone arithmetic remain separate future proposals.
