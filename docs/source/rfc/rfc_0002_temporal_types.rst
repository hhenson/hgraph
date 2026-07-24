RFC 0002: Temporal Types, Zones, and Ranges
===========================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-07-23
:Target: Incremental core foundation

Summary
-------

Define compact, immutable value types that distinguish:

* absolute engine time from elapsed time;
* Gregorian civil fields from absolute time;
* fixed elapsed durations from calendar-relative periods;
* a named-zone identifier from a resolved zoned value; and
* a time point from a bounded or unbounded time range.

The values are native C++ scalar types with equivalent Python authoring
surfaces.  Their hot-path operations are fixed-size, allocation-free, and do
not consult a time-zone database.  Operations that interpret civil fields in a
named zone use an explicit, versioned provider from the graph's
``GlobalState`` and require explicit policies for folds and gaps.

Domain-specific calendars, trading sessions, holidays, day-count conventions,
and business-day iteration remain extension concerns.

Motivation
----------

The existing ``DateTime`` and ``TimeDelta`` values are sufficient for engine
scheduling, but they do not describe several different meanings that trading
and data systems must keep separate:

* ``09:30`` in New York is a civil value which needs a date, zone, and
  resolution policy before it identifies an instant.
* Twenty-four elapsed hours and one civil day can produce different local
  times across a daylight-saving transition.
* One month has no fixed elapsed duration.
* A UTC offset records one resolution at one point in time; it is not a named
  zone and does not contain future transition rules.
* A range needs boundary semantics before containment, intersection, and
  partitioning are deterministic.

Collapsing these concepts into a naïve ``datetime`` transfers policy into
call sites, makes replay dependent on the host, and encourages per-tick Python
objects and time-zone lookups.  The core needs a small vocabulary that makes
the distinctions visible to C++ overload resolution, Python wiring, schema
reflection, persistence, and operator dispatch.

Design principles
-----------------

The normative design follows these rules:

* **One timeline precision.** All sub-day values use signed 64-bit
  microseconds.  This matches the engine, Python ``datetime``, and Arrow
  interoperability requirements without the 2262 limit of signed 64-bit
  nanoseconds.
* **POSIX timeline.** ``Instant`` counts microseconds since the Unix epoch and
  does not represent leap seconds.  No operation silently changes time scale.
* **Proleptic Gregorian civil calendar.** The portable civil domain is
  ``0001-01-01`` through ``9999-12-31``.  Other calendar systems belong in
  extensions.
* **No implicit local zone.** A process, operating-system, or Python local
  zone is never inferred.
* **Policy is explicit.** Ambiguous and nonexistent local times reject by
  default.  Calendar month-end adjustment also rejects by default.
* **Values are immutable.** A value is safe to hash, use as a TSD key, retain
  across ticks, and share between C++ and Python.
* **Representation is not serialization.** Native layouts are compact but
  are never persisted with ``memcpy``.  Interchange encodings are
  schema-directed and versioned.
* **Resolution is separate from representation.** Comparing, hashing, and
  copying an already resolved value never queries a time-zone provider.

Terminology and time scale
--------------------------

``Instant``
   A point on the POSIX/Unix timeline.  It is independent of any civil zone.

``Duration``
   A signed fixed number of elapsed microseconds.

``Civil``
   Gregorian calendar or clock fields without a UTC offset or named zone.
   "Civil" is used instead of "local" because no machine-local zone is
   implied.

``Period``
   A signed calendar-relative quantity of months and days.  Its elapsed
   duration depends on the value to which it is applied.

``ZoneId``
   A case-sensitive structured identifier, normally an IANA TZDB name such as
   ``America/New_York``.  It is not itself a set of transition rules.

``ZonedDateTime``
   A resolved instant, the named-zone intent, and the UTC offset that was used
   for that resolution.

``TimeRange[T]``
   A normalized set of values between two optional ordered endpoints.

The timeline follows the same practical model as
``std::chrono::system_clock`` and RFC 3339: leap seconds are not represented as
distinct ``Instant`` values.  Historical civil accuracy is limited by the
selected TZDB data and must not be described as more authoritative than that
data.

Scope and ownership
-------------------

``hg_cpp`` owns:

* the scalar schemas and native/Python value contracts in this RFC;
* checked timeline graph operations and named checked C++ functions, plus
  checked Gregorian civil arithmetic;
* the normalized range algebra;
* the provider interface used to resolve named zones;
* standard operator overloads and dispatch rules; and
* canonical text, JSON, and Arrow mappings.

Downstream libraries own:

* exchange and jurisdiction business dates;
* holiday and trading-session calendars;
* business-day and session iteration;
* day-count and roll conventions;
* bar definitions tied to a trading calendar; and
* policy selecting a business calendar for a strategy.

Core iteration may step an ``InstantRange`` by a positive ``Duration`` or a
``CivilDateRange`` by Gregorian days.  "Next trading day", "previous fixing
day", and similar operations require an extension-provided calendar.

Type model
----------

The public concepts are:

``Instant`` and ``Duration``
   UTC timeline values.  Existing ``DateTime`` and ``TimeDelta`` remain source
   and schema aliases for compatibility.

``CivilDate``, ``CivilTime``, and ``CivilDateTime``
   Valid Gregorian fields with no offset or zone.  Existing ``Date`` and
   ``Time`` remain aliases for the first two.

``Period``
   Canonical total months plus days.  Years are an input and display
   decomposition of total months, not an independent equality component.

``ZoneId``
   A validated and interned name handle.  Provider lookup establishes whether
   that name exists in a particular TZDB version.

``ZonedDateTime``
   An ``Instant``, ``ZoneId``, and resolved offset in seconds.

``TimeRange[T]``
   A normalized empty, finite, half-bounded, or unbounded range.  Concrete
   scalar schemas are registered for supported endpoint types, initially
   ``InstantRange`` and ``CivilDateRange``.

``FixedRangeSet[T, Capacity]``
   A normalized, allocation-free sequence of at most ``Capacity`` disjoint
   ranges.  Binary range union and difference use capacity two.

Scalar invariants
-----------------

``Instant``
~~~~~~~~~~~

The canonical payload is a signed 64-bit count of microseconds since
``1970-01-01T00:00:00Z``.  The scalar representation may cover the complete
signed range.  Engine scheduling continues to enforce the narrower existing
``MIN_ST``/``MAX_ET`` domain; a storable historical value is not automatically
a valid engine evaluation time.

Supported operations are:

* total ordering, equality, and hashing by microsecond count;
* ``Instant + Duration -> Instant``;
* ``Instant - Duration -> Instant``; and
* ``Instant - Instant -> Duration``.

Adding two instants is ill-formed at wiring time.  Hgraph graph operators and
the named C++ functions ``checked_add`` and ``checked_subtract`` check
overflow and never wrap.

``Instant`` remains an alias of the existing ``DateTime``
``std::chrono::time_point`` for source, schema, and engine compatibility.
Consequently, an ordinary C++ expression using the raw ``std::chrono``
``operator+`` or ``operator-`` has the standard library's arithmetic contract;
hgraph cannot replace those operators through an alias.  C++ callers requiring
the checked contract use the named functions.  Hgraph's temporal operator
implementations must use those functions rather than raw chrono arithmetic.

``Duration``
~~~~~~~~~~~~

The canonical payload is a signed 64-bit microsecond count.  It supports
equality, total ordering, hashing, addition and subtraction, unary negation,
multiplication or division by a numeric scalar, and the existing
duration-to-duration ratio operations.  Hgraph graph operators and the named
``checked_*`` C++ functions check overflow.  As with ``Instant``, raw
``std::chrono::duration`` operators retain their standard-library contract
because ``Duration`` remains the ``TimeDelta`` alias.  Integer scaling through
hgraph is exact and checked.  Floating-point scaling follows Python
``timedelta`` compatibility by rounding to the nearest microsecond with ties
to even.  Division by zero rejects.

``Duration`` is not implicitly convertible to or comparable with ``Period``.
A day expressed as ``Duration`` is exactly 86,400 elapsed seconds; it is not a
civil-calendar day.

Civil values
~~~~~~~~~~~~

``CivilDate`` uses the proleptic Gregorian calendar and accepts only valid
dates in the portable range ``0001-01-01`` through ``9999-12-31``.  Its
canonical interchange value is a signed count of days from the Unix epoch.

``CivilTime`` is a signed 64-bit count of microseconds since midnight with the
invariant:

.. code-block:: text

   0 <= microseconds < 86_400_000_000

``24:00:00`` and ``23:59:60`` are not values of this type.

``CivilDateTime`` is a strong type whose canonical payload is a signed 64-bit
count of local microseconds from the civil Unix epoch.  "Local" in this
sentence identifies the arithmetic coordinate only; it does not attach UTC or
any zone.  Construction and access expose date and time fields, but the stored
form avoids padding and repeated field conversion in time-series storage.

Civil values support equality, lexicographic/chronological civil ordering,
hashing, field access, and the following operations:

* ``CivilDate + days -> CivilDate``;
* ``CivilDate - CivilDate -> Duration`` containing a whole number of days;
* ``CivilDate + CivilTime -> CivilDateTime``;
* ``CivilDateTime +/- Duration -> CivilDateTime``; and
* ``CivilDateTime - CivilDateTime -> Duration``.

Adding a sub-day ``Duration`` directly to ``CivilDate`` is rejected rather
than silently truncating.  Adding a duration to ``CivilTime`` alone is not a
core operator because crossing midnight needs a date or an explicit
day-carry result.

``Period``
~~~~~~~~~~

The canonical representation is:

.. code-block:: cpp

   struct PeriodPayload {
       std::int32_t total_months;
       std::int32_t days;
   };

Construction accepts ``years``, ``months``, and ``days`` and checks
``years * 12 + months`` for overflow.  Equality and hashing use
``total_months`` and ``days``, so ``Period{years=1}`` equals
``Period{months=12}``.  Accessors may present normalized years and a remainder
whose absolute value is less than 12.  Mixed-sign months and days are valid.

Periods support equality, hashing, checked addition and subtraction, unary
negation, and multiplication by an integer.  They have no natural total order:
without an anchor, one month cannot be ordered against thirty days.

Applying a period to a civil date performs month adjustment first and day
adjustment second.  Month adjustment requires a wiring-time
``MonthEndPolicy``:

``reject``
   Reject a target year/month in which the original day does not exist.

``clamp``
   Select the last valid day of the target month.

``preserve_end_of_month``
   If the source is the last day of its month, select the last day of the
   target month; otherwise use ``clamp``.

Rejection is the default.  Period arithmetic is defined for ``CivilDate`` and
``CivilDateTime``.  There is deliberately no ``Instant + Period`` and no
implicit ``ZonedDateTime + Period``.  Zoned calendar arithmetic is expressed
as an explicit three-step graph:

#. convert to ``CivilDateTime`` in a selected zone;
#. apply the period with a month-end policy; and
#. resolve the result with explicit fold and gap policies.

This makes the potentially surprising daylight-saving resolution visible at
the wiring boundary.

``ZoneId`` and the zone-name registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ZoneId`` stores an eight-byte process-local checked intern handle, not an
owning ``std::string``:

.. code-block:: cpp

   struct ZoneIdPayload {
       std::uint32_t slot;
       std::uint16_t generation;
       std::uint16_t name_tag;
   };

``slot`` indexes the core process-wide registry.  ``generation`` rejects a
handle retained across a test reset or future slot recycling.  ``name_tag`` is
the low 16 bits of a stable 64-bit FNV-1a hash of the validated ASCII name,
with zero remapped to one.  It is a diagnostic corruption/mismatched-registry
check, not a uniqueness or security boundary; the registry record always
compares the complete name when interning.

The all-zero payload is an invalid/default sentinel and is never emitted as a
valid time-series value.  Valid slots and generations start at one.  The
production registry is append-only for the life of the process, so generation
does not change during ordinary operation.  A reset or recycled slot
increments the generation and a slot is retired rather than allowing
generation to wrap.  Lookup validates all three fields before returning the
name.

Names are case-sensitive ASCII strings using the TZDB identifier vocabulary.
Validation rejects:

* empty names and names longer than 255 bytes;
* leading, trailing, or repeated ``/``;
* ``.`` or ``..`` path components;
* NUL, backslash, control characters, and non-ASCII bytes; and
* characters outside ASCII letters, digits, ``.``, ``_``, ``-``, ``+``, and
  ``/``.

The syntactic registry does not silently canonicalize TZDB links.  If
``US/Eastern`` and ``America/New_York`` are both accepted by a provider, the
two ``ZoneId`` values preserve the names supplied by the caller.  Equality and
hashing are by exact interned name.

Syntactic validation and provider validation are separate.  This permits a
value to be decoded and reported even when the active provider lacks a newer
name, while any operation requiring transition rules fails clearly.

The intern registry is analogous to the core type registry: it owns identity,
not graph-specific configuration.  Time-zone rules and TZDB version selection
belong to a provider in ``GlobalState``.

The packed payload participates in native equality and hashing.  Within the
active production registry this is equivalent to equality and hashing by the
exact interned name because one name has exactly one live handle.  Raw handles
are never serialized or accepted across processes: interchange writes the
name and the receiving process interns it again.  The registry itself must be
owned by the core shared library so extensions cannot accidentally create a
second registry.

``ZonedDateTime``
~~~~~~~~~~~~~~~~~

The canonical payload is:

.. code-block:: cpp

   struct ZonedDateTimePayload {
       std::int64_t instant_microseconds;
       ZoneIdPayload zone_id;
       std::int32_t offset_seconds;
   };

The offset is stored because it distinguishes folds, permits local-field
reconstruction without a provider lookup, and detects changes or
inconsistencies when data is read under another TZDB version.

Construction paths are deliberately distinct:

``at_zone(Instant, ZoneId, provider)``
   Resolve the offset for an unambiguous timeline instant.

``resolve(CivilDateTime, ZoneId, policies, provider)``
   Resolve zero, one, or two candidate instants using explicit policies.

``from_resolved(Instant, ZoneId, offset)``
   Construct a structurally valid value from trusted serialized data.  Strict
   decoding additionally verifies the offset with the selected provider.

``ZonedDateTime`` equality and hashing are structural and include instant,
zone identity, and resolved offset.  Values for the same instant in two zones
are therefore not equal.  Timeline equivalence is provided explicitly by
``same_instant``.  There is no default relational ordering; callers select
timeline ordering or civil ordering explicitly.

Adding a ``Duration`` is timeline arithmetic: shift the instant, retain the
zone, and resolve the offset at the new instant.  It therefore requires a
provider but no fold/gap policy.  Calendar-period arithmetic follows the
explicit civil pipeline described above.

Time-zone provider and resolution
---------------------------------

The core exposes a pure-C++ provider boundary conceptually equivalent to:

.. code-block:: cpp

   class TimeZoneProvider {
   public:
       std::string_view version() const noexcept;
       bool contains(ZoneId) const noexcept;
       OffsetInfo at(Instant, ZoneId) const;
       LocalResolution resolve(CivilDateTime, ZoneId) const;
   };

``LocalResolution`` reports one of:

* ``unique`` with one instant and offset;
* ``ambiguous`` with the earlier and later instant/offset pairs; or
* ``nonexistent`` with the last representable value before the gap and the
  first representable value after the gap.

Resolution policies are:

``AmbiguousTimePolicy::Reject``
   Reject a fold.

``AmbiguousTimePolicy::Earliest`` / ``Latest``
   Select the candidate with the smaller/larger ``Instant`` respectively.

``NonexistentTimePolicy::Reject``
   Reject a gap.

``NonexistentTimePolicy::NextValid``
   Select the first representable civil microsecond after the gap.

``NonexistentTimePolicy::PreviousValid``
   Select the last representable civil microsecond before the gap.

These gap policies do not mean "shift by the gap length".  If that behaviour
is later required it must have a separately named policy.

The provider is immutable for one graph run, is supplied through
``GlobalState``, and reports its TZDB version.  A replay requiring exact civil
resolution supplies the recorded provider/version.  Updating TZDB rules does
not mutate a running graph.

The initial backend-selection policy is:

``StdChronoTimeZoneProvider``
   Preferred when the selected C++ standard library supplies the required
   C++20 TZDB surface.  The adapter uses ``std::chrono::get_tzdb()``,
   ``locate_zone()``, and ``time_zone::get_info()`` for system and local time,
   and reports ``tzdb::version``.  Backend selection uses a CMake compile-and-
   link probe, a native runtime smoke test when not cross-compiling, and the
   provider conformance suite; it does not trust the ``__cpp_lib_chrono``
   feature macro alone.  This is necessary because calendar and time-zone
   implementation coverage differs between libstdc++, libc++, and the
   Microsoft STL.

``DateTzTimeZoneProvider``
   Fallback using Howard Hinnant's ``date/tz`` library.  It is a complete
   pure-C++ IANA TZDB parser, is the direct precursor of the standardized
   C++20 chrono API, and remains independently packaged and maintained.  The
   initial implementation pins release ``v3.0.4`` rather than following its
   default branch; later updates use the normal dependency-review process.
   When ``date/tz`` reads an operating-system TZDB that does not materialize
   IANA links as files, the adapter resolves the link records from the
   installed ``tzdata.zi`` while preserving the caller's original ``ZoneId``.

The ``HGRAPH_TIME_ZONE_BACKEND`` CMake cache variable accepts ``auto``,
``std``, and ``date``.  ``auto`` selects the standard provider only when its
probe succeeds and otherwise selects ``date``.  The build looks for a CMake
package for ``date/tz`` first; an explicitly enabled, version-pinned
``FetchContent`` fallback may supply it when no package is available.  Both
backends are exercised by the same conformance vectors.  A build that cannot
provide either backend fails configuration rather than silently disabling
named zones.

The standard provider normally reads the host's current TZDB.  A replay or
deployment which requests a specific data set/version not selectable by that
implementation is built with ``HGRAPH_TIME_ZONE_BACKEND=date`` and configures
the provider with the explicitly installed, pinned IANA data.  No provider
downloads or reloads data implicitly during a graph run.  Thus use of the
standard library is preferred, but it does not override deterministic replay.

Every backend must:

* produce the same results on supported platforms for a pinned data set;
* expose the data version;
* support second-resolution historical offsets;
* avoid Python in the C++ runtime path; and
* pass the same provider conformance vectors.

Range model
-----------

Shape and normalization
~~~~~~~~~~~~~~~~~~~~~~~

``TimeRange<T>`` is available when ``T`` is an immutable totally ordered
scalar.  The value consists of two endpoint payloads and one flag byte.  The
flags encode:

* empty;
* lower unbounded;
* upper unbounded;
* lower closed; and
* upper closed.

Unbounded endpoints are always open.  Ignored endpoint storage is zeroed
during normalization, so equality and hashing never inspect indeterminate
bytes.

The default range is the canonical empty range.  Factory operations construct:

* ``empty()``;
* ``all()``;
* ``bounded(start, end, lower_boundary, upper_boundary)``;
* ``from(start, lower_boundary)``; and
* ``until(end, upper_boundary)``.

The ordinary bounded constructor defaults to ``[start, end)``.  This permits
adjacent bars or replay partitions to share a coordinate without double
counting it.

Construction enforces ``start <= end``.  When ``start == end``:

* ``[t, t]`` is the singleton containing ``t``; and
* every other boundary combination normalizes to the canonical empty range.

All empty results compare and hash equally regardless of the operation that
produced them.

Range operations
~~~~~~~~~~~~~~~~

The initial algebra is:

``empty`` / ``bounded`` / ``lower_bounded`` / ``upper_bounded``
   Structural queries.

``contains(value)``
   Membership with both boundary flags honoured.

``contains(range)``
   Set containment.  The empty range is contained by every range.

``intersection(other)``
   Return a normalized ``TimeRange``.  Disjoint inputs return the canonical
   empty range, not ``None``.

``overlaps(other)``
   True exactly when the intersection is non-empty.

``touches(other)``
   True when a finite upper and lower endpoint share the same coordinate,
   independent of boundary inclusion.

``adjacent(other)``
   True when the ranges do not overlap and their union is a single range under
   the boundary model.  At a shared endpoint this requires exactly one side
   to include the point.  Two closed sides overlap; two open sides leave a
   hole.

``mergeable(other)``
   True when the ranges overlap or are adjacent.

``merge(other)``
   Return the normalized union when mergeable, otherwise no value.

``hull(other)``
   Return the smallest range containing both inputs even when it spans a gap.

``difference(other)``
   Return ``FixedRangeSet<T, 2>`` containing zero, one, or two normalized
   ranges.

``set_union(other)``
   Return ``FixedRangeSet<T, 2>``.  Mergeable inputs produce one range;
   separated inputs produce two; two empty inputs produce none.

``shift(delta)``
   Shift both finite endpoints by the compatible ``Duration`` or Gregorian
   period operation, with checked overflow.

``extent()``
   For a bounded ``InstantRange``, return ``end - start``.  Boundary flags do
   not alter elapsed extent.  Unbounded ranges have no extent.

``adjacent`` uses the endpoint topology above and does not infer a sampling
grid.  A caller that wants ``[d1, d2]`` to be adjacent to
``[d2 + one_day, d3]`` supplies a one-day grid or normalizes both ranges to
half-open form.  Business-day adjacency remains a calendar operation.

Ordering of ranges, when required for sorting, is structural over the
normalized form: empty first, then lower endpoint with unbounded first, lower
closure, upper endpoint with unbounded last, and upper closure.  At the same
finite lower coordinate, closed sorts before open; at the same finite upper
coordinate, open sorts before closed.  It is not a set-inclusion ordering.

Fixed-capacity range result
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The concrete native return container for binary range algebra is:

.. code-block:: cpp

   template <std::totally_ordered T, std::size_t Capacity>
   class FixedRangeSet {
       std::array<TimeRange<T>, Capacity> ranges_{};
       std::uint8_t size_{};

   public:
       static constexpr std::size_t capacity() noexcept;
       constexpr std::size_t size() const noexcept;
       constexpr bool empty() const noexcept;
       constexpr const TimeRange<T>& operator[](std::size_t) const;
       constexpr auto begin() const noexcept;
       constexpr auto end() const noexcept;
   };

``Capacity`` is in the range 1--255.  Construction is through the range
algebra and normalization factories; there is no public mutating
``push_back``.  The active prefix contains only non-empty ranges, sorted by
the structural range ordering, pairwise disjoint, and non-adjacent (adjacent
ranges are coalesced).  Unused slots contain the canonical empty range so
equality, hashing, diagnostics, and sanitizers never observe indeterminate
storage.

Two input ranges can produce no more than two components under union or
difference, so ``FixedRangeSet<T, 2>`` cannot overflow for the operations in
this RFC.  A future operation whose mathematical result can exceed its
capacity must use a different compile-time capacity or return an explicit
capacity error; it must not truncate.  The type is standard-layout and
trivially copyable whenever ``TimeRange<T>`` is, returns by value, and performs
no allocation.

The initial registered hgraph scalar schemas are ``InstantRangeSet`` and
``CivilDateRangeSet``, aliases of the corresponding
``FixedRangeSet<T, 2>`` specializations.  The generic C++ template is not
automatically a scalar schema.  Python exposes each concrete result as an
immutable sequence with ``len``, iteration, indexing, equality, and hashing;
``tuple(result)`` is available for ordinary Python consumption without making
a dynamic tuple the native time-series representation.

C++ public contract
-------------------

The intended C++ shape is:

.. code-block:: cpp

   using Instant = DateTime;
   using Duration = TimeDelta;
   using CivilDate = Date;
   using CivilTime = Time;

   class CivilDateTime;             // one int64 local-microsecond payload
   class Period;                    // int32 total_months + int32 days
   class ZoneId;                    // 8-byte checked intern handle
   class ZonedDateTime;             // at most 24-byte resolved payload

   enum class MonthEndPolicy : std::uint8_t;
   enum class AmbiguousTimePolicy : std::uint8_t;
   enum class NonexistentTimePolicy : std::uint8_t;
   enum class Boundary : std::uint8_t;

   template <std::totally_ordered T>
   class TimeRange;

   template <std::totally_ordered T, std::size_t Capacity>
   class FixedRangeSet;

   using InstantRange = TimeRange<Instant>;
   using CivilDateRange = TimeRange<CivilDate>;
   using InstantRangeSet = FixedRangeSet<Instant, 2>;
   using CivilDateRangeSet = FixedRangeSet<CivilDate, 2>;

The classes expose value accessors, named factories, equality/hash behaviour,
and free functions for the algebra above.  Fields are not publicly mutable.
The public API does not expose a backend-specific zone pointer or a Python
object.

Because ``Instant`` and ``Duration`` deliberately preserve their existing
chrono aliases, their checked C++ arithmetic surface is expressed by
``checked_add``, ``checked_subtract``, ``checked_negate``, ``checked_multiply``,
and ``checked_divide``.  Native graph operator implementations call this
surface.  Raw chrono operators are compatibility operations and are not
advertised as overflow-checking APIs.

``TimeRange<T>`` is a C++ template, but only explicitly registered concrete
endpoint types are hgraph scalar schemas.  A downstream scalar may register
its own concrete range only when it provides the same total-order, hash,
codec, and checked-shift contracts.

Python public contract
----------------------

Python authoring uses:

* built-in ``datetime`` for ``Instant``/``DateTime`` compatibility;
* built-in ``timedelta`` for ``Duration``/``TimeDelta``;
* built-in ``date`` and naïve ``time`` for ``CivilDate`` and ``CivilTime``;
* immutable ``hgraph.CivilDateTime``;
* immutable ``hgraph.Period``;
* immutable ``hgraph.ZoneId``;
* immutable ``hgraph.ZonedDateTime``;
* immutable ``hgraph.InstantRange``;
* immutable ``hgraph.CivilDateRange``;
* immutable ``hgraph.InstantRangeSet``; and
* immutable ``hgraph.CivilDateRangeSet``.

Every class is hashable when its C++ scalar is hashable.  Python equality and
range normalization match C++ exactly.  Python wrappers must not expose
writable fields that can invalidate a value after it has been hashed or
published on a time series.

Python value code, including code executing inside a ``compute_node``, uses
``hgraph.temporal`` for the direct-value form of the temporal graph operators.
The module is a thin binding to the same native C++ functions used by the
operator implementations; it is not a second Python implementation.
``CivilDateTime`` and ``Period`` additionally expose their natural checked
arithmetic through Python's ``+``, ``-``, unary ``-``, and integer ``*``
protocols.  Built-in ``datetime`` and ``timedelta`` retain their normal Python
value protocols.

Provider-backed value functions such as ``hgraph.temporal.at_zone``,
``resolve``, ``convert_zone``, and checked zoned-duration addition obtain the
provider from the active ``GlobalState``.  This is the same state consulted by
the corresponding graph operator, so a value operation inside a compute node
and a graph operator resolve against one provider version and policy contract.
Calling one of these functions without first installing a provider fails
rather than silently constructing an unrelated process-global provider.

For compatibility, a naïve Python ``datetime`` at an ``Instant`` boundary is
interpreted as UTC.  An aware ``datetime`` is normalized to UTC.  No local
zone is inferred.  ``CivilDateTime`` remains a distinct class because a naïve
``datetime`` cannot simultaneously identify both an absolute instant and
unresolved civil fields.

``TimeRange`` may be a generic typing surface, but it must not be a runtime
alias for ``InstantRange``.  Concrete range classes preserve endpoint schema
identity for Python dispatch.

Structural accessors
--------------------

The value surface exposes fields without converting through strings or Python
objects:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Type
     - Accessors
   * - ``Instant``
     - signed epoch microseconds
   * - ``Duration``
     - signed microseconds and floating/decimal total seconds
   * - ``CivilDate``
     - year, month, day, weekday, ISO weekday, and day of year
   * - ``CivilTime``
     - hour, minute, second, and microsecond
   * - ``CivilDateTime``
     - date and time plus the corresponding delegated civil fields
   * - ``Period``
     - total months, normalized years/months, and days
   * - ``ZoneId``
     - exact interned name
   * - ``ZonedDateTime``
     - instant, zone, offset seconds, and civil date/time derived from
       ``instant + offset``
   * - ``TimeRange[T]``
     - optional start/end, lower/upper boundary, and empty/bounded/unbounded
       predicates
   * - ``FixedRangeSet[T, Capacity]``
     - size, compile-time capacity, and indexed/iterated normalized ranges

A zone abbreviation is deliberately not a ``ZonedDateTime`` field.  It is
provider/version dependent and is obtained through an explicit provider
query.  Civil field extraction from an already resolved ``ZonedDateTime``
uses its stored offset and does not query the provider.

Operator and dispatch semantics
-------------------------------

Temporal scalar functions are exposed as ordinary C++ functions, direct
Python value functions under ``hgraph.temporal``, and native hgraph operator
overloads.  Python graphs call the overloads while Python compute nodes may
apply the value functions to ``input.value``.  Both paths execute the same
native implementation.

The operator matrix is:

.. list-table::
   :header-rows: 1
   :widths: 30 34 36

   * - Inputs
     - Operation
     - Result
   * - ``Instant``, ``Duration``
     - add/subtract
     - ``Instant``
   * - ``Instant``, ``Instant``
     - subtract
     - ``Duration``
   * - ``CivilDate``, ``CivilTime``
     - combine
     - ``CivilDateTime``
   * - ``CivilDateTime``, ``Duration``
     - add/subtract
     - ``CivilDateTime``
   * - ``CivilDate``/``CivilDateTime``, ``Period``
     - add/subtract with ``MonthEndPolicy``
     - same civil type
   * - ``Instant``, ``ZoneId``
     - ``at_zone``
     - ``ZonedDateTime``
   * - ``CivilDateTime``, ``ZoneId``
     - ``resolve`` with fold/gap policies
     - ``ZonedDateTime``
   * - ``ZonedDateTime``
     - ``to_instant`` / ``to_civil``
     - ``Instant`` / ``CivilDateTime``
   * - ``ZonedDateTime``, ``ZoneId``
     - ``convert_zone`` preserving the instant
     - ``ZonedDateTime``
   * - ``TimeRange[T]``, ``T`` or ``TimeRange[T]``
     - range algebra
     - boolean, range, or ``FixedRangeSet<T, 2>``

``MonthEndPolicy``, ``AmbiguousTimePolicy``, and
``NonexistentTimePolicy`` are scalar wiring-time policy inputs in the initial
operator set.  Overload/implementation selection resolves them before
evaluation.  The node does not branch on an unchanged policy every tick.  If a
genuinely time-varying policy is later required, a graph-level switch composes
the static implementations as required by RFC 0000.

A scalar or time-series ``ZoneId`` may vary.  A dynamic-zone implementation
caches the last intern key and provider lookup.  Static-zone overloads bind
the provider's zone record once at node start.

Quantization, bucketing, and iteration
--------------------------------------

The core supplies fixed-timeline utilities needed by generic engines and data
processing:

``floor`` / ``ceil`` / ``round``
   Quantize an ``Instant`` or ``Duration`` to a positive ``Duration`` quantum.
   Instant quantization is relative to an explicit origin, defaulting to the
   Unix epoch.  Negative values use mathematical floor/ceiling rather than
   truncation toward zero.  ``round`` selects the nearest quantum and resolves
   an exact halfway value with ties-to-even: the result whose integral quantum
   index relative to the origin is even is selected.  Thus, with a one-second
   quantum and epoch origin, ``+0.5s`` and ``-0.5s`` round to zero,
   ``+1.5s`` rounds to ``+2s``, and ``-1.5s`` rounds to ``-2s``.

``bucket``
   Return the half-open ``InstantRange`` containing an instant for a positive
   width and explicit/default epoch origin.

``iterate``
   Lazily visit coordinates in a bounded ``InstantRange`` using a positive
   ``Duration`` step, or in a bounded ``CivilDateRange`` using a positive
   Gregorian-day step.  The iterator honours the lower and upper boundaries,
   detects overflow, and does not materialize a container.

``partition``
   Lazily produce half-open sub-ranges over a bounded range.  The final range
   is clipped to the requested input boundary unless an explicit "full
   buckets" mode is selected.

Zero/negative steps and unbounded iteration reject.  Months are not accepted
as a fixed bucket width; a calendar-aware partition uses ``Period`` plus the
explicit month-end and zone-resolution pipeline.  Business-day and
session-based bucketing remain extension operations.

Runtime representation and ABI
------------------------------

The logical payload and 64-bit-build targets are:

.. list-table::
   :header-rows: 1
   :widths: 24 46 15 15

   * - Type
     - Native payload
     - Size goal
     - Heap per value
   * - ``Instant``
     - signed microseconds since epoch
     - 8 bytes
     - none
   * - ``Duration``
     - signed microseconds
     - 8 bytes
     - none
   * - ``CivilDate``
     - validated Gregorian fields; canonical epoch-day codec
     - <= 8 bytes
     - none
   * - ``CivilTime``
     - microseconds since midnight
     - 8 bytes
     - none
   * - ``CivilDateTime``
     - signed local microseconds since civil epoch
     - 8 bytes
     - none
   * - ``Period``
     - int32 total months + int32 days
     - 8 bytes
     - none
   * - ``ZoneId``
     - uint32 slot + uint16 generation + uint16 name tag
     - 8 bytes
     - none
   * - ``ZonedDateTime``
     - int64 instant + 8-byte zone handle + int32 offset
     - <= 24 bytes
     - none
   * - ``InstantRange``
     - two instants + normalized flags
     - <= 24 bytes
     - none
   * - ``CivilDateRange``
     - two dates + normalized flags
     - <= 16 bytes
     - none
   * - ``InstantRangeSet``
     - two normalized instant ranges + uint8 active count
     - <= 56 bytes
     - none
   * - ``CivilDateRangeSet``
     - two normalized civil-date ranges + uint8 active count
     - <= 40 bytes
     - none

The fixed-size types are standard-layout and trivially copyable where the
supported standard-library representation permits.  Their ``ValueOps`` do not
allocate for construct, copy, move, equality, comparison, hash, or range
algebra.  Formatting and serialization may allocate their result buffers.

``ZoneId`` name storage and TZDB transition tables are shared process/run
resources respectively.  A ``ZonedDateTime`` value never owns either resource.
Native scalar storage remains valid without Python, nanobind, or a Python
interpreter.

Exact member offsets are not a wire format.  Before these types become part of
the stable extension ABI, the installed-SDK ABI tests must lock the intended
size, alignment, standard-layout, trivial-copy, and cross-shared-library
behaviour.  A later ABI change requires the normal ABI-version process.

Error and overflow model
------------------------

Construction rejects invalid civil fields, invalid range ordering, invalid
zone-name syntax, and out-of-range offsets with ``std::invalid_argument``.
Arithmetic overflow raises ``std::overflow_error``.  Provider lookup and
resolution use specific errors for unknown zone, ambiguous civil time,
nonexistent civil time, and TZDB/offset inconsistency.

Graph operators translate those failures into the normal hgraph node-error
path with the type, value, zone, provider version, and selected policy in the
diagnostic.  Comparison, hashing, and queries on valid values are ``noexcept``.
No operation relies on signed integer overflow or silently saturates.

Serialization and interchange
-----------------------------

Canonical interchange is independent of the C++ layout:

.. list-table::
   :header-rows: 1
   :widths: 24 36 40

   * - Type
     - Arrow representation
     - Text/JSON representation
   * - ``Instant``
     - ``timestamp[us, "UTC"]``
     - RFC 3339 UTC timestamp ending in ``Z``
   * - ``Duration``
     - ``duration[us]``
     - canonical signed-microsecond string
   * - ``CivilDate``
     - ``date32``
     - ``YYYY-MM-DD``
   * - ``CivilTime``
     - ``time64[us]``
     - ``HH:MM:SS[.ffffff]``
   * - ``CivilDateTime``
     - timezone-free ``timestamp[us]``
     - timezone-free civil date/time
   * - ``Period``
     - month-day-nano interval with zero nanoseconds
     - schema-directed ``months``/``days`` object
   * - ``ZoneId``
     - UTF-8 zone name
     - zone-name string
   * - ``ZonedDateTime``
     - struct of instant, zone name, and offset seconds
     - RFC 9557 timestamp with offset and named-zone suffix
   * - ``TimeRange[T]``
     - struct of nullable endpoints and normalized flags
     - schema-directed range object
   * - ``FixedRangeSet[T, Capacity]``
     - fixed-size list/struct plus active count
     - ordered array of normalized range objects

The canonical ``Duration`` text value, including the JSON string value, uses
this exact ASCII grammar:

.. code-block:: text

   duration = "0us" / ("-"? non-zero-digit *digit "us")

Positive values have no leading ``+``; non-zero values have no leading zeroes;
and ``-0us`` is invalid.  Examples are ``"0us"``, ``"1us"``, and
``"-86400000000us"``.  Writers emit only this form.  A separate,
explicitly-selected non-canonical ingestion parser may accept ISO 8601 duration
strings, but the canonical decoder and all hgraph writers do not treat those
strings as a second interchange representation.

``timestamp[us, "UTC"]`` is the canonical Arrow ``Instant`` representation,
introduced as temporal encoding version 2.  Arrow schema metadata records
``hgraph.temporal.version = "2"`` for this encoding.  An absent marker is
treated as legacy version 1 only at a schema-declared hgraph compatibility
boundary; generic Arrow ingestion does not infer a version.  The codec version
is independent of the package version and native ABI.

Readers continue to accept the legacy timezone-free ``timestamp[us]`` only
when the declared hgraph schema identifies the field as ``Instant`` (or an
explicit legacy-UTC ingestion mode says so).  A bare timezone-free Arrow
timestamp is not inferred to be an instant because it is also the canonical
representation of ``CivilDateTime``.  In-memory values normalize to
``Instant`` immediately and a subsequent write emits
``timestamp[us, "UTC"]`` under version 2.  A version-2 field declared as
``Instant`` but encoded without ``"UTC"`` is invalid.  Writers do not emit the
legacy form once the migration is enabled.

Native and Arrow ``Instant`` payloads can represent values outside Python's
year 1--9999 civil domain.  Conversion of such a value to Python
``datetime``, RFC 3339, or RFC 9557 text rejects with an explicit range error;
it never clips or wraps the value.

RFC 9557 permits offset and named-zone information to disagree.  Strict hgraph
decoding rejects an inconsistency.  A permissive "offset authoritative" mode,
if required for external ingestion, must be explicit and must preserve the
original zone annotation for diagnostics.

The TZDB version is recorded once in the serialization envelope, frame schema
metadata, recording manifest, or run metadata rather than repeated in every
``ZonedDateTime`` payload.  Data from different provider versions must not be
combined under one version marker without explicit reconciliation.

Performance and memory goals
----------------------------

The performance contract is:

* Timeline arithmetic, civil field access, equality, hashing, range
  containment, intersection, overlap, adjacency, and merge are ``O(1)`` and
  allocation-free.
* Copying a temporal point or single-range TS value copies no more than 24
  bytes and never copies a zone-name string.  The two-component fixed range
  result copies no more than 56 bytes.
* Constructing/interning ``ZoneId`` is ``O(name length)`` and occurs at
  configuration, decode, or value-ingress boundaries.  Copy, equality, and
  hash are ``O(1)``.
* ``Instant -> ZonedDateTime`` and civil resolution perform no heap allocation
  once a zone is bound.  Lookup is amortized ``O(1)`` for values within the
  cached transition and at worst ``O(log transitions)``.
* A static-zone graph node binds its zone record once at ``start``.  A dynamic
  zone node repeats lookup only when the intern key changes.
* Loading and indexing one TZDB data set occurs once per provider, not once per
  graph or scalar value.
* Pure C++ temporal graphs contain no Python calls or Python object storage.

Release-mode conformance includes:

* ``static_assert`` or equivalent tests for the size/alignment ceilings;
* allocation-count tests for hot scalar/range operations and cached zone
  resolution;
* microbenchmarks against direct ``int64``/``std::chrono`` arithmetic and
  hand-written endpoint comparisons;
* separate cold-name, cold-transition, cached-transition, fold, and gap
  benchmarks; and
* recorded baselines with a review threshold of 10% regression once the first
  accepted implementation establishes those baselines.

The intent is not to promise a platform-specific nanosecond count.  The
measurable contract is fixed-size storage, no hot-path allocation, bounded
lookup complexity, and protection against unexplained regression.

Compatibility and migration
---------------------------

``DateTime``/``TimeDelta`` and ``Date``/``Time`` remain available.  ``Instant``
and ``Duration`` are aliases, so they do not introduce duplicate native schema
identity.  Existing Python graphs using naïve UTC ``datetime`` values continue
to work.

New civil and zoned types are distinct scalar schemas and require explicit
conversion.  An existing naïve ``datetime`` is never silently reinterpreted as
``CivilDateTime`` merely because it appears beside a ``ZoneId``.

The preliminary proposal-branch prototype is superseded by the accepted
implementation.  In particular, the implementation uses the checked intern
handle for ``ZoneId``, total months for ``Period``, immutable Python values,
normalized unbounded ranges, and a value-returning intersection.

Alternatives considered
-----------------------

Use Python ``datetime``/``zoneinfo`` objects as the runtime representation
   Rejected because it makes the C++ path depend on Python, allocates and
   reference-counts per value, and leaves C++ users without the same contract.

Expose ``std::chrono::zoned_time`` directly
   Rejected as the stored ABI because it exposes a backend-specific zone
   pointer and standard-library TZDB lifecycle.  A chrono implementation may
   satisfy the provider boundary.

Store only an instant
   Rejected for zoned values because it loses named-zone intent and makes
   calendar projection dependent on external context.

Store civil fields plus a zone but not an instant/offset
   Rejected because folds are ambiguous, gaps may be nonexistent, and later
   TZDB changes can silently alter the interpretation.

Store the zone name string in every value
   Rejected because ordinary TS copies and equality would carry
   variable-length storage and possible allocation.  Interning keeps value
   semantics while moving name ownership out of the hot path.

Treat months as a fixed duration
   Rejected because the elapsed length depends on the anchor and zone.

Give ``Period`` a total order
   Rejected because there is no anchor-independent ordering between months and
   days.

Infer the process-local zone
   Rejected because it is host-dependent and breaks deterministic replay.

Make every range closed
   Rejected because adjacent replay partitions and bars would double-count
   shared endpoints.  ``[start, end)`` remains the default.

Use sentinel endpoint values for infinity
   Rejected because legitimate minimum/maximum values become unavailable and
   arithmetic can accidentally shift a sentinel.  Explicit flags represent
   unboundedness.

Resolved implementation choices
-------------------------------

This revision resolves the remaining representation choices:

* prefer a conforming standard ``std::chrono`` TZDB provider and use Howard
  Hinnant's ``date/tz`` as the pinned pure-C++ fallback;
* use an eight-byte checked ``ZoneId`` handle containing slot, generation, and
  diagnostic name tag;
* return ``FixedRangeSet<T, 2>`` from binary range union and difference; and
* make Arrow ``timestamp[us, "UTC"]`` the canonical ``Instant`` encoding in a
  separately versioned migration.

The implementation PR must supply the feature-probe, backend-conformance,
layout, allocation, and migration evidence specified by this RFC.  These
choices do not permit changing fold/gap semantics, equality, range
normalization, or wire-level information without updating the RFC.

Acceptance criteria and test plan
---------------------------------

Type and ABI tests
~~~~~~~~~~~~~~~~~~

* Every native type has one canonical scalar schema and equivalent ``TS`` and
  collection-schema behaviour in C++ and Python.
* Values are immutable and hash/equality compatible.
* Size, alignment, trivial-copy, storage-plan, installed-header, and
  cross-shared-library tests enforce the representation goals.
* Pure C++ consumers build and run without Python.

Arithmetic tests
~~~~~~~~~~~~~~~~

* Named checked functions and graph operators exercise overflow at every
  physical and portable-domain edge; tests separately document that raw chrono
  alias operators are outside that guarantee.
* Quantization covers positive and negative halfway values and proves the
  ties-to-even rule in C++ and Python.
* Duration and Period operations cannot be mixed implicitly.
* Period canonicalization proves one year equals twelve months.
* Month-end policies cover leap and non-leap February, positive and negative
  periods, and mixed month/day signs.
* C++ and Python produce identical values and errors.

Zone tests
~~~~~~~~~~

* A fake provider proves unique, ambiguous, nonexistent, unknown-zone, and
  version-mismatch paths without relying on the host TZDB.
* Pinned integration vectors cover ordinary transitions, both sides of a fold,
  both edges of a gap, a 30-minute transition, a skipped civil day, historical
  second offsets, and TZDB links.
* Static-zone resolution binds once; dynamic-zone resolution rebinds only when
  its ``ZoneId`` changes.
* Registry tests cover duplicate interning, invalid slot, stale generation,
  bad name tag, generation exhaustion, and re-interning names from serialized
  text rather than copying raw handles.
* RFC 9557 round trips preserve instant, offset, and exact named-zone identity.
* Inconsistent offset/zone input rejects under strict decoding.

Range tests
~~~~~~~~~~~

* Exhaustive small-domain tests cover empty, singleton, finite, half-bounded,
  unbounded, and every open/closed combination.
* Property tests prove intersection commutativity and idempotence,
  normalization stability, merge/difference reconstruction, and containment
  consistency.
* Union and difference prove their result never exceeds two components;
  ``FixedRangeSet`` ordering, coalescing, active count, canonical unused slots,
  equality, hashing, and immutable Python sequence behaviour agree in C++ and
  Python.
* Default half-open partitions neither overlap nor leave a boundary hole.
* Range algebra and TS round trips agree in C++ and Python.

Performance tests
~~~~~~~~~~~~~~~~~

* Hot scalar and range operations allocate zero times.
* Representation ceilings are enforced on all supported 64-bit platforms.
* Cold/cached provider benchmarks and static/dynamic zone-node benchmarks are
  recorded.
* Performance regression thresholds are added once the accepted baseline is
  established.

Serialization tests
~~~~~~~~~~~~~~~~~~~

* Text/JSON/Arrow golden vectors cover every scalar and range shape.
* Duration golden vectors enforce the canonical signed-microsecond grammar,
  including rejection of leading ``+``, leading zeroes, ``-0us``, missing
  suffixes, and overflow.
* New-version ``Instant`` writers emit ``timestamp[us, "UTC"]``; readers
  accept schema-identified legacy UTC ``timestamp[us]``, reject ambiguous
  unqualified input without a schema/policy, and normalize legacy values on
  rewrite.
* Version-2 Arrow schemas carry ``hgraph.temporal.version = "2"`` and reject
  timezone-free fields declared as ``Instant``.
* Frame, FrameStore, Arrow C stream, record/replay, and Python round trips
  preserve temporal schema identity and TZDB version metadata.
* No test relies on the host local zone or an unversioned host TZDB.

Delivery plan
-------------

Implementation may be delivered in reviewable stages:

#. fixed timeline/civil/period values and checked arithmetic;
#. normalized range values and algebra;
#. interned zone identifiers and the provider boundary;
#. zoned resolution and operator overloads;
#. canonical codecs and persistence integration; and
#. allocation, ABI, and performance gates.

Partial stages do not change this RFC to ``Accepted`` while normative
acceptance criteria remain unimplemented.  If the provider or serialization
work is intentionally deferred beyond this RFC, the scope must be split into a
new numbered RFC rather than marking a partial contract accepted.

Implementation status
---------------------

The accepted implementation supplies the compact native values, checked
arithmetic, fixed-capacity range algebra, interned zone identifiers,
``std::chrono``/``date::tz`` provider selection, static-policy graph
overloads, immutable Python authoring values, and canonical JSON and Arrow
version-2 codecs.  Automatic provider selection runs a historical
base-offset-transition conformance vector as well as the compile-time TZDB
feature probe; a standard library that exposes the API but fails that vector
uses the pinned ``date/tz`` fallback.

Conformance coverage includes portable layout checks, exhaustive
small-domain range properties, physical arithmetic edges, wiring-level C++
and Python parity, fake-provider resolution paths, pinned IANA transition
vectors, RFC 9557 offset verification, TZDB-version mismatch rejection, and
legacy/version-2 Arrow migration tests.  Release benchmarks enforce zero
allocation for checked timeline arithmetic, binary range difference, and a
warmed cached-zone projection, and record the initial timing baselines used
for later regression thresholds.

References
----------

* Hinnant, H. E. and Kamiński, T., *Extending <chrono> to Calendars and Time
  Zones*, WG21 P0355R7, 2018:
  https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0355r7.html
* Hinnant, H. E., *date: A date and time library based on the C++11/14/17
  <chrono> header*, including the ``date/tz`` IANA TZDB parser:
  https://github.com/HowardHinnant/date and
  https://howardhinnant.github.io/date/tz.html
* GNU Project, *libstdc++ C++ 2020 implementation status*:
  https://gcc.gnu.org/onlinedocs/libstdc++/manual/status.html
* LLVM Project, *libc++ C++20 status* and *Time Zone Support*:
  https://libcxx.llvm.org/Status/Cxx20.html and
  https://libcxx.llvm.org/DesignDocs/TimeZone.html
* Newman, C. and Klyne, G., *Date and Time on the Internet: Timestamps*,
  RFC 3339, 2002: https://www.rfc-editor.org/rfc/rfc3339
* Sharma, U. and Bormann, C., *Date and Time on the Internet: Timestamps with
  Additional Information*, RFC 9557, 2024:
  https://www.rfc-editor.org/rfc/rfc9557
* Eggert, P., *The Time Zone Information Format (TZif)*, RFC 9636, 2024:
  https://www.rfc-editor.org/rfc/rfc9636
* IANA, *Theory and pragmatics of the tz code and data*:
  https://www.iana.org/time-zones/theory
* Allen, J. F., "Maintaining Knowledge about Temporal Intervals",
  *Communications of the ACM* 26(11), 1983, pp. 832--843,
  doi:10.1145/182.358434
* Noda Time, *Core types* and *Date and time arithmetic*:
  https://www.nodatime.org/3.3.x/userguide/core-types and
  https://www.nodatime.org/3.3.x/userguide/arithmetic
* Apache Arrow, *Columnar Format -- Data Types*:
  https://arrow.apache.org/docs/format/Columnar.html
