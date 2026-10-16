HGL temporal scalar publications
================================

The HGL publication profile admits thirteen scalar leaves: ``bool``, ``i64``,
``f64``, ``str``, ``bytes``, ``date``, ``time``, ``datetime``, ``duration``,
``civil_datetime``, ``timezone``, ``zoned_datetime`` and ``zoned_time``. ``delta<S>`` and ``atomic<S>`` normalize to ``S`` for each
admitted leaf. The same admission is recursive through existing structural
and finite atomic shapes; it does not relax collection
key, set-element, optional-field or recursive-shape restrictions.

The checker, canonical type substitution, native declaration metadata and
ordinary value plans preserve this single profile. Civil datetime maps to
``CivilDateTime``, zone to ``ZoneId``, zoned datetime to ``ZonedDateTime`` and
zoned time to ``ZonedTime`` (RFC 0045). Zoned time retains wall-clock time and
exact zone spelling with no date or offset.
Existing owning ``Value`` capture and ordinary delta plans retain these values
without conversion. Generic ``delta<T>`` operator checks defer until the
origin is concrete: scalar normalization permits ordinary equality, while
structural delta equality remains a type error. Const aggregate forwarding
retains an owning ``Value`` before native wiring. A generic observer whose
shape comes only from a const aggregate prepares its ordinary plans from that
argument's exact metadata, projecting list elements and tuple or nominal
fields before start. Evaluation uses the prepared plans. Equal present
publications remain ticks. Zoned equality
includes instant, exact zone spelling and resolved offset; civil values retain
wall-clock fields. Provider-dependent literal comparisons cannot be folded
away before their operands have been validated.

``hgl/temporal_literals.h`` is the compiler's literal-materialization boundary.
It validates exact provider membership and, for zoned datetimes, the explicit offset, then
produces an ordinary runtime scalar. It adds no source injectable or public
resolution operator. Retaining and replaying an existing scalar never invokes
this boundary again. Provider-dependent literal construction in generated
node hooks (including ordinary helpers that construct such literals) is
explicitly unsupported: callers supply an already constructed scalar. This
keeps zone lookup and provider validation off the publication hot path without
introducing a new provider lifetime contract. Provider-dependent defaults stay
as HGL IR recipes and are selected at HGL call/eval sites. They are excluded
from eager native ``defaults()`` metadata, which would otherwise validate an
unused default during registration. A generated C++ marker therefore requires
an explicit argument for such a default; ordinary HGL omission and override
semantics remain unchanged.

The direct backend owns the cold literal provider and installs that same
provider in each eval run seed before materializing inputs. It evaluates and
retains every supplied expression once
in written order, including const arguments interleaved with dense sequences.
Omitted defaults are selected only after supplied arguments. Generated cold
composition and helper calls likewise retain supplied arguments in written
order, then selected defaults in declaration order, before invoking the callee
with parameter-ordered temporaries. Complete input
validation precedes target construction and start; failure cannot escape as a
partial replay. Shared HGL replay and record continue to own scheduling and
recordings, including empty and all-silent horizons. Test setup and ordinary
helpers use the same cold literal provider. No ambient ``GlobalContext`` is
activated around an explicitly seeded ``Wiring``.

The language contract is pinned in
``external/hgraph_spec/language/docs/design/temporal-scalar-publications.md``.
Shared conformance cases live in the three ``temporal_*_values.hgl`` test parts,
``recursive_eval_values.hgl`` and ``record_observation_values.hgl`` of
``external/hgraph_std``; compiler checks additionally cover unsupported
shapes, strict literal failure and materialization order.

Signal observation
------------------

``signal`` remains an input-only view that preserves endpoint validity,
modification and last-modified metadata while hiding its payload. A generic
composition forwarding a concrete endpoint to a signal observer is materialized
using its exact source schema, including structural and atomic boundaries.
Generated helper definitions precede their callers, and reuse the same prepared
composition contract as the direct backend. No scalar conversion or payload
snapshot is needed for signal observation. Shared ``signal_observation_values``
fixtures cover each admitted scalar, structural and atomic inputs, equal
publications, silence, and observation from an independent trigger.
A signal never observes a REF designation (TS-18). Offered a REF endpoint, the
binding owner adapts it into the target-following route, exactly as for an
ordinary input, so the signal's validity, tick and time are those of the
designated output: a changed designation re-binds and samples it, a repeated
equal designation is not a tick (TS-16), and a target tick is. Cold wiring
still retains the supplied port's declared view on the graph edge and on
nested input bindings, independently of the physical producer schema, and
binding resolves that view through the existing output-owned adapter
(RFC 0036); a prepared input then observes that stable route (RFC 0008). An
opaque REF port and its explicit ordinary view therefore give a signal the
same observations. The logical SIGNAL input keeps its Boolean current/delta
facade and metadata-only source capability. Edge interning, manifests and
nested checkpoint signatures include the declared view, so bindings with
different declarations do not share a graph descriptor.
Nested input owners cache the resolved declared view by producer handle. An
unchanged producer reuses that route without schema discovery or registry access;
retargeting the producer's REF value is handled by the existing adapter. A new
producer is resolved when its topology is bound. The cache borrows handles,
adds no subscriptions and is discarded with the input owner's storage.
Binding also caches the ordinary scalar target's forwarding links.
If a REF deselects that scalar in the same cycle as its former target publishes,
the followed view is invalid and unmodified, with time ``MIN_DT``; a scheduling
notification already received from the former target cannot become its event.
This is the ``REF-SAMPLE`` rule in the canonical Spec's
``runtime/cases_references.md`` and binding table in ``runtime/time_series.md``.
The policy distinguishes an empty designation from an explicit producer
invalidation, and does not suppress structural withdrawal deltas or REF
designation ticks. Metadata follows only these cached links, without schema
discovery or registry lookup.

Python graph parameters establish the same declared view through ``value_port``
while wiring, after generic resolution. Python node argument packs dereference
REF-bearing shapes for SIGNAL fields exactly as for ordinary fields: a SIGNAL
over a peered REF endpoint follows its target, and a SIGNAL over a non-peered
structural argument follows the targets of its REF children. Only an explicit
REF declaration keeps a designation shape in the pack.

Fixed List endpoint projection
------------------------------

A graph expression selecting a checked constant index of a concrete fixed
List uses the existing ``tsl_element`` wiring projection. It preserves the
selected child's exact schema, endpoint identity and traffic; siblings do not
introduce a runtime selection node. Passing that child to a REF parameter
uses the normal binding adaptation. Explicit REF-wrapped List projections
retain their prepared designation route, and temporal indices retain dynamic
operator selection.

Graph-time fixed List assembly into explicitly declared REF elements retains
the ordered child ports in an existing
``WiringPortRef::structural_source``. Its concrete size comes from the written
children; the ordinary runtime List literal admission remains separate.
The inferred element type participates in normal generic substitution and
REF-transparent input binding, so explicit REF positions preserve designations
without a value-copying conversion node. Generic test-harness calls bind their
signature from concrete publication elements before validating input and output
profiles, and retain those substitutions while wiring replay and recording.

The scalar target-link policy suppresses a sample when a REF designates an
invalid endpoint, including an unpublished or explicitly invalidated producer.
Invalidation still notifies and schedules active consumers; an ordinary invalid
scalar input reads unmodified with no current delta. Sampled structural ``delta_value`` observations use
``capture_delta`` to preserve the target-link strategy's sampled transition,
including dictionary removals and current children, rather than borrowing a
producer delta from an earlier cycle.
Ordinary producer ticks retain their borrowed delta path; owning capture is
required only for a sampled rebind or structural transition.

A generated structural delta observation keeps its sampled ``Value`` owner in
``DeltaObservation`` for the whole local scope. Ordinary producer deltas remain
borrowed views, so holding a ``let`` does not copy them per tick. Views of sampled
storage are computed from the current owner after moves and local bindings;
generator cache retention materializes an independent ordinary delta before it
crosses a yield boundary. Scalar invalidation keeps notification/scheduling
separate from payload modification. Tracking/sample clocks and structural
withdrawal transitions retain their existing behavior, including the distinct
child-change observations of a still-valid assembled structure.


Live map key connections
------------------------

Graph indexing of a non-atomic map, including a map reached through an outer
REF, preserves the native ``getitem_`` selector's REF result. The checked
Index result is ``ref<V>``; ordinary dictionary values and atomic payloads keep
their value lookup rules. Both backends use the existing native dictionary
selector and ordinary input-side REF binding, including parent-map retargeting.
No child payload is read during wiring and no value-forwarding node is added.
The selector observes structural membership changes, retains a stable child
position while it exists, and deduplicates equal designations. The pinned
``map-key-connections.md`` specification owns absence, sampling and lifetime
semantics; native and generated HGL controls exercise those observations.
