HGL temporal scalar publications
================================

The HGL publication profile admits eleven scalar leaves: ``bool``, ``i64``,
``f64``, ``str``, ``date``, ``time``, ``datetime``, ``duration``,
``civil_datetime``, ``timezone`` and ``zoned_datetime``. ``delta<S>`` and ``atomic<S>`` normalize to ``S`` for each
admitted leaf. The same admission is recursive through existing structural
and finite atomic shapes; it does not admit ``zoned_time`` or relax collection
key, set-element, optional-field or recursive-shape restrictions.

The checker, canonical type substitution, native declaration metadata and
ordinary value plans preserve this single profile. Civil datetime maps to
``CivilDateTime``, zone to ``ZoneId`` and zoned datetime to ``ZonedDateTime``.
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
It validates exact provider membership and the explicit zoned offset, then
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
