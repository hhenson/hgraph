# Contracts around the runtime

Status: extracted requirements and open work; this file preserves useful
material outside the six core chapters without expanding their scope.

An HGL program, a C++ graph and Python authoring can describe the same
computation. Their syntax and host-value conversions differ; the runtime
behaviour they denote must agree. The runtime specification does not replace
the HGL language model, authoring guides, operator library or ABI contracts.

## Types and wiring

Keep five relations distinct: identity, subtyping, assignability, explicit
conversion and generic-pattern matching. Equal physical layout implies none
of them. Replacing storage alone must not change logical identity or matching.
Nominal declarations with equal fields remain distinct; an alias does not
create a new declaration. Canonical encodings, registry generations,
fingerprints and collision handling still need a portable profile. Numeric
pointer addresses are never a cross-process identity.

Semantic capabilities such as equality, hashing and recordability constrain
admitted operations. Trivial copying, alignment and inline storage are target
properties. An opaque native type needs a logical identity and operation
contracts, not merely a C++ class name. Formation rules must cover parameter
resolution, aliases, recursion, variance, constraints and nullable fields.
Current scalar rules and remaining questions live in [Scalar types](scalar_types.md).

An operator contract names a callable, signature, domain and behaviour. A
candidate implements it for some admitted arguments. A complete resolution
contract must specify argument binding, defaults, roles, substitution,
constraints, rejection, ranking and ambiguity. Merely naming helpers such as
`canonical`, `compatible` or `more_specific` does not define an algorithm.
Declaration order must not serve as an unexplained tie-breaker. Preserve the
old proposed vectors: a proven more-specific exact candidate wins regardless
of order; distinct equally best candidates produce ambiguity. These vectors
do not themselves define the rank relation or duplicate-candidate policy.

HGL temporalization, `atomic<T>`, value-function lifting, ordered `when`
handlers, opaque references and input-only SIGNAL need explicit mappings to
chapter rules. Backend choice cannot silently broaden their access or effects.
The existing language documents remain authoritative for source syntax;
constructor names in these cases are descriptive type notation, not new HGL.

## Providers and interoperability

Preserve the proposed **provider removal** trace from #796: register a provider
and candidate; wire a plan retaining it; remove it from future discovery;
resolution finds no candidate, but physical unload is denied while that plan
or a live instance still needs its code/metadata; release the last dependent
and unloading may become eligible. The trace assumes no other dependents.
It is a safety proposal, not a claim of a portable loader implementation.

Transactional registration needs defined commit/rollback visibility, failed
initialization cleanup, dependency removal, reset generations and installer
replay. Logical deregistration and physical unload are different operations.
An owning handle, a scoped borrow, exclusive mutation and shared immutable
retention need different lifetime contracts. Repeated evaluation borrows a
live node; it does not consume its ownership. Collection iterators and views
must not escape their allowed interval through state or callbacks.

Backend profiles can map these concepts to C handles and tables, C++ owners
and views, Rust owners and borrows, or Swift values and scoped facades. No
universal vtable, reference-counting mechanism or ABI is selected. A C ABI
profile would separately define widths, versions, explicit contexts, opaque
handles, caller-visible view lifetimes and status returns across boundaries.
Package compatibility, schema evolution, generated-source compatibility and
binary compatibility are separate promises.

If generation is revisited, preserve readable descriptors, contracts,
registration and conformance artifacts with named handwritten extension
points. Regeneration must not overwrite algorithms. A small two-backend
identity/observation experiment can test that boundary before a broad generator
is adopted. Parser grammar, import rules, spans and round-trip formatting are
deferred tooling questions; Markdown needs none of them.

## Recovery, effects and larger compositions

Cache reconstruction means future behaviour is unchanged given equivalent
authoritative inputs, semantic history, pending work and future external
events. A running total with lost history is not a reconstructible cache.
[Node](node.md) owns this distinction; the HGL lowering and native APIs need
their own implementation-status evidence.

Recovery must additionally define topology, bindings, clocks, timers, child
membership, source positions and effects. Replaying values or seeding inputs
alone proves no exact recovery or exactly-once effects. The checkpoint RFCs
and implementation remain separate from this core specification.

Map, switch, reduce, mesh, feedback, services and adaptors need their own
library contracts built on the chapters' components. Do not erase their
requirements merely because their algorithms are outside the core. Likewise,
numeric overflow, NaN, negative division, Unicode and algebraic laws belong
to precise scalar/operator domains, not host-language defaults.

## Remaining extraction questions

| Earlier question | Destination and remaining limit |
|---|---|
| OPEN-01: type algebra | Scalar chapter and wiring relations above; portable identity still incomplete |
| OPEN-03: initial state and publication | TS-1/TS-2 and atomic cases; setter admission and multiple-write adapters need evidence |
| OPEN-04: time and activation | Engine, graph and node chapters; externally driven stepping and phase hooks remain deferred |
| OPEN-05: binding and SIGNAL | Time-series chapter; full compatibility, structural observation and expired stored references remain open |
| OPEN-06: collections/windows | Time-series chapter and collection cases; full delta adapter and eviction boundaries still need cases |
| OPEN-07: resolution | Wiring requirements above; complete ranking algorithm remains outside this runtime core |
| OPEN-08: failures | Node/graph lifecycle and pair cases; nested capture and post-error output policy need reconciliation |
| OPEN-09: restore | Recovery boundary above; no whole-run recovery claim |
| OPEN-10: special nodes | Separate library contracts using nested-graph components |
| OPEN-11: numerical/library laws | Scalar questions and operator-domain contracts |
| OPEN-12: native/resource/ABI profiles | Provider and representation profiles; explicit concurrency and backpressure boundaries |

The earlier ledger has no OPEN-02 entry; `all_valid` is settled in TS-9, not a
new open decision. Extend coverage in dependency order: atomic graph, structural
collections, binding, wiring, state/failure, dynamic graphs/providers, then
library and target completeness. Each extension needs rules, boundary examples
and evidence together.
