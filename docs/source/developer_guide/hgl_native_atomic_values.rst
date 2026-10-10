HGL native atomic values
========================

NVAL-1–5 in
``external/hgraph_spec/language/docs/design/native-atomic-values.md``
defines opaque ordinary scalar declarations. ``native type`` retains a
module-local binding; ``export native type`` exposes that binding through the
same selective and qualified import paths as other public types. It adds no
fields, generic parameters, representation access or type-name constructor.

The native package descriptor maps each HGL declaration to an existing
canonical native scalar identity and records its shared capability contract.
Checking reads this data without loading or executing provider code. Aliases
with one canonical identity retain one value type; similar representations
with different canonical identities remain different. Declared copy and text
operations are mandatory. Equality, hashing, ordering and serialization remain
explicit capabilities, independent of what a C++ class happens to implement.
Physical operations may exceed that contract. A checked binding narrows the
exposed equality, hash and order flags while retaining the exact provider
layout, lifecycle and operations needed for storage. Copies, including boxes
and aggregate leaves, retain this binding; empty aggregates propagate the
declared child capabilities before any values arrive.

Preparation binds that contract to the provider's registered scalar metadata
and rejects missing or incompatible providers before execution. The current
raw C++ scalar storage ABI requires physical default-construction and
copy-assignment hooks in addition to owning copy and text. These are private
layout/lifecycle compatibility requirements, not additional source capability
flags; unsupported mappings fail before publishing their exposed binding.
Generated native helpers use exact read-only signatures. Prepared ordinary and temporal
plans retain canonical bindings, so native publication, observation, copying
and boxed operations perform no type-name lookup or capability selection per
tick. The owning value and its operations remain usable through destruction;
linked providers and resident scripted images supply the existing provider
lifetime boundary.

Executed test setup and cold materialization use provider-owned
``OperatorRegistry::evaluate_const`` kernels only when the checked helper
explicitly permits Wiring and requests no runtime capabilities. Source
checking and required constant evaluation never invoke these kernels. A source
``const fn`` wrapper cannot hide native helper dependencies from a required
default; the checker resolves these dependencies before lowering. Ordinary
source calls used by defaults retain the declaration-enclosing scope, as
specified by Scope96.
Generated node hooks call the exact C++ function pointer signature directly;
construction results and reader arguments must match the declared C++ type.
Read-only helper arguments borrow the prepared scalar for that invocation,
without an extra owning copy or DSO-local operation-address check. Consuming
an unset retained scalar keeps the existing ``value.unset_read`` diagnostic.

Native leaves use the ordinary scalar publication path recursively through
sparse structures, complete atomic payloads, boxes, rolling arrivals and
timed replay/record. Atomic and delta scalar wrappers normalize to the same
native identity. Empty native contents and repeated equal publications remain
ticks; unset elements represent silence. A native scalar global-state read
owns an independent copy, while an enclosing aggregate retains its existing
lexical borrow behavior. Native resource state does not gain any of these
ordinary or temporal admissions.

The provider fixture supplies distinct owning text-backed Token and TokenOther
scalars with equality, hashing and lexicographic ordering, plus a TextOnly
scalar without those optional capabilities. Descriptor/checker tests cover
identity, exports, aliases, phase and capability rejection without running
helpers. Native and generated graph tests exercise transport and provider
lifetimes, and shared HGL cases validate the specified observations. The full
shared Native fixture also has a precompiled host with scripted compilation
disabled, preserving the same authored cases on Windows.
