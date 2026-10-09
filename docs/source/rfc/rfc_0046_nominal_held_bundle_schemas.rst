RFC 0046: Exact held schemas for nominal bundles
================================================

:Status: Proposed
:Created: 2026-10-08
:Target: TypeRegistry, static schemas, and HGL temporal record lowering

Problem
-------

A nominal ordinary record and its recursively temporalized record can have
different value-layer field schemas. A positional ordinary tuple is a Tuple;
its temporal counterpart holds an unnamed Bundle. The current NominalTSB
descriptor resolves by the ordinary record's name and requires those fields
to be identical. Consequently a record with a tuple field cannot be registered,
even when used only by a composition identity.

The shared language model requires recursive temporalization of records,
tuples, lists and maps, while nominal identity remains the module-qualified
origin and complete argument list. The representation must preserve both rules.

Contract
--------

Add a preparation-time nominal Bundle projection factory. It accepts the
canonical ordinary nominal origin, the exact held field schemas in declaration
order, and exact projected parents. A projected Bundle retains the source
namespace, local name, generic arguments, abstract flag and discriminator. Its
field metadata and structural twin describe only its actual held storage.
Its immutable hierarchy records its ordinary origin, and its parents and
ancestry refer to the projected parent schemas. Even an abstract parent whose
fields have unchanged scalar representations gets distinct held metadata; held
children never join the ordinary parent family or covary with an ordinary schema. Projected metadata is interned
by ordinary origin and exact held fields; it is never registered as another
value-name alias. Ordinary and held schemas remain distinct and are never
accepted as interchangeable by value copying, schema equality or covariance.

The factory verifies field names/counts and the structural projection relation:
scalar leaves preserve their schema, Tuple children can project to positional
Bundles, lists preserve extent while projecting their elements, and maps
preserve key schemas while projecting their values. Exact ordinary children
remain valid at an atomic boundary. Named child projections and parents must
retain their exact ordinary origins. No arbitrary scalar substitution is
permitted. All validation and interning happen at preparation time.

Add a TypeRegistry::tsb overload taking an exact Bundle value schema and explicit
temporal fields. It requires every temporal child's held schema to equal the
corresponding Bundle field schema. Existing canonical named TSB registration
remains unchanged; a projected nominal TSB is interned by its exact Bundle
metadata and temporal field schema, without rewriting a name-cache entry.

A static held-nominal Bundle marker supplies the origin, temporal fields and
temporal parent markers to these same factories. Generated HGL uses the
ordinary marker for scalar records and the held marker for NominalTSB. The
direct wiring type bridge calls the same factories with its already resolved
source contracts. Both paths therefore produce identical metadata.

Runtime conversion
------------------

A node's ordinary record observation normalizes the source through its prepared
held binding and converts each named child to its prepared ordinary binding.
Complete ordinary output values perform the inverse conversion. Recursion
follows the resolved source fields; tuples, lists and maps reuse their existing
prepared conversion paths. Atomic and recursive owner edges stop structural
conversion. No alias, schema lookup, type-kind discovery or registration occurs
during evaluation. Atomic abstract families keep their ordinary schema and
stop structural conversion. This extension does not broaden the shared eval
profile to structural abstract-family inputs.

Validation
----------

A direct public factory test proves that the ordinary and held schemas have
different tuple children, the same nominal origin/arguments, correct held
parent relationships and exact storage descriptors. Mismatched field names,
primitive schemas, tuple positions and parent origins must be rejected.
Composition identity, guarded positional observations, retained locals,
complete results and nested list/map/tuple records are tested through public
wiring. Shared HGL fixtures supply the corresponding spec conformance cases.
Native/Python acceptance, installed-SDK consumption and focused ASan lifetime
tests cover the final change.
