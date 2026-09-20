# ADR 0013: struct imports

Status: proposed (2026-09-20).

## Context

`export struct` is already source syntax, and the user guide already states
what it means: "the nominal identity is the module-qualified name", and
"`export struct` exposes the name to other modules in the same way that
`export fn` exposes an ordinary function" (types-and-expressions.md,
"Structured values"). Module descriptors already carry an exported struct's
whole layout — `DeclarationCategory::Structure` with its fields, generic
parameters, parents, and, since format 6 (ADR 0012), a `recursive` mark per
field.

The import half does not exist. Name resolution rejects every qualified
source type outright:

> qualified source types require a module descriptor; only local struct types
> are available in this prototype
> — `src/semantics/resolve.cpp`

So the exporter writes a complete layout that no importer can read. The
catalog (`semantics/module_catalog.h`) carries `functions` and `operators` and
nothing else, and there is no binding kind for a struct that is not a local
declaration. ADR 0012's acceptance item "imported by a second module" is
blocked on this, and so is any module that wants to publish a data shape
rather than only behaviour.

## Decision

**A struct imports exactly as a function does.** `use m::{Quote}` binds the
name in the importing module for resolution. Nothing else about it is local.

**The type identity stays with the owning module.** An imported `Quote` is
`m.Quote`, not a copy under the importer's namespace. There is no second type
and no second registry entry: a name is bound, an identity is referred to.
Two modules that import the same struct name the same type, and a value
crosses between them without conversion.

**An importer registers the same description under the owner's qualified
name, and the registry interns it.** This is the rule `bundle()` already
follows — an identical description under an existing name is the same schema —
and `TypeRegistry::recursive_bundle_closure` already follows for a batch
(hgraph RFC 0041). Re-describing is how an importer says *which* type it
means without requiring the exporting module's process to have run first; it
is not a copy. A description that disagrees with the registered one is a
conflict and is reported, not silently accepted.

### Catalog

`ImportableModule` gains `structs`, filled by `add_to_catalog` from the
descriptor's `Structure` declarations:

```cpp
struct ImportedStructField
{
    std::string  name{};
    ImportedType type{};
    bool         optional{false};
    bool         recursive{false};   ///< ADR 0012 edge; target named by identity
};

struct ImportedStruct
{
    std::string                      module_identity{};
    std::string                      name{};
    std::string                      identity{};       ///< m.Quote
    bool                             abstract{false};
    std::vector<ImportedGeneric>     generics{};
    std::vector<ImportedStructField> fields{};
    std::vector<ImportedType>        parents{};         ///< Symbol, by identity
    std::vector<std::string>         public_headers{};  ///< the exporter's generated header
    std::string                      descriptor_fingerprint{};
    std::string                      support_error{};
};
```

`public_headers` follows `ImportedFunction`: an imported entity carries what a
consumer needs in order to use it.

### Resolution

A qualified `Named` type resolves through the catalog to an `ImportedStruct`,
bound as a new `BindingKind::ImportedStruct` whose `index` names
`ResolvedModule::imported_structs`, mirroring `ImportedFunction`. The
prototype rejection is removed. An unqualified name still resolves locally
first; a `use` of a struct name makes the unqualified spelling available the
way a `use` of a function does.

### The shared passes

Typed HIR and hgraph IR name an imported struct **by identity**, never by
expansion — the rule ADR 0002 already sets for the backend boundary and ADR
0012 already follows for a recursive edge's target. `ImportedTypeKind::Symbol`
with `binding_identity` is the existing representation and is reused.

### Inheriting an imported family

**A local struct may inherit an imported abstract parent.** Extending a family
a library publishes is a large part of why a library is worth having, so this
is part of the decision rather than a later question.

It needs no new mechanism. hgraph requires a parent to be registered before
its children, and `bundle()` takes parents as already-registered metadata. An
importer holds the parent's whole layout, so it registers the imported parent
and then the local child — the parent-before-child order both backends already
compute for local parents, with imported parents joining the same topological
sort.

The consequence is deliberate and worth stating: a local child **joins the
imported family**. `a.Base`'s bundle hierarchy gains a member and its
generation advances, process-wide. Polymorphic dispatch over `a.Base` then
sees the importing module's struct, which is the point of publishing an
abstract family.

### Exports are closed under reachability

**Everything an exported struct reaches must itself be exported** — its field
types, its parents, its generic arguments, and its recursive edge targets
(ADR 0012). An importer rebuilds a struct from its layout, and a layout that
names a module-internal struct cannot be rebuilt.

This is checked **at export time**, so the error lands on the module that
broke its own contract rather than on whoever consumes it. The format 6
reader's rule that a recursive edge's target must be a declared struct is the
same rule, and generalizes to every field.

An unexported struct stays unconstrained: a module-internal leaf, or a whole
internal chain, may reference other internal structs freely. The closure rule
applies only from an exported root.

### The backends

*Direct wiring* describes the imported layout to `TypeBridge` exactly as it
describes a local one, and registers under the owner's qualified name; the
registry interns it. A recursive imported struct registers through
`recursive_bundle_closure`, whose closure walks the same edges format 6
records.

*Generated C++* refers to the exporting module's generated type and includes
its `public_headers`. **It does not re-declare the struct.** One C++
definition per struct means a value passes between two generated modules as
itself, with no conversion and no chance of two definitions drifting.

## Slices

1. **Export closure.** An exported struct's reachable types must be exported,
   checked on the exporting module at export time, with a message naming the
   unexported type and the field that reaches it. Lands first because it is
   what makes every later slice's input well formed.
2. **Catalog and descriptors.** `ImportedStruct`, `ImportableModule::structs`,
   `add_to_catalog` reading `Structure` declarations, catalog validation
   (duplicate names, a struct and a function of one name, unresolvable parent
   or field identity). No resolver change: the catalog is populated and
   tested on its own.
3. **Resolution.** Remove the prototype rejection; resolve a qualified type
   through the catalog; `BindingKind::ImportedStruct` and
   `ResolvedModule::imported_structs`; report a name that is not exported, a
   module that is not in the catalog, and an arity mismatch on a generic
   application. Admit a local struct inheriting an imported abstract parent.
4. **Shared passes.** Typed HIR and hgraph IR carry an imported struct by
   identity, with the termination audit ADR 0012 slice 2 established.
5. **Direct wiring.** Realize an imported struct through the type bridge under
   the owner's identity, imported parents ahead of local children; conflict
   detection when a description disagrees with a registered schema; recursive
   imported structs through the closure.
6. **Generated C++.** Refer to the exporter's type and include its headers,
   never re-declaring; the two backends agree tick for tick on an imported
   shape, including a local child of an imported family.
7. **Docs and example.** The guide's "Structured values" section gains the
   import; an example module pair exports and imports a struct and extends an
   imported family, asserted on both backends. ADR 0012's acceptance item
   closes.

## Consequences

- A module can publish a data shape, not only behaviour. Two modules that
  import one struct exchange values without conversion.
- An exported struct's layout becomes part of its module's contract: changing
  a field changes the descriptor fingerprint, and an importer built against
  the old one is rejected rather than silently mismatched.
- The registry gains no new mechanism. Interning an identical description
  under an existing name is what `bundle()` already does, and registering an
  imported parent before a local child is the ordering both backends already
  compute.
- A local child of an imported abstract parent joins that family's bundle
  hierarchy process-wide, and its generation advances. Polymorphic dispatch
  over the imported parent then sees the importing module's struct. That is
  the intent of publishing an abstract family, and it means a family's members
  are no longer all known to the module that declared it.

## Alternatives

**Bind the descriptor's schema identity without re-describing.** Rejected: it
requires the exporting module to have registered first, which orders two
independent compilations, and it gives the importer no way to detect that it
was built against a different layout. Re-describing detects exactly that.

**Copy the struct into the importer's namespace.** Rejected: it creates a
second type with equal fields, and the nominal identity rule
(types-and-expressions.md) says two separately declared structs with equal
fields are different types — so values would not cross between modules. It
also contradicts how `use` already works for functions.

## Unresolved

- **Generic application** of an imported family, `m::Pair<i64, str>`. The
  descriptor records the parameters and the specialization is named by the
  spelling both backends already use (`Pair[int, str]`), so this is expected
  to fall out of slices 3 to 6 rather than need its own decision — but it is
  not proven until slice 6 exercises it.
- **Transitive module supply.** A module that imports a struct whose parent
  or field belongs to a third module needs that third module in its catalog.
  Discovery and transitive-closure policy already belong to the driver and
  package target rather than the resolver (`module_catalog.h`), so this ADR
  adds no policy; it does add a case the driver must cover.
- **Version skew.** Two descriptors in one catalog that describe the same
  identity differently are a conflict. Slice 5 reports it at registration;
  whether the driver should refuse the catalog earlier, on fingerprints
  alone, is left open.

## Acceptance

1. A module exports a struct; a second module imports it, constructs a value,
   reads a field, and passes it to a function of the exporting module.
2. The imported type is the exporting module's type: a value built in one
   crosses to the other without conversion, and both name the same schema.
3. A recursive exported struct (ADR 0012) imports and rebuilds its edges.
4. An importer built against a changed layout is rejected with a pointed
   message, not silently mismatched.
5. Both backends agree tick for tick on a module pair that exports and
   imports a struct.
6. `hgl check` validates an importing module against a descriptor without
   loading code.
