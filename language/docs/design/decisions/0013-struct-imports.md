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
is not a copy.

**Detecting a disagreement needs work the registry does not do today.**
`bundle()` interns an identical description, but `recursive_bundle_closure`
returns `value_type(root)` as soon as the name is registered and never calls
the describer, and its concurrent path accepts an existing batch without
comparing it. So two importers built from different revisions of a recursive
struct would not collide: the stale one would silently receive the other's
layout. Acceptance item 4 therefore requires an explicit preflight comparison
in slice 5 — describe, then compare against the registered schema before
reusing it — or a registry change. The earlier claim that the existing closure
already provided this was wrong.

**The comparison is over the whole description, not its shape.** Renaming a
field's *type* is what a version bump usually looks like, and it leaves the
field count and every field name unchanged — so kind, arity and names would
call two different schemas a match. The preflight compares each field's
realized type, the parents, abstractness and the generic arguments, and names
the part that disagrees. A recursive field is compared by the **target it
names** rather than realized, because realizing the edge would need the very
type being checked; "an owner of some named bundle" would accept an edge that
owns a different struct.

**A disagreement fails; it does not merely report.** The backend aborts on a
null result, so returning the registered-but-incompatible metadata would let a
run continue against the wrong field layout and print the diagnostic
afterwards.

**Every member of the closure is compared, on the way in and on the way out.**
Two things make a root-only check insufficient. The registry describes only
what is *not* already registered, so a member that is already there is never
described and never compared — registering `A { next: atomic<B> }` against
somebody else's `B` is as wrong as registering somebody else's `A`, and
comparing an edge by the target it *names* is only sufficient because that
target is checked as a member in its own right. And
`recursive_bundle_closure` accepts whichever batch closed first, under its own
lock and without comparing descriptions, so a bridge that loses that race
finds nothing to compare on the way in and would cache the winner's layout.
The closure is therefore collected whole before anything is decided, every
registered member is compared before the close, and every member again after
it. **Every return goes through that comparison, including the one that finds
the root already registered** — another bridge can register between the
comparison and the lookup, so finding the root there is not evidence that it
agrees.

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

**Whatever crosses a module boundary crosses whole, or is refused by name.**
A partial record is worse than an absent one, because it still looks complete:
a short layout registers under the owner's name while disagreeing with the
exporter, a dropped field default rebuilds the parent's, a skipped generic
argument admits an unchecked specialization, and half a `where` requirement is
weaker than the one the exporting module declared. Each of those was a real
defect in this work. So a record that cannot be rebuilt whole records a
support error naming what failed, and carries nothing partial.

**Construction metadata does not cross yet.** A descriptor records a field's default, which
`ImportedStruct` does not carry, so an imported constructor cannot yet
reproduce the calls the exporting module accepts — an omitted argument with a
default, or an inherited default a child overrides. Such a struct records a
support error and is unavailable rather than wrong.

A generic struct's `where` requirement **does** cross (owner's ruling): the
descriptor's normalized constraint graph rebuilds into `ImportedStruct::
constraints`, whose shape mirrors both the descriptor's and the typed HIR's,
so lowering reconstructs it for the **existing** solver rather than a second
checker. It crosses whole or not at all.

`ImportedType` was built to describe **signatures**, and a layout is a richer
thing: a parameter list never names a nominal struct and never carries an
`atomic`. So the type vocabulary widens with the layouts that need it —
`nominal_identity` beside `binding_identity` (a parameter is substituted, a
struct is registered), and `ImportedTypeKind::Atomic`. `atomic` converts only
where a layout asks for it and only at a field's top level: the resolver
rejects it in value position, so it is not a signature's shape, and ADR 0012
rule 8 forbids an edge reached through a container. Expect the same widening
wherever a later slice asks a layout to say something a signature cannot.

### Resolution

A qualified `Named` type resolves through the catalog to an `ImportedStruct`,
bound as a new `BindingKind::ImportedStruct` whose `index` names
`ResolvedModule::imported_structs`, mirroring `ImportedFunction`. The
prototype rejection is removed.

**Both spellings, through one path.** `use m::{Quote}` binds the name in the
importing module and `m::Quote` names it through an alias; an unqualified name
still resolves locally first. The two forms share the binding, the arity check
and the per-argument role checks, so they cannot drift — the unqualified form
needs the `use` to bind it *and* the bare name to resolve as a type, and
missing either half makes the documented spelling fail while the alias one
works.

### The shared passes

Typed HIR and hgraph IR name an imported struct **by identity**, never by
expansion — the rule ADR 0002 already sets for the backend boundary and ADR
0012 already follows for a recursive edge's target. `ImportedTypeKind::Symbol`
with `binding_identity` is the existing representation and is reused.

**The whole closure is bound with the struct.** A parent is never spelled in
the importing module, and neither is a struct that only a *field* reaches, so
binding just the named struct leaves a backend with no layout for part of the
shape it has to register — it reports an unknown nominal type, at a name the
source never mentions. Reachability is over parents and field types alike,
which is the same closure the exporting module's export check walks.

**A cycle the layout cannot bound is refused, judged per strongly connected
component.** An owned edge bounds a cycle, so a component whose internal links
are all owned is the ADR 0012 shape; a cyclic component containing any
ordinary internal link is an infinite value. Two weaker versions of this check
were wrong in ways worth recording: removing owned edges before looking missed
a cycle running through an edge and back through inheritance, and judging each
back edge as it was found made the answer depend on field order — an all-owned
path can finish a node before the ordinary link into it is examined, and a
finished node says nothing.

**Nothing walks the closure recursively.** A descriptor is an input, so its
chain length is not this compiler's to put on a stack: the resolver's binding,
the cycle search, and typed HIR's lowering all use an explicit worklist.
Lowering orders ancestors before descendants — a descendant's flattening reads
its ancestors' fields — and queues what a field *names* as a root of its own
rather than descending into it.

**A cycle through ordinary fields or parents is refused.** It is not a layout
but an infinite value, and the local rule already says so (ADR 0012 rule 2: an
edge must be an optional `atomic`, which bounds it). An imported layout is not
exempt because another module wrote it — a backend realizing one recurses
`register_value(A)` → `value(B)` → `register_value(A)` and takes the process
with it, and two records are enough to build one.

A name the closure cannot find is **reported where the import is**, not
skipped: the module declaring it is missing from the supplied package target,
so this module's layout cannot be rebuilt whole. That is the transitive-supply
case under Unresolved, and it belongs to the driver to satisfy — but the
resolver has to say so rather than let a half-bound shape reach a backend.

**A re-described struct's fields are its whole layout, ancestors first**, the
same as a local declaration's. A *catalog record* holds only the fields it
declares, which is right for a descriptor; typed HIR is where the two meet,
so the ancestry is flattened when the struct is re-described and every later
consumer — the type checker, hgraph IR, and through it both backends — reads
one shape. hgraph's registry holds the same rule from the other side:
`bundle()` refuses a child that does not preserve its parents' fields, so a
short layout is not a subtler description, it is a registration failure. A
diamond dedupes by name; each field keeps the identity of the ancestor that
declares it.

The description is guarded against **re-entry, not just repetition**: a
recursive edge (ADR 0012) names its own struct, and the record is complete
only once its fields are lowered, so the guard covers a struct that is still
being described. A guard that only skipped already-recorded structs would not
terminate on the first recursive import.

### Inheriting an imported family

**A local struct may inherit an imported abstract parent.** Extending a family
a library publishes is a large part of why a library is worth having, so this
is part of the decision rather than a later question.

Registration needs no new mechanism: hgraph requires a parent to be registered
before its children, `bundle()` takes parents as already-registered metadata,
and an importer holds the parent's whole layout, so it registers the imported
parent and then the local child — the order both backends already compute.

**Resolution did need one, and getting it wrong is instructive.** Inheriting a
local parent *copies* its resolved fields into the child. Extending that to an
imported parent is impossible and, more to the point, wrong: a `StructField`'s
type is an `ast::TypeId` into the inheriting module's own AST, which an
imported field has no node in. The copy is the defect, not the obstacle.

An inherited struct is **referenced, and keeps the information describing its
source**. `StructField::origin` already existed to record which struct
declares a field; it was an `ast::DeclId`, so it could not name another
module's struct, and the moment a parent was imported an inherited entry
degraded into an anonymous copy. `origin` and `StructInfo::parents` are a
`StructSource` instead — a local declaration or an index into
`imported_structs` — so an inherited field says which module declares it, a
diagnostic names that module, and the importing module gains no declaration
for a struct it does not own. An inherited field's `type` stays `no_node`: its
type lives in the owner's descriptor, and synthesising a local node for it
would be the same copy by another route.

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
itself, with no conversion and no chance of two definitions drifting. The
reference is spelled with the **owner's** C++ namespace, derived from the
struct's identity the same way a module derives its own; a bare local name
would have nothing to bind to, which is the point.

**Reading a field of an imported struct is a type-checker concern, not only a
backend one.** The effective-field walk reaches a struct through its
`StructDecl`, and an imported struct has none, so it answers from the
re-description instead — whose fields are already the whole layout, so they
are the effective list as they stand.

**Constructing** a value of an imported struct — `m::Quote(...)` or
`use m::{Quote}` then `Quote(...)` — checks against the re-described layout
rather than a `StructDecl`, and refers to the struct as a struct rather than
as a binding, so both backends see a construction of the owner's type. The
two spellings share one binding and cannot drift.

**The ancestry travels with the struct, at binding time.** An ancestor is
reached only through a parent and is never spelled in the importing module, so
binding just the named struct leaves its inherited fields with nowhere to come
from. Binding the family was previously reached only when a local struct
inherited an imported parent, which left `m::Quote(...)` short of the fields
`Quote` inherits.

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

   This is larger than it reads, and each layer needs its own accommodation.
   A struct this module does not own has **no declaration here**, so: it is an
   external HIR symbol interned by the owner's identity
   (`SymbolKind::ImportedStruct`), the way an imported function already is; it
   names a type beside `SymbolKind::Struct` in the type checker; an inherited
   field carries `origin_identity` because `hir::StructField::origin` is a
   declaration id that cannot name one; that field's **type** is lowered from
   the owner's layout, since it has no AST node here; and hgraph-IR lowering
   skips the local generic-scope walk, which has no declaration to walk to.
   Losing any one of them leaves the field typeless, or anonymous, in the IR
   both backends realize from.

   **An imported struct is a record, not a declaration.** `hir::Module` gains
   `imported_structs` beside `native_functions` and `imported_operators` —
   this module declares nothing for it — and hgraph IR emits a
   `StructContract` from each, which is what both backends register the
   schema from. That record is the re-description the decision above calls
   for, carried once rather than rebuilt independently by each backend, and it
   is where the `where` slice 3c reconstructed finally attaches, so the
   existing solver checks an applied family.

   The contract is **not** marked exported: the struct is exported by its own
   module, and re-exporting it would make this module's descriptor claim a
   type it does not declare — which its own export closure would then reject.

   The whole **ancestry** is re-described, not only the struct the source
   names. An ancestor is reached through a parent and never spelled here, so
   it would otherwise go undescribed, and a backend cannot register a family
   whose ancestors it has no layout for.

   Inheriting a **generic** imported struct still refuses by name, because its
   parameters would have to map into this module's symbols and typing those
   fields against the wrong scope would be silently wrong.
5. **Direct wiring.** Realize an imported struct through the type bridge under
   the owner's identity, imported parents ahead of local children; conflict
   detection when a description disagrees with a registered schema; recursive
   imported structs through the closure.
6. **Generated C++.** Refer to the exporter's type and include its headers,
   never re-declaring; the two backends agree tick for tick on an imported
   shape, including a local child of an imported family.

   Two things the earlier slices left short surfaced here, both of them
   front-half rather than backend. Reading a field of an imported struct did
   not type-check at all, because the effective-field walk reaches a struct
   through its `StructDecl`. And a local child of an imported family could not
   be *constructed*: the field index is keyed by the declaring spelling, which
   for a local field is a view into the source text and outlives everything,
   but for a field seeded from a catalog record is a view into a record held
   by value -- so the lookup read freed memory and reported a field the struct
   plainly had. The keys own their spelling now.
7. **Constructing an imported struct.** `m::Quote(...)` and
   `use m::{Quote}` then `Quote(...)`: the qualified reference binds to the
   imported struct, the constructor validates against the re-described layout
   rather than a local declaration, and the struct is referred to as a struct
   rather than as a binding so both backends construct the owner's type. The
   ancestry is bound with the struct, not only when a local child inherits it.
8. **Docs and example.** The guide's "Structured values" section gains the
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
- **Passing an imported value back to a function of the exporting module**,
  the last clause of acceptance 1. This is not a struct question: an ordinary
  HGL `export fn` is not importable at all, because behaviour crosses a module
  boundary through an operator contract. A module that wants to publish both a
  shape and something to do with it declares an operator whose signature names
  the struct, which already works. Whether a plain exported function should
  also be importable belongs to its own decision.

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
