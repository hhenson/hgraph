# ADR 0012: recursive struct fields

Status: accepted (2026-09-19); implemented (2026-09-20), and complete since
struct imports landed (ADR 0013, 2026-09-21). Implemented
through the passes the two backends share. `check_recursive_fields` (`src/semantics/resolve.cpp`) finds
every recursive edge, admits the ones rules 2, 3, 4 and 8 allow and reports
the rule each other edge breaks (`tests/semantics/resolve_tests.cpp`); typed
HIR and hgraph IR mark each admitted edge and name its target by identity.
Both backends realize edges through hgraph's
`TypeRegistry::recursive_bundle_closure` (hgraph RFC 0041): direct wiring from
`src/wiring/type_bridge.cpp`, and generated C++ through the static schema's
`hgraph::Edge<T>` field, with the two agreeing tick for tick
(`tests/wiring/recursive-structs.hgl`,
`tests/codegen/generated_recursive_tests.cpp`). Module descriptor format 6
marks each edge in an exported struct's layout, and `hgl check` validates it
without loading code (`tests/driver/recursive-export.hgl`).
`examples/recursive-fields.hgl` and the user guide's "Recursive fields"
section show the feature. A second module imports a recursive struct and
rebuilds its edges through ADR 0013's struct imports
(`examples/struct-imports/`); the edge's mandatory `= null` is the one field
default the catalog carries, precisely so that case can cross.

## Context

A list node, a tree and an expression family are ordinary value shapes, and
recursive fields were always meant to be part of HGL. The first structured-
value slice deferred them ("There are no ... self-recursive fields in the
first slice") and they have stayed on the open-question list since.

hgraph already supports them natively (developer guide, *Recursive Bundle
fields*). A recursive field is an `Owned[T]` value schema: one owner pointer
inline, whatever `T` is; an unset field leaves it null; the pointee is
allocated on demand and deep-copied. `TypeRegistry::recursive_bundle` declares
a self-recursive named Bundle, a null field schema standing for `Owned<Self>`;
`TypeRegistry::recursive_bundles` declares a mutually recursive batch. Python
recognises a direct self reference, including `Optional[Self]`, on a
`CompoundScalar` and uses the same path.

Two properties of HGL make recursion more than a matter of removing the
check:

- one struct declaration gives both a scalar schema and a temporal shape, and
  temporalization is recursive. A temporal `Node` with a temporal `next: Node`
  is a bundle that never ends;
- a required recursive field has no finite value.

## Decision

1. **A field may name its own struct, or a struct of the same module that
   leads back to it.** That field is a *recursive edge*. No new syntax is
   introduced: a recursive edge is an ordinary field declaration.

2. **A recursive edge is optional.** It is declared `= null`. A recursive edge
   without a null default is an error, because no value of the struct could
   be completed. A value is therefore always a finite tree; there is no way to
   construct a cycle.

3. **A recursive edge is an atomic boundary, and says so.** Its declared type
   is `atomic<T>`:

   ```hgl
   struct Node {
       value: i64
       next: atomic<Node> = null
   }
   ```

   This is the existing rule that an atomic boundary belongs in the
   declaration, applied where it is forced. The marker is erased when the
   canonical scalar schema is derived, so `const Node` and `atomic<Node>` are
   the plain recursive value; temporal `Node` is a finite bundle whose `next`
   is one endpoint carrying a complete `Node` or null. A recursive edge not
   written `atomic<...>` is an error that names the fix.

4. **Generic structs recurse on themselves only.** Inside `Tree<T>`, a
   recursive edge is `atomic<Tree<T>>` with the struct's own parameters
   unchanged. `Tree<list<T>>` inside `Tree<T>` is rejected: it denotes an
   unbounded family of specializations.

5. **Mutual recursion is confined to one module** and is lowered as one batch.
   Every edge that closes a cycle obeys rules 2 and 3. Module imports are
   acyclic, so a cycle cannot cross modules.

6. **Recursion through an abstract parent is a recursive edge.** In
   `struct Add: Expr { lhs: atomic<Expr> = null }` the edge's target is the
   closed family that contains `Add`. It obeys rules 2 and 3.

7. **Values are compared, hashed, ordered and copied through their whole
   depth**, by hgraph's existing `Owned` operations. `fields(U)` and
   `field_type(U, name)` report a recursive edge as its declared type.

8. **Recursion through a container stays rejected** —
   `children: atomic<list<Node>> = null` — until hgraph can express it. The
   registry's recursive declarations accept only a direct owned edge to a
   member of the batch; `list<Self>` needs `Self`'s schema before it exists.
   This is recorded as an RFC ask on hgraph, not worked around in the
   compiler.

## Clarifications (2026-09-19)

Implementing the resolver raised four questions the rules above did not
settle. The owner decided them as follows; the rules are read with these
answers.

- **Rule 4 admits whatever hgraph can register.** A generic struct may join a
  wider cycle (`Tree<T>` and `Forest<T>`), recurse through its abstract parent
  (`struct Add<T>: Expr<T> { lhs: atomic<Expr<T>> = null }`), or be reached
  from a non-generic struct, provided the cycle reaches finitely many
  specializations: every generic argument on an edge is a parameter of the
  declaring struct or mentions none. `Tree<list<T>>` inside `Tree<T>` stays
  rejected.
- **A cycle through inheritance is rejected.** A parent's field that names its
  own descendant (`abstract struct Base { child: atomic<Leaf> = null }` with
  `struct Leaf: Base {}`) cannot be registered, because hgraph declares a
  parent before its children and a recursive batch cannot name one of its own
  members as a parent. What hgraph cannot implement, HGL does not allow.
- **Recursion through another struct's generic argument is a container edge**
  under rule 8 (`boxed: atomic<Box<Node>> = null`): the argument's schema would
  be needed before it exists.
- **Rule 2's null default is permanent.** A descendant may not replace a
  recursive edge's null default with a value.

## Consequences

- **Name resolution** (`semantics/resolve`): `check_recursive_fields` is
  the detector of recursive edges. It follows same-module structs (rule 5),
  abstract families (rule 6), generic arguments and collection elements,
  admits the edges the rules above allow and marks them on the struct's
  fields. (When this record was written the check looked only for the struct
  itself, so edges through another struct or an abstract parent passed the
  resolver and crashed direct wiring; that was fixed first, as its own
  change.)
- **Every pass that walks struct fields must terminate on a recursive type**:
  canonical types, capability and recordability checks, temporalization,
  constraint reflection, substitution, and both backends' type realization.
- **Typed HIR and hgraph IR** mark a field as a recursive edge and name its
  target by struct identity, never by expansion.
- **Direct wiring** (`wiring/type_bridge`) realizes a recursive struct through
  `recursive_bundle` or `recursive_bundles` instead of `bundle`. Its present
  field loop would recurse without end.
- **Generated C++** needs a spelling hgraph does not have. The static schema
  has `hgraph::Owned<T>`, but a `NominalBundle<...>` alias cannot name itself
  from inside its own field list, and nothing stands for `Self`. Either the
  static schema gains a self marker, or the emitter registers a recursive
  struct through the registry call. This is an hgraph change and lands there
  first. (Resolved by hgraph RFC 0041: the static schema's `Edge<T>` names a
  generated struct, which may be incomplete, and a `NominalBundle` with an
  edge registers through `TypeRegistry::recursive_bundle_closure`, the same
  operation direct wiring uses.)
- **Module descriptors** must express an edge to the enclosing struct or to a
  batch member in a struct layout. That is a format-version change (ADR 0004).
- **`delta<S>`** needs no new rule: a recursive edge is an atomic field, and
  an atomic field in a delta is replaced whole or omitted.

## Alternatives

- *An implicit atomic boundary*: `next: Node = null`, with the compiler
  stopping temporalization silently. Shorter, and the only possible meaning;
  but it makes the temporal shape differ from what the declaration says, in
  exactly the place an author most needs to see it. Rule 3's diagnostic can
  name the fix, so the explicit form costs one error message.
- *Allowing a required recursive edge and rejecting the constructor.* It moves
  a declaration error to every use.
- *A new `owned<T>` type constructor in source.* It would expose a storage
  choice hgraph makes on the language's behalf, and invents syntax for a
  question `atomic` and `= null` already answer.
- *Leaving recursion to native types.* A tree then cannot be declared,
  constructed or matched in HGL at all.

## Acceptance

- resolver tests: a direct edge, a same-module mutual pair, an edge through
  an abstract parent and a generic self edge are accepted; a required edge, a
  non-atomic edge, a changed generic argument, a container edge and a
  cross-module cycle are each rejected with a diagnostic that names the rule;
- the existing rejection test is replaced, not deleted;
- a temporal use of a recursive struct has the finite bundle shape of rule 3
  in both backends;
- construction, equality, hashing and a three-deep value round-trip through
  `eval` in the parity corpus, with identical ticks from direct wiring and
  generated C++;
- a module descriptor carrying a recursive struct is written, validated by
  `hgl check` without loading code, and imported by a second module
  (`examples/struct-imports/`, ADR 0013 slice 8: the edge's mandatory
  `= null` is the one default the catalog carries, precisely so this closes);
- an example under `examples/` and a user-guide section.
