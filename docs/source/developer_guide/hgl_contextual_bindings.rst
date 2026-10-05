HGL contextual local bindings
=============================

The specification in
``external/hgraph_spec/language/docs/design/contextual-local-bindings.md`` fixes
one canonical value type and one ordinary or temporal category for every local.
An annotation checks the initializer in its category; it never lifts an ordinary
value to a connection. Runtime locals hold ordinary values, including composite
values read from atomic inputs. Only an explicit temporal publication boundary
uses ``atomic<V>``; an ordinary local uses ``V``.

The checker resolves initialized locals from their initializer and typed
uninitialized locals from their first assignment. It retains that phase on the
HIR declaration and carries it to hgraph IR, allowing generated code to reserve
the correct storage without constructing an implicit initial value. Existing
path-sensitive definite assignment still rejects a read on an unassigned path.
Compound assignments check their computed result, retaining ordinary numeric
widening and permitting temporal addition with an ordinary operand.

Temporal conditional result construction is explicit. An uninitialized enclosing
result slot resolves as temporal in its child context; scalar assignments in that
construction carry a branch-output lift flag into both backends. Repeated
assignments and reads therefore use connections. After the conditional, the
category is fixed. Already initialized connections cannot accept scalar
assignments, and temporal children cannot write enclosing ordinary locals even
when the result is unused. Branch-local ordinary variables remain ordinary.

This metadata belongs to compiler preparation, not tick execution. Rebinding a
connection changes the lexical handle without redirecting prior aliases or
mutating a producer. Projection writes cannot use a temporal local to bypass
input read-only access. Neither ``let`` nor ``var`` introduces persistent state.

Shared specification fixtures exercise checking and C++ emission on every
platform. Direct execution of runtime-node fixtures requires the existing
Unix-only scripted native loader; it is omitted on Windows. The separately
compiled native contextual-binding fixture covers those node behaviors on all
platforms, including integer-to-floating initialization and reassignment,
alongside graph rebinding and temporal branch-result construction. Windows
therefore checks and emits the shared ``node_scalar_local`` and
``ordinary_widening`` cases, then executes their compiled native equivalents.
