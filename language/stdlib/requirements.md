# Requirements discovered by the HGL library extraction

Status: active prototype ledger; entries are questions, not accepted syntax

The HGL files reference these identifiers from `PROVISIONAL` comments. A source
form is illustrative until its entry is accepted and implemented.

This recovered ledger is design input, not an extension of the accepted
language specification. `HGL-MIG-*` identifiers are deliberately separate
from the `HGL-LIB-*` blockers attached to the compiled `standard.hgl` slice.
Entries marked partial identify the implemented substrate before describing
the decision that remains open.

## HGL-MIG-001: multi-file modules

The current native operator identities live in one `hgraph.std` namespace, but
one source file for 207 names is not maintainable. The prototype writes:

```hgl
module hgraph.std part arithmetic
```

Required semantics include one nominal declaration scope, deterministic part
ordering for diagnostics only, duplicate-declaration checks across files, one
descriptor/provider identity, and no declaration re-export. An alternative is
a package manifest that declares several source files as one module without new
source syntax.

## HGL-MIG-002: variadic and keyword parameter packs

`merge`, `all_`, `any_`, `race`, `format_`, `print_`, `log_`, `map_`, and
other current contracts accept variadic inputs or keyword bundles. The
prototype uses `...T` and `...{str: T}`. It still needs rules for minimum
arity, heterogeneous packs, name preservation, type unification, defaults,
ranking, and generated C++ signatures.

## HGL-MIG-003: operator algebra properties

Associative reductions cannot be inferred from an operator spelling. The
prototype uses an illustrative declaration clause:

```hgl
operator add_<T>(lhs: T, rhs: T) -> T
properties associative, commutative
```

Properties must apply to a precise candidate/type domain and state numerical
exceptions. For example, mathematical addition is associative while floating-
point addition is not exactly associative. This metadata affects legal graph
transformations and therefore needs verification rather than trust.

## HGL-MIG-004: runtime scalar primitive boundary (partial)

An HGL implementation of temporal `add_` naturally evaluates `lhs + rhs`
inside a node. The compiler now lowers that runtime scalar expression directly
rather than recursively wiring the temporal `add_` candidate. The remaining
review is to keep error, overflow, and conversion behavior identical in
scripted and generated modes as the scalar surface expands.

Complex scalar algorithms such as regular expressions, JSON codecs, timezone
resolution, Arrow operations, and optimized numeric kernels remain constrained
native functions described by module descriptors. The compiled and installed
`hgraph.native` module now supplies concrete `len` and `is_empty` overloads for
strings and explicitly declared list, set, map, and tick-count rolling input
views. Other borrowed endpoint shapes must be admitted deliberately rather than
inferred from C++ headers. Duration rolling windows also remain outside
descriptor ABI v1 because its constant generic values are integral.

## HGL-MIG-005: generic recordable state

`dedup`, `take`, and `drop` can use existing output/state syntax. Other stream
nodes need generic state with no natural default, sparse validity, queues, or
windows. Decide whether this is expressed through optional state cells,
constructor functions, or an admitted native opaque state type. State that
affects later output remains recordable; scratch caches and external resources
must not be disguised as recordable state.

## HGL-MIG-006: collection output mutation and delta ranges (partial)

The compiler implements the agreed borrowed `added`, `modified`, and `removed`
ranges across `elements`, `values`, `items`, and `keys` where the collection
kind supports them. Incremental set/map/list implementations still need typed
output mutations such as insert, erase, update, clear, and resize. The
prototype uses function-shaped `insert(out, value)` and `erase(out, value)`
placeholders rather than member methods.

These operations must preserve last-write-wins per child, accumulate distinct
child updates, distinguish removal from invalidation, and never expose borrowed
iterators beyond the evaluation.

## HGL-MIG-007: delta capture and forwarding

`pass_through_node` requires the complete input delta, including structural and
collection removals, and applies it to an output of the same temporal schema.
`delta(value)` currently has an open result shape. The migration needs a
first-class, type-preserving delta value or a dedicated `forward_delta` effect;
it must work through the public runtime contract and across alternative
backends.

## HGL-MIG-008: output and type resolution

Many operators determine an output not expressible as a simple repeated generic
parameter: `convert`, `combine`, `collect`, `split`, frame joins, structural
field access, and higher-order calls. Current C++ candidates use type patterns,
type arguments, `resolve_default_types`, and result resolvers.

The HGL contract needs associated type expressions or declarative resolution
functions that lower to the shared hgraph resolver. It must not add a second
ranking or type-inference algorithm.

## HGL-MIG-009: source names versus native registry names

Native operator names include HGL keywords (`const`, `default`) and internal
names beginning with `__`. The library needs an explicit, reviewable mapping
between a legal source declaration and the stable native operator identity.
Renaming a source symbol must not accidentally create a new overload family.

The prototype leaves keyword-colliding declarations commented out rather than
inventing an attribute spelling.

## HGL-MIG-010: graph/runtime implementation arity

Native operator candidates may refine a variadic contract with fixed arity or
add implementation-specific scalar parameters. Define how an `impl fn`
declares that relationship, how calls discover extra parameters, and how
candidate ranking compares fixed and packed forms.

## HGL-MIG-011: compiler-owned higher-order forms

`map_`, `reduce`, `switch_`, `dispatch_`, `mesh_`, and `try_except` carry
callables, child graphs, binding modes, and lifecycle semantics. Much of their
behavior is a compiler lowering rather than an ordinary library function.
Specify the minimal kernel contracts they target and which public convenience
overloads can still be implemented in HGL.

## HGL-MIG-012: effects and approved capabilities

I/O, logging, engine control, scheduling, record/replay, and exception capture
need effects visible to checking and optimization. `inject logger` is already
available, but arbitrary file/network access remains outside HGL. The library
needs a closed capability/effect vocabulary shared with native descriptors and
the backend-neutral runtime specification.

## HGL-MIG-013: library documentation and compatibility metadata

The native headers currently own operator documentation, parameter meanings,
complexity notes, defaults, and Python examples. A migrated HGL declaration
needs structured documentation and stability metadata from which C++, Python,
and HGL surfaces can be generated without making comments executable.

## HGL-MIG-014: explicit empty input policies

The agreed handler defaults give `modified()` and `valid()` the complete
temporal parameter list, and omitted selectors receive those same defaults.
They therefore cannot also represent hgraph's explicit empty selector sets.

A separate source form is required for nodes that are activated only by a
scheduler and for implementations that intentionally admit invalid inputs.
The backend-neutral runtime model already distinguishes `none` (runtime
default) from `some([])` (explicitly empty); HGL syntax and flow analysis for
selecting `some([])` remain unresolved.

## HGL-MIG-015: open generic implementation publication (partial)

`instantiate op<A, ...>` gives a module a precise set of operator candidates.
A concrete argument closes that generic position; `_` retains it in the
candidate signature for the resolver to select. This is enough for finite
element domains with open marker dimensions: `sum_` publishes
`sum_<i64, _>` and `sum_<f64, _>`, requiring a concrete accumulator type while
accepting every fixed-list size through one candidate per element type.

Several core-library implementations are intentionally open. `sample<T>`,
`filter_<T>`, `merge<T>`, and the generic sinks must accept types declared by a
downstream module. Retention also does not imply body availability. A
signature-only `SIZE<"size">` marker cannot satisfy an implementation that
genuinely reads the selected value.

Collection length does not require body-visible generic reification. The
compiled `standard.hgl` slice calls the native `len(value)` overload, which
receives the live typed input view and reads its current size. Retained element,
key, value, and list-size positions remain selection-only. This solves the
implemented list/set/map metadata cases without per-tick schema inspection.
Rolling candidates remain omitted because their retained extents cannot yet
materialize the required concrete input schema; `is_empty` has the same
boundary.

The design must choose who owns later open-type materializations and how
resolver-selected generics are reified when a body needs them: a consuming AOT
module, a portable descriptor-backed implementation factory, or an explicit
body-availability contract on a retained candidate. The choice must preserve
one operator identity, ordinary overload ranking, module lifecycle removal,
readable generated code, and compatibility across backend languages. Until
that is settled, the prototype keeps those templates visibly unmaterialized
rather than implying that a short built-in type list is complete.
