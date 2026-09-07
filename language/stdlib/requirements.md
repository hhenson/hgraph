# Requirements discovered by the HGL library extraction

Status: active prototype ledger; entries are questions, not accepted syntax

The HGL files reference these identifiers from `PROVISIONAL` comments. A source
form is illustrative until its entry is accepted and implemented.

## HGL-LIB-001: multi-file modules

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

## HGL-LIB-002: variadic and keyword parameter packs

`merge`, `all_`, `any_`, `race`, `format_`, `print_`, `log_`, `map_`, and
other current contracts accept variadic inputs or keyword bundles. The
prototype uses `...T` and `...{str: T}`. It still needs rules for minimum
arity, heterogeneous packs, name preservation, type unification, defaults,
ranking, and generated C++ signatures.

## HGL-LIB-003: operator algebra properties

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

## HGL-LIB-004: runtime scalar primitive boundary

An HGL implementation of temporal `add_` naturally evaluates `lhs + rhs`
inside a node. The compiler must define that runtime scalar expression as a
direct scalar operation, not recursively wire or resolve the temporal `add_`
candidate being implemented. Error, overflow, and conversion behavior must be
the same in scripted and generated modes.

Complex scalar algorithms such as regular expressions, JSON codecs, timezone
resolution, Arrow operations, and optimized numeric kernels remain constrained
native functions described by module descriptors. The existing native
interface must be extended deliberately when a primitive needs a borrowed
endpoint or collection view rather than canonical owned scalar values.

## HGL-LIB-005: generic recordable state

`dedup`, `take`, and `drop` can use existing output/state syntax. Other stream
nodes need generic state with no natural default, sparse validity, queues, or
windows. Decide whether this is expressed through optional state cells,
constructor functions, or an admitted native opaque state type. State that
affects later output remains recordable; scratch caches and external resources
must not be disguised as recordable state.

## HGL-LIB-006: collection output mutation and delta ranges

The target iterator design describes `added`, `modified`, and `removed` ranges,
but the compiler does not yet implement every `elements`/predicate form.
Incremental set/map/list implementations also need typed output mutations such
as insert, erase, update, clear, and resize. The prototype uses function-shaped
`insert(out, value)` and `erase(out, value)` placeholders rather than member
methods.

These operations must preserve last-write-wins per child, accumulate distinct
child updates, distinguish removal from invalidation, and never expose borrowed
iterators beyond the evaluation.

## HGL-LIB-007: delta capture and forwarding

`pass_through_node` requires the complete input delta, including structural and
collection removals, and applies it to an output of the same temporal schema.
`delta(value)` currently has an open result shape. The migration needs a
first-class, type-preserving delta value or a dedicated `forward_delta` effect;
it must work through the public runtime contract and across alternative
backends.

## HGL-LIB-008: output and type resolution

Many operators determine an output not expressible as a simple repeated generic
parameter: `convert`, `combine`, `collect`, `split`, frame joins, structural
field access, and higher-order calls. Current C++ candidates use type patterns,
type arguments, `resolve_default_types`, and result resolvers.

The HGL contract needs associated type expressions or declarative resolution
functions that lower to the shared hgraph resolver. It must not add a second
ranking or type-inference algorithm.

## HGL-LIB-009: source names versus native registry names

Native operator names include HGL keywords (`const`, `default`) and internal
names beginning with `__`. The library needs an explicit, reviewable mapping
between a legal source declaration and the stable native operator identity.
Renaming a source symbol must not accidentally create a new overload family.

The prototype leaves keyword-colliding declarations commented out rather than
inventing an attribute spelling.

## HGL-LIB-010: graph/runtime implementation arity

Native operator candidates may refine a variadic contract with fixed arity or
add implementation-specific scalar parameters. Define how an `impl fn`
declares that relationship, how calls discover extra parameters, and how
candidate ranking compares fixed and packed forms.

## HGL-LIB-011: compiler-owned higher-order forms

`map_`, `reduce`, `switch_`, `dispatch_`, `mesh_`, and `try_except` carry
callables, child graphs, binding modes, and lifecycle semantics. Much of their
behavior is a compiler lowering rather than an ordinary library function.
Specify the minimal kernel contracts they target and which public convenience
overloads can still be implemented in HGL.

## HGL-LIB-012: effects and approved capabilities

I/O, logging, engine control, scheduling, record/replay, and exception capture
need effects visible to checking and optimization. `inject logger` is already
available, but arbitrary file/network access remains outside HGL. The library
needs a closed capability/effect vocabulary shared with native descriptors and
the backend-neutral runtime specification.

## HGL-LIB-013: library documentation and compatibility metadata

The native headers currently own operator documentation, parameter meanings,
complexity notes, defaults, and Python examples. A migrated HGL declaration
needs structured documentation and stability metadata from which C++, Python,
and HGL surfaces can be generated without making comments executable.
