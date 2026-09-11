# Requirements discovered by the HGL library extraction

Status: migration ledger reconciled with `main` at `36fa1113a` on 2026-09-11

The HGL files reference these identifiers from design annotations. Unresolved
source forms remain illustrative until their entries are accepted and
implemented. Implemented entries record accepted language substrate that the
remaining prototypes may use.

This recovered ledger is design input, not an extension of the accepted
language specification. `HGL-MIG-*` identifiers are deliberately separate
from the `HGL-LIB-*` blockers attached to the compiled `standard.hgl` slice.
Entries marked partial identify the implemented substrate before describing
the decision that remains open.

## Progress at a glance

**Implemented** means the stated language slice is available, not that the
whole operator family has migrated. **Partial** names working substrate and a
remaining boundary. **Open** needs a source/ABI contract before implementation.
Deferred extensions are not prerequisites for the current accepted slice.

| Requirement | Status | Still to do |
| --- | --- | --- |
| HGL-MIG-001 — module parts | Implemented | No language blocker; automatic discovery/package manifests are separate tooling. |
| HGL-MIG-002 — parameter packs | Partial | Runtime aggregate input views; minimum arity and type-pack constraints/reflection. |
| HGL-MIG-003 — algebraic properties | Implemented, scoped | Verify each candidate/domain before using a claim for optimization; richer laws/policy domains are deferred. |
| HGL-MIG-004 — scalar/native boundary | Partial | Broader kernels, imported atomic types, typed view shapes, and effect/lifetime contracts. |
| HGL-MIG-005 — recordable state | Partial | Generic state without a default, sparse state, queues/windows, and owned native-state construction. |
| HGL-MIG-006 — collection mutation | Partial | Implement the accepted functional output-mutation vocabulary and its typed C++ wrapper layer; graph-form semantics remain separate. |
| HGL-MIG-007 — delta forwarding | Open | Type-preserving capture/apply or a dedicated forwarding effect. |
| HGL-MIG-008 — output resolution | Partial | General dependent outputs and imported resolver metadata beyond current constraints/signatures. |
| HGL-MIG-009 — operator identity | Partial | Imported public contract binding and keyword/native-name aliases; symbol mapping is done. |
| HGL-MIG-010 — implementation arity | Partial | Fixed candidates refining packs, extra scalar parameters, and shared-resolver selection coverage. |
| HGL-MIG-011 — higher-order forms | Partial | Explicit switch and general callable/kernel contracts; automatic loop reductions remain deferred. |
| HGL-MIG-012 — effects/capabilities | Partial | Source/descriptor contracts for additional approved effects, throwing calls, and resources. |
| HGL-MIG-013 — library metadata | Open | Structured public documentation, stability, defaults, and compatibility metadata. |
| HGL-MIG-014 — empty input policies | Open | Explicit empty selectors, startup scheduling, and bound-but-invalid observation. |
| HGL-MIG-015 — generic publication | Partial | Open downstream-type materialization and body-visible generic reification. |

### Merged evidence and the compiled boundary

- [#809](https://github.com/hhenson/hgraph/pull/809): module parts
  ([ADR 0006](../docs/design/decisions/0006-multi-file-module-parts.md)).
- [#837](https://github.com/hhenson/hgraph/pull/837): three explicit pack
  shapes, composition traversal/forwarding, descriptor format v2, and accepted
  control contracts ([ADR 0007](../docs/design/decisions/0007-parameter-packs.md)).
- [#838](https://github.com/hhenson/hgraph/pull/838): fixed symbol identities,
  domain properties, arithmetic semantics, `//` floor division, and HGL
  `#` / `/* ... */` comments
  ([operator design](../docs/design/operators.md)).
- [#851](https://github.com/hhenson/hgraph/pull/851): public semantic
  requirements remain on `operator`; dependencies of a chosen algorithm stay
  on its `impl fn`, without constraining sibling implementations.
- [#792](https://github.com/hhenson/hgraph/pull/792),
  [#798](https://github.com/hhenson/hgraph/pull/798), and
  [#805](https://github.com/hhenson/hgraph/pull/805): compiled native
  value/view functions, the first HGL library candidates, and erased endpoint
  observations ([module status](hgl/hgraph/README.md)).
- [#803](https://github.com/hhenson/hgraph/pull/803): accepted source has
  [generated C++ snapshots](../generated/README.md); `.hgl.proposed` remains
  excluded from compiler acceptance.

The compiled surface is **not** the complete recovered family inventory:

| Source | What is available | What it does not establish |
| --- | --- | --- |
| [`native.hgl`](hgl/hgraph/native.hgl) | Typed `len`/`is_empty`; erased `valid`, `all_valid`, `modified`, `last_modified`. | General payload/delta/output access, arbitrary imported types, or throwing native helpers. |
| [`standard.hgl`](hgl/hgraph/standard.hgl) | Registered HGL `len_`/`is_empty` for strings, lists, sets, and maps. | Production identity, collection startup parity, rolling and TSB coverage (`HGL-LIB-001`–`004`). |
| [`control.hgl`](hgl/hgraph/control.hgl) | Compiled `merge`, `race`, `all_`, `any_` variadic contracts. | HGL replacements for their native implementations. |
| [`operators.hgl`](hgl/hgraph/operators.hgl) | 16 contracts and 75 native-delegating primitive materializations, including mixed numeric domains. | Production identity replacement, all native arithmetic operators, or temporal/structural/downstream domains. |
| [`std/*.hgl.proposed`](hgl/hgraph/std) | Nine review-only family designs. | Compiler acceptance, behavioral parity, or completed migration. |

No production operator replacement is claimed. Use the
[recovery checklist](recovery.md#remaining-work) for the next migration steps
and the [historical inventory](inventory.md) only as a recovery checkpoint.

## HGL-MIG-001: multi-file modules (implemented)

The current native operator identities live in one `hgraph.std` namespace, but
one source file for the entire library is not maintainable. The prototype writes:

```hgl
module hgraph.std part arithmetic
```

[`ADR 0006`](../docs/design/decisions/0006-multi-file-module-parts.md) accepts
and implements this syntax. Parts share one nominal declaration scope and one
descriptor/provider identity; a part name is an ownership label rather than a
namespace or export route. The compiler diagnoses duplicate part names and
declarations across the explicitly supplied source set while preserving source
order for diagnostics. The CLI accepts repeatable `--part` inputs and CMake
targets accept a complete `PARTS` list. Automatic discovery and a package
manifest remain separate tooling questions rather than missing language
semantics.

## HGL-MIG-002: variadic and keyword parameter packs (partial)

`merge`, `all_`, `any_`, `race`, `format_`, `print_`, `log_`, `map_`, and
other current contracts accept variadic inputs or keyword bundles. The
accepted forms are:

```hgl
operator homogeneous<T>(values: ...T) -> T
operator positional<...Ts>(values: ...Ts) -> i64
operator keyword<...Fields>(values: ...{Fields}) -> i64
```

[ADR 0007](../docs/design/decisions/0007-parameter-packs.md) implements
signatures, calls, composition traversal and exact forwarding, name/type
preservation, descriptors, and `VarIn`/`VarKwIn` emission. Packs have no default
and may be empty; an empty homogeneous pack must infer its element type from
another position. The old `...{str: T}` sketch is superseded, not an additional
accepted pack form. Candidate ranking remains the native resolver's job.

Still open: runtime-node aggregate input-view ABI, minimum arity, and type-pack
reflection/constraints in `requires`. A compiled variadic contract does not
make a runtime variadic implementation available. Implementation arity changes
are tracked separately in HGL-MIG-010.

## HGL-MIG-003: operator algebra properties (implemented, scoped)

Associative reductions cannot be inferred from an operator spelling. The
accepted syntax binds the property domain to the operator's generic parameters
in declaration order:

```hgl
operator add_<L, R, O>(lhs: L, rhs: R) -> O
properties<str, str, str> { associative, identity = "" }
properties<i64, i64, i64> { commutative, identity = 0 }
```

The implemented vocabulary is `associative`, `commutative`, and `identity`.
Checking, HIR, graph IR, and descriptor emission/loading are covered. Laws
require fixed binary signatures with equal input types;
associativity and identity also require `(T, T) -> T` closure. Commutativity
can describe equality's `(T, T) -> bool`. Packs, `ref`, and `signal` are not
admitted law domains. Identity literals are checked after domain substitution.

These are claims, not proofs or optimizer permissions. Signed overflow,
floating rounding, NaNs, infinities, and signed zero prevent unconditional
associativity claims for ordinary numeric addition/multiplication. Native
kernel flags are independently conservative; HGL metadata does not overwrite
them. Result types and lifting belong in signatures (`i64 / i64 -> f64`,
`i64 // i64 -> i64`), not in an algebraic or loss annotation.

The [operator design](../docs/design/operators.md) and
[paired HGL/C++ scenarios](../docs/developer-guide/operator-cpp-mappings.md)
own the details. Verification of selected candidates and policies is required
before any future optimizer consumes claims. Inverse/group/field vocabularies,
partial/const-generic/policy-dependent domains, and automatic reduction
inference are deliberately deferred, not missing parts of this syntax slice.

## HGL-MIG-004: runtime scalar primitive boundary (partial)

An HGL implementation of temporal `add_` naturally evaluates `lhs + rhs`
inside a node. The compiler now lowers that runtime scalar expression directly
rather than recursively wiring the temporal `add_` candidate. The remaining
work is to keep error, overflow, and conversion behavior identical in
scripted and generated modes as the scalar surface expands. The implemented
numeric slice now aligns constant, graph, and node division, floor division,
and modulo with native kernels, including negative operands and floating
remainder edge cases; it is not a claim about every temporal overload.

Complex scalar algorithms such as regular expressions, JSON codecs, timezone
resolution, Arrow operations, and optimized numeric kernels are intended to
remain constrained native functions described by module descriptors; their
HGL publication is not complete. The compiled and installed
`hgraph.native` module now supplies concrete `len` and `is_empty` overloads for
strings and explicitly declared list, set, map, and tick-count rolling input
views. It also supplies `valid`, `all_valid`, `modified`, and `last_modified`
over a payload-erased `signal` input view, covering every standard temporal
shape, including references and duration windows. These calls require direct
live inputs; `signal` remains input-only and exposes no payload.

Typed `len`/`is_empty` still lack duration-window, nominal-bundle, and reference
input patterns. Descriptor format v2 adds packs but does not change integral
constant-generic extents into duration values. Imported C++/Python value types
remain atomic scalar values, not time-series schemas; their HGL publication
and value-operation contracts are still work, not inferred from host headers.
See the [native gap table](../docs/design/native-interface.md#exact-native-value-and-view-functions)
for equality, hashing, ordering, formatting, metadata, and lifetime boundaries.

## HGL-MIG-005: generic recordable state (partial)

`dedup`, `take`, and `drop` can use existing output/state syntax. Other stream
nodes need generic state with no natural default, sparse validity, queues, or
windows. Decide whether this is expressed through optional state cells,
constructor functions, or an admitted native opaque state type. State that
affects later output remains recordable; scratch caches and external resources
must not be disguised as recordable state.

## HGL-MIG-006: collection output mutation and delta ranges (partial)

The compiler implements the agreed borrowed `added`, `modified`, and `removed`
ranges across `elements`, `values`, `items`, and `keys` where the collection
kind supports them. The output-mutation vocabulary is function-shaped and is
now fixed as follows:

| Output | Operation | Contract |
| --- | --- | --- |
| `set<T>` | `insert(out, value)` | Strictly add an absent member; an already-present member is an error. |
| `set<T>` | `upsert(out, value)` | Ensure membership; an already-present member is a no-op. |
| `set<T>` | `remove(out, value)` | Strictly remove a present member; an absent member is an error. |
| `set<T>` | `discard(out, value)` | Remove when present; absence is a no-op. |
| `set<T>` | `clear(out)` | Remove every member. |
| `map<K, V>` | `insert(out, key, value)` | Strictly create an absent child with a complete initial value. |
| `map<K, V>` | `update(out, key, value)` | Strictly write an existing child. A typed `delta<V>` may be admitted when delta application is implemented. |
| `map<K, V>` | `upsert(out, key, value)` | Create or write a child. The insertion case requires a complete initial value. |
| `map<K, V>` | `remove(out, key)` | Strictly remove a present key. |
| `map<K, V>` | `discard(out, key)` | Remove when present; absence is a no-op. |
| `map<K, V>` | `clear(out)` | Remove every key. |
| unbounded `list<V, _>` | `push(out, value)` | Append one initialized trailing child. |
| unbounded `list<V, _>` | `pop(out)` | Remove the trailing child; an empty list is an error. |
| unbounded `list<V, _>` | `clear(out)` | Remove every child. |

`update` has no set overload because a set has membership but no independently
updatable child value. Fixed lists retain indexed child writes and have no
structural mutation operations. Unbounded lists initially expose only the
stack-shaped `push`/`pop` surface; raw `resize` remains an implementation
mechanism rather than HGL syntax. The first `pop` slice is an effect and does
not return a borrowed child. A future result-bearing form must return an owned
snapshot whose invalid/empty cases have a separately accepted type contract.

The names describe HGL and a matching public functional C++ surface. This adds
free functions without renaming or altering existing C++ `Out` or View member
functions. Generated C++ remains readable by calling constrained wrappers such
as `hgraph::insert(out, key, value)` and `hgraph::upsert(out, value)`; those
wrappers validate strict preconditions and delegate to the current typed output
selectors and raw mutation views. The compiler applies the same collection,
key, child-value, fixed-versus-unbounded, and complete-initial-value constraints
before emission.

The wrapper layer maps to the existing members rather than adding another
mutation engine:

| Functional operation | Existing-member implementation |
| --- | --- |
| set `insert` | Reject `contains(value)`, then call `add(value)`. |
| set `upsert` | Call idempotent `add(value)`. |
| set `remove` | Require `contains(value)`, then call `remove(value)`. |
| set `discard` | Call tolerant `remove(value)`. |
| map `insert` | Reject `contains(key)`, then write through `set`/the typed child output. |
| map `update` | Require `contains(key)`, then write through the existing child. |
| map `upsert` | Write through create-on-access `set`/the typed child output. |
| map `remove` | Require `contains(key)`, then call `erase(key)`. |
| map `discard` | Call tolerant `erase(key)`. |
| set/map `clear` | Call the existing `clear()`. |
| unbounded-list `push` | Grow with `resize(size + 1)` and initialize the new child as one exception-safe operation. |
| unbounded-list `pop` | Require a non-empty list, then call `resize(size - 1)`. |
| unbounded-list `clear` | Call `resize(0)`. |

The typed free-function constraints reject mismatched key/value types, map-only
operations on sets, structural operations on fixed lists, and mutation through
an input view. Strictness is implemented in the wrapper and is therefore not a
change to the tolerant raw members.

Strict preconditions observe the staged collection at the point of the call,
while the published delta is reconciled against membership at the beginning of
the evaluation. Consequently an absent key followed by `insert` then `update`
is one addition with the last child value; `insert` then `remove` cancels; a
present key followed by `remove` then `insert` is a modification; repeated
writes to one child use the last write; and writes to distinct children
accumulate.

Structural removal and child invalidation are different effects:

```hgl
remove(out, key)       # remove key membership and publish it through `removed`
invalidate(out, key)   # retain the key but invalidate its existing child
```

The keyed form is strict and does not use create-on-access lookup. Indexed
invalidation likewise does not grow an unbounded list. Value-level `null`,
temporal invalidation, and structural removal remain three distinct states.
All borrowed collection ranges and projected children remain evaluation-scoped
and cannot be stored, returned, or captured by a longer-lived value.

These effects are initially for runtime-node bodies using `inject out`. Their
function shape reserves the same names for possible graph overloads. A graph
form would accept an ordinary temporal collection and return a new collection
port; it would not mutate its input. Its activation, validity, and state
semantics require a separate decision before acceptance.

## HGL-MIG-007: delta capture and forwarding

`pass_through_node` requires the complete input delta, including structural and
collection removals, and applies it to an output of the same temporal schema.
`delta(value)` currently has an open result shape. The migration needs a
first-class, type-preserving delta value or a dedicated `forward_delta` effect;
it must work through the public runtime contract and across alternative
backends.

## HGL-MIG-008: output and type resolution (partial)

Many operators determine an output not expressible as a simple repeated generic
parameter: `convert`, `combine`, `collect`, `split`, frame joins, structural
field access, and higher-order calls. Current C++ candidates use type patterns,
type arguments, `resolve_default_types`, and result resolvers.

Implemented substrate includes independent input/output generics, nominal
operator requirements, and positive-conjunction type equalities with field
reflection. A selected numeric candidate can already have a different result
type from its operands. See
[requirements and type constraints](../docs/user-guide/functions.md#requirements-and-type-constraints).

Still needed are general dependent output schemas, imported resolver metadata,
and the collection/frame/higher-order cases not expressible by those existing
rules. These must lower to the shared hgraph resolver, not add a second ranking
or type-inference algorithm. A generic `O` in a prototype is not a resolver.

## HGL-MIG-009: source names versus native registry names (partial)

The [fixed symbol-to-name mapping](../docs/design/operators.md#fixed-symbol-to-name-mapping)
is agreed and implemented, including `*` to `mul_`, `/` to `div_`, and `//` to
`floordiv_`. Symbols select stable system identities independently of local
short names; no `symbol = ...` clause or Python dunder names are introduced.

Native operator names include HGL keywords (`const`, `default`) and internal
names beginning with `__`. The library needs an explicit, reviewable mapping
between a legal source declaration and the stable native operator identity.
Renaming a source symbol must not accidentally create a new overload family.

General imported operator-contract publication/binding is still missing.
Compiled `hgraph.std.*` and `hgraph.operators.*` contracts remain parallel
identities, even when their bodies delegate to native operators. This is also
`HGL-LIB-001` in the compiled slice. The prototype leaves keyword-colliding
declarations commented out rather than inventing an alias attribute.

## HGL-MIG-010: graph/runtime implementation arity (partial)

Local signature conformance, explicit pack contracts, and the separation of
public `operator` requirements from candidate-local `impl fn` requirements are
implemented. Algorithm dependencies belong to the candidate; they must not
leak into unrelated sibling implementations.

Native operator candidates may refine a variadic contract with fixed arity or
add implementation-specific scalar parameters. Define how an `impl fn`
declares that relationship, how calls discover extra parameters, and how
candidate ranking compares fixed and packed forms.

## HGL-MIG-011: compiler-owned higher-order forms (partial)

`map_`, `reduce`, `switch_`, `dispatch_`, `mesh_`, and `try_except` carry
callables, child graphs, binding modes, and lifecycle semantics. Much of their
behavior is a compiler lowering rather than an ordinary library function.
Specify the minimal kernel contracts they target and which public convenience
overloads can still be implemented in HGL.

Existing temporal `if` lowering already uses native switch child graphs with
input capture, result remapping, reference forwarding, and sink branches.
Fixed graph iteration and the accepted dynamic per-element map-like slice
also exist; neither implies general higher-order library support.
[Explicit `switch`](../docs/design/switch.md) is agreed but still awaits
parsing/checking/lowering, including constant cases and unmatched-case failure
without a default. Its [HGL/C++ scenarios](../docs/developer-guide/control-flow-cpp-mappings.md)
remain design fixtures.

General map/reduce/mesh callable signatures and convenience overload coverage
remain work. Automatic graph-loop accumulations and predicate-to-switch
conversion are on the back burner: unordered map reductions and order-sensitive
linear list reductions are documented options, not implemented loop lowering
([iteration](../docs/design/iteration.md)).

## HGL-MIG-012: effects and approved capabilities (partial)

I/O, logging, engine control, scheduling, record/replay, and exception capture
need effects visible to checking and optimization. `inject logger` is already
available, but arbitrary file/network access remains outside HGL. The library
needs a closed capability/effect vocabulary shared with native descriptors and
the backend-neutral runtime specification.

HIR/descriptor phase, ownership, and effect metadata and a constrained native
function path exist. Source-native evaluation functions are currently
non-blocking and `noexcept`; an arbitrary C++ body is not permission to publish
an unenforceable contract. Throwing equality/formatting, owned opaque-state
construction, additional source effect declarations, and resource lifecycle
are still separate work.

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

This is also why collection startup/never-valid behavior is still blocked by
`HGL-LIB-002`; implemented `when {}` defaults do not close that parity gap.

## HGL-MIG-015: open generic implementation publication (partial)

`instantiate op<A, ...>` gives a module a precise set of operator candidates.
A concrete argument closes that generic position; `_` retains it in the
candidate signature for the resolver to select. This is enough for finite
element domains with open marker dimensions: the proposed `sum_` candidates use
`sum_<i64, _>` and `sum_<f64, _>`, requiring a concrete accumulator type while
accepting every fixed-list size through one candidate per element type.
Those `sum_` bodies remain review-only, not compiled standard-library coverage.

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
