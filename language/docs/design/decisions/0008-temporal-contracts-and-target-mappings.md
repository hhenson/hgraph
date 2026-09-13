# ADR 0008: temporal programming, value functions, and target mappings

Status: accepted design direction, not an implemented language extension.
`const fn` is the agreed value-function marker. The cache concept and lifecycle
are agreed; its complete declaration syntax, native-type lifecycle syntax, and
target-mapping syntax remain open. Examples below do not belong to the
compiler's accepted example corpus.

## Context

HGL needs to express the core node and graph library without binding its
language contracts to one implementation's C++ spelling. The immediate cases
are reusable value-level helpers, recoverable node-local caches, and native
types with explicit lifetime operations. An alternative C++ engine should be
able to share some mappings while replacing others; a future Rust or Zig
target may require different representations entirely.

This is a language design decision, not authorization to implement another
runtime inside the compiler. The current target remains the public C++ hgraph
SDK, including its type, operator, lifecycle, and record/replay semantics.

## HGL is a temporal programming language

HGL is a temporal programming language for expressing computations over values
that evolve through time. It combines temporal computations with value-level
functions, explicit state, and native implementations.

Change, validity, activation, and history are part of the programming model.
Graphs and nodes describe how temporal computations compose and execute; they
are not competing source-language categories. Native or imported types remain
nominal atomic values, like `i64`, `f64`, and `str`, rather than becoming
time-series types by virtue of their declaration. Their use in a temporal
parameter introduces the temporal context.

## Value-level functions

Use `const fn` to declare a non-temporal function. It executes directly on
values, or on explicitly admitted views, and has no independent activation,
output ticks, or graph topology. Its arguments and result are value-level;
they are not recursively temporalized as ordinary temporal parameters are.

An ordinary `fn` retains the current temporal callable model. Calling it at
wiring time either composes existing operations or wires a runtime node,
depending on its body. In particular, an ordinary `fn` containing `when`
does not become a value function merely because its body runs at tick time.

| Call context | Temporal `fn` or operator implementation | Value-level `const fn` implementation |
| --- | --- | --- |
| Graph construction | Compose topology or wire a node | Execute on available values, if its phase/effect contract permits wiring-time use |
| Runtime node evaluation | Cannot wire or invoke a temporal computation as a value call | Execute on current values or admitted views; the caller owns activation and output |
| Node lifecycle hook | Cannot introduce topology | Execute only if the helper is admitted in that lifecycle phase |

The marker does not mean compile-time-only, constant folding, purity, immutable
arguments, or absence of side effects. A native helper may mutate an explicitly
permitted cache argument. Mutation, allocation, I/O, borrowing, and allowed
lifecycle phases need their own contracts; `const fn` does not authorize them.
Nor may a value function declare node state, inject a node capability, or
contain `when`, `start`, or `stop` blocks. Its calls must remain value-level.

Parameter-level `const` retains its existing meaning: fixed wiring-time
configuration on a temporal callable. Function-level `const` is not shorthand
for adding that qualifier to every parameter. A source configured entirely by
fixed values can still tick, so an all-`const` signature is not evidence that
the function is non-temporal.

### HGL example and C++ expectation

The following uses the agreed, not-yet-implemented `const fn` spelling:

```hgl
const fn scale(value: f64, factor: f64) -> f64 =>
    value * factor

fn scaled(value: f64, const factor: f64) -> f64 {
    when modified(value) && valid(value) {
        return scale(value, factor)
    }
}
```

The expected C++ shape is a plain value helper called by the containing node's
evaluation hook, not another node. Illustrative lowering, not current emitter
output:

```cpp
double scale(double value, double factor) {
    return value * factor;
}

struct scaled {
    static void eval(hgraph::In<"value", hgraph::TS<double>> value,
                     hgraph::Scalar<"factor", double> factor,
                     hgraph::Out<hgraph::TS<double>> out) {
        out.set(scale(value.value(), factor.value()));
    }
};
```

For this single-input example, the node's activation/readiness policy supplies
the handler's modified/valid admission. `scale` neither schedules evaluation
nor emits the result; `scaled` does. A permitted call to `scale` on two
wiring-time scalar values instead computes a scalar immediately, without
wiring a node. A temporal port cannot be passed to that helper as a scalar in
graph composition: there is no current payload to read at wiring time.

### Operators and native implementations

Execution role and implementation language are independent. One nominal
operator may have temporal and value-level candidates, and either role may
have HGL or native implementations. A native graph implementation wires a
graph; a native value implementation directly operates on its admitted
arguments. Being native does not determine the role.

The call context first constrains candidate eligibility. Selection must retain
the operator identity, concrete type domain, signature, and applicable
constraints; it must not depend on a coincidentally equal name or rediscover
an overload on each tick. The existing hgraph resolver remains the owner of
temporal candidate matching/ranking. Extending the model to value candidates
must preserve its shared matching rules, not add an independent dispatcher.

The [fixed symbol-to-name mapping](../operators.md#fixed-symbol-to-name-mapping)
is unchanged. For example, `*` identifies `mul_`; the context and domain
determine an eligible implementation. Domain-bound algebraic properties do
not automatically transfer to a different numerical policy or candidate.

A scalar implementation does not implicitly supply a temporal implementation.
Any lifting facility must separately define activation, validity, reference
access, output/delta behavior, and result typing. Conversely, having a temporal
operator does not make it callable as scalar work inside a node.

The modifier combinations for operator implementations, native declarations,
and exports are not settled here. Existing source `native fn` value/view
helpers remain the implemented interface; their migration or compatibility
with `const fn` is follow-up work, not an immediate syntax change.

## Cache versus recordable state

The language distinguishes semantic history from reconstructible local data:

| HGL concept | Meaning | Current native counterpart | Record/replay |
| --- | --- | --- | --- |
| `state` | Persistent data needed to determine subsequent computation | `RecordableState<TSchema>` | Recorded and restored |
| `cache<T>` | Node-local data reconstructible independently of missing history | `State<T>` | Excluded; rebuilt after restart |

`cache<T>` names the agreed concept. This record does not choose the complete
declaration or initializer grammar. It does not rename C++ `State` or introduce
a native `Cache` selector.

Given the same restored inputs and recordable state, restarting with an empty
or reconstructed cache must preserve subsequent output values, validity,
ticks, deltas, and semantic side effects. Only the cost of producing them may
differ. Incremental cache maintenance is fine if the contents can also be
recovered from authoritative current data without replaying lost history just
to rebuild the cache. Rebuilding must finish before any evaluation that
depends on the contents; inputs need not be available during physical
construction, so a cache may initially be empty and explicitly not ready.

Examples:

- An index derived from the complete current input map is a cache if it can
  be rebuilt after restoration without changing observable results.
- A cached REF is suitable when current input connections and a current or
  restored selection identify its source. A historical selection known only
  to the cache is semantic state and must instead be recordable.
- A running total, last-seen value, or queue of unconsumed events is not a
  cache merely because it is stored privately. When missing history is needed
  to reconstruct it, use recordable state or an explicit temporal structure.

REF opacity and binding-change semantics remain unchanged. Caching a reference
does not grant payload access below it, serialize an engine handle, extend a
borrowed view's lifetime, or keep a handle valid after its owning graph dies.

### Construction and typing

Cache and state storage are both planned and their objects constructed during
node initialization, before `start`. There is no physical lifetime distinction
that lets cache allocation or construction be deferred to `start`. Constructing
the object is separate from populating its logical contents or restoring
recorded values. Replay-aware state initializers must not overwrite restored
state; cache reconstruction must use the restored authoritative data.

Normal teardown runs semantic `stop` before destroying the constructed
objects and releasing their storage. Partial initialization must clean up
whatever was successfully constructed without assuming `start` completed.
Detailed failure and rollback rules for native hooks remain to be specified.

Cache follows state's applicable typing and lifetime rules but does not
require recordability. It may therefore contain admitted native/non-recordable
types. HGL `state` still requires recordable types; a native type is eligible
there only if its recordability contract is supplied. Opaque native storage
does not remove this distinction. Cache is also not a blanket permission to
own external resources or introduce I/O outside a native lifecycle contract.

A node may need both recordable history and a derived cache. Supporting both
is the agreed direction. The current
[C++ static-node API](../../../../include/hgraph/types/static_node.h) explicitly rejects
combining `State` and `RecordableState`; that restriction, storage planning,
and their lifecycle integration require implementation work. HGL cache
declarations and generic native cache construction are not implemented.

For **HGL-MIG-005**, this settles the reconstructible-cache distinction, not
generic recordable-state construction. Non-default-constructible generic
state, sparse validity, queues, and windows still require their own accepted
representation and initialization rules. They must not be relabelled as
cache to bypass record/replay.

## Language contracts and target mappings

Keep four concerns distinguishable, without prescribing four new source
declaration forms:

| Concern | Information it owns |
| --- | --- |
| Semantic contracts | Nominal type and callable identities, signatures, behavior, execution roles, ownership/effects, and required capabilities |
| Requirements and use | Which contracts and capabilities a module needs, without naming a provider's implementation spelling |
| Implementations | Candidates satisfying those contracts, their domains/constraints, and HGL or native bodies |
| Target mappings | Concrete representations, lifecycle operations, symbols/wrappers, engine APIs, ABI, and build/load dependencies |

A target is more specific than an emitted language: it includes the engine,
language, and relevant ABI/platform/profile. Two C++ engines may share scalar
representations and numerical helpers while using different node/view APIs.
A Rust or Zig realization must preserve the same semantics without pretending
those C++ types or ownership operations exist there. Mapping reuse and
overrides need explicit, deterministic compatibility rules; their declaration
syntax and composition mechanism are still open.

Stable language identity is distinct from a target layout or ABI fingerprint.
Where a type already exists in hgraph, reuse its canonical identity and
operations rather than registering a duplicate identity from its HGL name.
An absent or incompatible mapping/capability must fail during compilation or
planning, not silently substitute different behavior.

### Native type realization and lifecycle

The discussion's illustrative sketch was:

```hgl
type SomeType {
    cpp { some::Type }
}
```

This is a design sketch, not accepted parser syntax. `SomeType` would be an
atomic nominal language type; `some::Type` would be a target mapping, not the
type's portable meaning. A colocated mapping could be convenient, but the
model must also permit using the contract with another target's mapping.
Native fields and layout do not become visible automatically.

A type realization needs an explicit lifecycle protocol covering:

- memory requirements, including size and alignment for a concrete realization;
- construction in supplied storage and destruction of a live object;
- supported copying, ownership transfer/move, borrowing, and sharing;
- who allocates and releases storage, separately from who constructs and
  destroys an object;
- optional capabilities such as equality, hashing, ordering, and recordability,
  required only by contexts that use them.

These are semantic capabilities, not a requirement to imitate every C++
special member on every target. Destroying an arena-owned object must not free
the containing arena. Retaining/releasing a Python-backed reference is not
automatically an independent or deep value copy. Unsupported operations must
remain unavailable rather than acquire an accidental byte-copy fallback.

Native lifecycle helpers implement this protocol; an ordinary function merely
named `init` does not become a constructor automatically. Association with the
type, receiver/result ownership, supported arguments, allowed phases, failure
handling, and cleanup need explicit contracts. Their source spelling remains
open. Node `start`/`stop` hooks and type construction/destruction are separate
layers, even when both ultimately call native functions.

## Implementation boundary and next work

The existing [descriptor model](../../../include/hgl/native_package.h)
already records native type categories, phase,
effect, and ownership information, but its `cpp_type`, `cpp_symbol`, headers,
and build metadata describe the current C++ target. It is a starting point,
not a completed target-mapping system. The exploratory runtime-contract
prototype in [PR #796](https://github.com/hhenson/hgraph/pull/796) is related
design input; this decision neither adopts its entire provisional syntax nor
claims a specification parser or generator exists.

Extend the existing typed HIR and hgraph semantic IR boundaries deliberately.
Represent execution roles and required capabilities before emission; perform
target-specific realization through an explicit mapping boundary. Emitters
must not invent language semantics or reimplement resolution. The current
compiler remains hgraph-specific; another engine or language requires a
separately validated target integration, not just a different output suffix.

Suggested bounded implementation order, not additional syntax decisions:

1. Define execution-role metadata and `const fn` checking/lowering, including
   value results and diagnostics for illegal cross-phase calls. Settle modifier
   combinations and compatibility with existing native declarations.
2. Settle cache declarations and native type-construction contracts. Add the
   public C++ path for a node containing both state categories, with pre-`start`
   construction and restart/teardown coverage.
3. Specify requirements and target mappings with one native cache type and one
   operator having value-level and temporal implementations. Separate logical
   identity from target compatibility metadata.
4. Probe mapping reuse with an alternative C++ engine and portability with a
   Rust or Zig realization. Use the same semantic conformance scenarios; do
   not claim backend support from a specification-only example.

Acceptance must cover current-value versus wiring-value calls, no accidental
temporal lifting, domain-specific operator selection, construction before
`start`, cache reconstruction after restore, partial initialization cleanup,
borrowed/REF lifetime rejection, and missing-capability diagnostics. Changes
to runtime behavior require native C++ tests and matching Python coverage
where exposed. This documentation change implements none of those extensions.
