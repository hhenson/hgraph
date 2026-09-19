# Core model

Status: proposed concepts and requirements; see the [status conventions](README.md).

hgraph describes computations over typed values that evolve through logical
time. Wiring constructs a graph of computations and connections. Execution
advances that graph through evaluations, during which selected computations
observe their inputs, update state, and publish outputs or effects.

An implementation may choose its representation, but its choices must preserve
what a program can observe. The following concepts establish that boundary.

## 1. Values, types, and representations

A **value** is data without an independent tick history: an integer, a string,
a record, or even a complete collection. "Scalar" in existing hgraph usage
often means such a non-temporal value; it does not necessarily mean a primitive.
A **value type** defines which values exist and which operations apply to them.

A **type description** expresses a type's logical meaning. A **realization**
chooses how a particular target stores and operates on values of that type.
Type identity, supported operations, and realization are related but distinct.

| Concept | Example | Required distinction |
| --- | --- | --- |
| Structural type | An ordered tuple of an integer and a string | Its structure determines identity under rules still to be enumerated. |
| Nominal type | `market.Price` and `risk.Price` | Equal fields do not make different declared types identical. |
| Parameterized type | A fixed list of three integers | Resolved type and size arguments participate as defined by the constructor. |
| Semantic capability | Equality, ordering, hashing, recordability | Capability requirements constrain where a type can be used. |
| Realization property | Alignment, inline storage, trivial destruction | A target property is not automatically part of semantic identity. |

**HG-TYPE-001 — Representation independence.** Changing only a value's storage
strategy MUST NOT change its logical type identity or the result of logical
type matching. A representation that cannot satisfy a required operation MUST
be rejected where that capability is required.

**HG-TYPE-002 — Nominal distinction.** Distinct nominal declarations MUST remain
distinct types even if their fields have the same names, order, and types.
An explicit alias to the same declaration does not create another type.
Distinctness does not itself determine subtyping or assignability.

Five relations need separate definitions: type identity, subtyping,
assignability, explicit conversion, and generic-pattern matching. Two types
can be assignable without being identical; sharing a storage layout establishes
none of these relations. REF compatibility similarly does not erase its runtime
meaning. The specification must eventually give formation and matching rules
for every constructor, including recursion, variance, constraints, and aliases.

Canonical type identity needs a semantic definition before a portable encoding
or fingerprint is selected. Interned pointers can implement equality locally;
their numeric addresses cannot establish identity across implementations.
Native types also need declared logical identities and operation contracts,
rather than names whose meaning is only "whatever this C++ class does".

## 2. Temporal shapes

A **time-series shape** specifies how data changes and how those changes can be
observed. It is not just a value type with timestamps attached. A structural
series can contain independently valid and independently modified children.

The notation below uses familiar hgraph constructor names to identify concepts;
it is not a new source grammar. `V` and `K` denote value types; `S` denotes a
time-series shape. Each constructor still needs complete formation rules.

| Shape | Meaning to specify |
| --- | --- |
| `TS[V]` | One temporal endpoint carrying a complete value of `V` on each publication. |
| `TSS[K]` | Membership in a set, with added and removed members. |
| `TSD[K,S]` | Key membership and a temporal child of shape `S` for each present key. |
| `TSL[S,N]` | Indexed temporal children; fixed size and dynamic extent need distinct rules. |
| `TSB[fields]` | Named or positional temporal children with a declared schema. |
| `TSW[V,window]` | Retained temporal samples governed by tick-count or duration bounds and readiness. |
| `REF[S]` | A temporal binding to a compatible series, with binding observations distinct from target observations. |
| `SIGNAL` input | Metadata-only observation of a connected series; HGL does not admit signal outputs. |

For example, `TS[map[K,V]]` publishes complete map values. `TSD[K,TS[V]]`
exposes membership and child changes separately. Likewise `TS[record]` and a
`TSB` with matching fields differ in validity, deltas, and activation. Matching
snapshot contents do not make these shapes interchangeable.

Queues and cyclic buffers from #796 need classification: some are value
containers, some hold semantic history, and some are implementation machinery.
Their presence in a runtime class inventory alone does not justify a new
portable temporal constructor.

## 3. Endpoints, observations, and change

An **output endpoint** is the publication surface controlled by a computation.
An **input endpoint** observes a connection to a producer. A **binding** relates
an input to the output, child, or reference adaptation that supplies it. Access
role determines allowed operations without changing the payload's value type.
A wiring **port** describes a future connection; it is not a current value.

**HG-ENDPOINT-001 — Access roles.** A consumer MUST NOT mutate a producer's
published series through its input observation interface. Mutation of an
explicitly admitted native mutable value needs its own aliasing/effect contract.

An endpoint observation includes the following when its interface admits them:

| Observation | Meaning |
| --- | --- |
| `valid` | Whether the endpoint currently supplies a valid value or structure under its shape's rules. |
| `value` | The current value or structural view; access requires the relevant validity and capability. |
| `modified` | Whether a modification is visible at this observation point in the current cycle. |
| `last_modified` | The logical time of the most recent modification, with the never-modified case specified separately. |
| `delta` | The shape-specific change view for the observation interval. |
| Binding identity | Which series is referenced, where reference observation permits this. |

Validity, modification, value equality, and collection membership are separate
dimensions. No publication, publishing an equal value, invalidating a value,
and removing a child must not be conflated. A present child may be invalid;
an absent key is not simply a child whose payload is null. Nullability inside
a value type is also distinct from temporal invalidity.

A **delta** records a temporal change; it is not generally reconstructible by
comparing two snapshots. Repeated equal publications, binding changes between
equal-valued targets, and removal/reinsertion can carry additional information.
Delta composition, normalization, empty deltas, and publication boundaries
require rules for each shape. No generic "merge the deltas" helper settles them.

**HG-VALID-001 — Single-level validity (established contract).** `all_valid`
MUST NOT recursively require descendants to be `all_valid`. For a `TSL` or
`TSB`, it requires the endpoint itself and every immediate child to be `valid`:

```text
all_valid(e) = valid(e) AND every immediate child c satisfies valid(c)
```

For `TS`, `TSD`, and `TSS`, `all_valid(e) = valid(e)`; a TSD does not walk its
values. Other shapes retain their own readiness rules rather than acquiring
a recursive traversal. This is established behavior, not an open decision.

For example, a valid outer list containing two partially populated inner lists
is `all_valid` when both inner lists are `valid`, even if neither inner list
is itself `all_valid`. If either immediate child is invalid, the outer list
is not `all_valid`. See the existing
[time-series contract and examples](../../docs/source/user_guide/concepts/time_series_types.rst).

## 4. Logical time and evaluation

**Logical time** is the time used to order graph computation. **Wall time** is
time in the executing environment. Simulation and real-time execution need
separate admission rules, but both must explain how admitted events acquire
logical time. An external event's own timestamp is another value unless the
source contract explicitly gives it scheduling meaning.

A **cycle** is an execution interval associated with logical time. It contains
ordered observation and publication points; it is not a promise that every
input changes or every node executes. Whether multiple cycles can share a
logical timestamp is an explicit outstanding decision. The worked scalar
contract uses strictly increasing cycle times to avoid deciding it by accident.

Four stages must remain distinguishable:

1. **Notification or scheduling** requests that a computation be considered.
2. **Readiness** checks whether required input validity permits evaluation.
3. **Evaluation** executes the admitted computation and its own guards.
4. **Publication** makes output changes visible under the operation's rules.

An active input can request evaluation when its observed series changes. A
passive input can still be read during an evaluation caused elsewhere.
Structural observation can subscribe to membership/binding changes rather
than every child payload update. Their exact event sets need definition.

**HG-EVAL-001 — Default versus empty policy.** A missing input-policy selection
MUST remain distinguishable from an explicitly empty selection. The former
selects the declared default; the latter selects no inputs for that policy.
Current default activation selects all temporal inputs and default readiness
requires their top-level validity. Source syntax for an explicit empty policy
is an HGL concern separate from the runtime's ability to represent it.

Do not define general evaluation as "some input is modified": scheduled sources,
timers, and nested graphs also cause work. Nor does evaluating a node necessarily
produce a tick. The scheduler contract must specify coalescing, ordering,
rescheduling, cancellation, progress, and what happens when readiness fails.

For dependency ordering, the spec must identify which publications a consumer
sees during a cycle. A fixed total order among independent pure nodes should
not be imposed unless observable behavior requires it. Sink effects, dynamic
rebinding, and feedback make ordering observable and need explicit rules.

## 5. Wiring and execution roles

**Wiring** resolves declarations and configuration into a typed graph plan.
The plan records computations, edges, policies, state, capabilities, and
required implementations. **Execution** instantiates and runs that plan.
Dynamic graph operations create child instances through specified runtime
protocols; ordinary value evaluation does not become unrestricted wiring.

The semantic roles are distinct from their implementation language:

| Role | Result |
| --- | --- |
| Value computation | Computes on admitted values/views; the caller owns temporal activation and publication. |
| Graph composition | Produces topology and wiring-time results. |
| Runtime node | Evaluates at runtime; may read inputs, retain state, and publish output. |
| Source | Introduces events through a scheduling or external-admission contract. |
| Sink | Produces specified effects without an ordinary result series. |
| Dynamic graph operation | Owns child graphs and their binding, scheduling, and teardown. |

An **operator contract** gives a callable identity, signature, domain, and
behavior. A **candidate** implements that contract for an admitted domain.
Resolution must define argument binding, defaults, generic substitution,
constraints, rejection, ranking, and ambiguity. A list of records plus a
function named `rank` is not enough for an independent implementation.

An alternative engine may implement its own resolver, provided it conforms to
the same rules and vectors. HGL, C++, and Python must not accidentally introduce
different semantics for the same operator contract. Algebraic laws belong to
specific domains and numerical policies, not to a suggestive operator name.

## 6. State, lifetime, and effects

**Semantic state** retains history necessary to determine future behavior.
A **cache** can be rebuilt from authoritative current/restored data without
recovering otherwise missing history. **Run configuration** supplies values and
capabilities associated with a graph run; it is not an unrelated process global.

**HG-STATE-001 — Cache reconstruction.** Given equivalent restored inputs,
semantic state, pending runtime work, and future external events, rebuilding a
cache MUST preserve subsequent observable outputs, validity, modifications,
deltas, and semantic effects. Only implementation cost may change. Calling a
running total a cache does not make lost history reconstructible.

This follows the direction of HGL's `state` and `cache` distinction in ADR 0008.
It does not imply that C++ `State<T>` already enforces reconstructibility or
that mixed cache and recordable state is implemented. Recovery must separately
define what pending timers, bindings, and dynamic membership are authoritative.

**Construction**, **logical initialization**, **restoration**, **start**,
**evaluation**, **stop**, and **destruction** are separate events. Stopping
performs semantic finalization; destruction ends an object's physical lifetime.
Failure during any phase needs defined cleanup for the objects and resources
already acquired. Restart and repeated lifecycle calls need their own rules.

**HG-LIFE-001 — Safe teardown.** Ending an owner's lifetime MUST NOT leave an
observer able to access its destroyed storage. Teardown MUST detach affected
bindings/subscriptions, or preserve the target's lifetime by an explicit
ownership contract. Normal node/graph stop precedes destruction of resources
needed by that stop; partial-start cleanup is specified separately.

Borrowing specifies an owner and a permitted interval. Retaining a view does
not extend either automatically. Copying, sharing, transferring ownership, and
snapshotting need distinct contracts, particularly for native mutable objects.
Memory layout can vary; permitted aliasing and observable lifetime cannot.

Provider registration makes types and implementations discoverable. Removing
them from future discovery and physically unloading their code are distinct.
Any live plan or instance requiring provider code/metadata needs a retention
contract. Registry reset, failed registration, and dependency removal also
need defined behavior. Exact leases and binary interfaces belong to profiles.

Errors and effects are outputs of the semantic model, even when they are not
ordinary value outputs. Every fallible operation needs failure identity,
failure timing, and post-failure state. Recording an error does not by itself
specify rollback, continuation, downstream evaluation, or graph termination.

## 7. The HGL connection

HGL adds source spelling and phase rules to this foundation. In particular:

- Temporalization maps HGL value/structural declarations to temporal shapes;
  `atomic<T>` preserves a complete value as one endpoint, and parameter `const`
  selects wiring-time configuration.
- A `const fn` performs value work. Default lifting into a temporal node needs
  specified input, readiness, and publication behavior; native implementation
  does not determine execution role.
- `when` expresses activation and evaluation guards. Ordered handlers, state
  writes, and output writes must preserve their source-level observations.
- `ref<T>` retains opaque binding access inside a node; compatible ordinary
  inputs can observe the target through a specified adaptation.
- `signal` provides metadata-only input observation. A native representation
  with a payload must not broaden the HGL access contract.

An HGL mapping must identify the relevant runtime clause and preserve its
observable behavior. A compiler's current fallback to "whatever the runtime
does" is a specification gap, not a rule an alternative engine can implement.
