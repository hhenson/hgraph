# Beyond the runtime core

Status: requirements retained from the earlier specifications; incomplete.

HGL, C++ and Python can describe the same computation. Their syntax differs;
the runtime behaviour must agree. These contracts sit beside the six chapters.

## Types and wiring

Identity, subtyping, assignability, conversion and generic matching are
separate relations. Equal layout establishes none of them. Storage changes
must preserve logical identity; aliases retain it, distinct nominal types do
not share it. Portable encoding, generations, fingerprints and collision
handling still need rules. Addresses are not portable identities.

Equality, hash and recordability are semantic capabilities. Alignment and
trivial copying are physical properties. Native types need logical identities
and operation contracts. [Scalar types](scalar_types.md) owns the type rules;
formation must also cover aliases, recursion, variance, constraints and nulls.

An operator **contract** gives its identity, signature, domain and behaviour.
A **candidate** implements it. Resolution must define arguments, defaults,
roles, substitution, constraints, rejection, ranking and ambiguity. The old
examples require a more-specific candidate to win regardless of declaration
order, and distinct equally best candidates to be ambiguous. The ranking
algorithm and duplicate-candidate policy remain to be specified.

HGL temporalization, `atomic<T>`, lifting, ordered `when` handlers, opaque
references and input-only SIGNAL map to runtime rules. Source syntax stays
in the language documents; the cases here use descriptive type notation.

## Providers

The proposed removal trace: register a provider and candidate; wire a plan;
remove the provider from discovery. New resolution fails, but the plan keeps
its code and metadata alive. With no remaining dependents, unloading may be
allowed. Deregistration and unloading are separate operations.

Registration still needs transaction visibility, failure cleanup, dependencies,
reset generations and installer replay. Ownership, borrowing, exclusive
mutation and shared immutable retention need distinct lifetimes. Evaluation
borrows a node; views and iterators cannot escape through state or callbacks.

C handles, C++ views, Rust borrows and Swift facades may realize these rules.
An ABI separately fixes widths, versions, contexts, handles, view lifetimes
and error returns. Package, schema, source and binary compatibility differ.
Generation, if revisited, must leave handwritten algorithms intact and be
proved on a small two-backend example. Grammar and tooling remain deferred.

## Recovery and libraries

[Node](node.md) defines cache reconstruction from authoritative state and
pending work. Recovery also needs topology, bindings, clocks, timers, child
membership, source positions and effects. Seeding inputs alone cannot promise
exact recovery or exactly-once effects.

Map, switch, reduce, mesh, feedback, services and adaptors need library
contracts built on the core. Numeric overflow, NaN, division, Unicode and
algebraic laws need explicit scalar/operator rules.

## Earlier open questions

| ID | Owning chapter or remaining work |
|---|---|
| OPEN-01 | Scalar types; portable identity |
| OPEN-03 | Time-series and atomic cases; setter admission and multiple writes |
| OPEN-04 | Engine, graph, node; stepping and phase hooks |
| OPEN-05 | Time-series; compatibility, structural observation, expired references |
| OPEN-06 | Collections and windows; delta adapters and eviction boundaries |
| OPEN-07 | Wiring; complete resolution algorithm |
| OPEN-08 | Lifecycle; nested capture and effects after failure |
| OPEN-09 | Recovery |
| OPEN-10 | Special-node library contracts; set operators, aggregates, formatting and recording begun in [Operator contracts](operators.md) |
| OPEN-11 | Numerical and string contracts; `ln` domain and container `repr` begun in [Operator contracts](operators.md) |
| OPEN-12 | Native, resource, concurrency and ABI contracts |

There was no OPEN-02. Single-level `all_valid` is settled in TS-9.
