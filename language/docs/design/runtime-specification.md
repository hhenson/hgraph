# Backend-neutral runtime specification

Status: exploratory prototype; no generator consumes this syntax

## Purpose

HGL describes graphs and nodes. It should not also become the source language
for implementing the hgraph runtime. A separate, smaller specification language
can describe the runtime's stable type, ownership, lifecycle, and operator
contracts and generate the repetitive surface needed by a C, C++, Rust, Swift,
or another implementation.

The prototype lives in [`runtime-spec`](../../runtime-spec/README.md). Its
examples use the provisional `.hgspec` syntax so the design can be tested
against the existing C++ runtime before a parser or generator is selected.
Unresolved syntax and semantics are indexed in its
[`requirements ledger`](../../runtime-spec/requirements.md).

The specification is intended to generate:

- canonical schema records and their kind/capability enumerations;
- storage-plan and erased-operation interfaces, without fixing their backing
  representation;
- owned and borrowed API surfaces with explicit lifetime relationships;
- node, graph, and provider lifecycle interfaces;
- node category, activation, structural-observation, and validity-readiness
  metadata;
- operator-contract descriptors and deterministic registration manifests;
- backend conformance fixtures and stable descriptor data.

It is not intended to generate algorithms, allocators, schedulers, collection
representations, or language-specific performance strategies. Those remain
backend implementations of the generated contracts.

## Two languages, two responsibilities

| Layer | Source | Responsibility |
| --- | --- | --- |
| Runtime contract | `.hgspec` | Defines portable semantic shapes, operations, ownership, lifecycle, and laws. |
| Runtime backend | C, C++, Rust, Swift, ... | Implements storage, scheduling, erased dispatch, and platform integration. |
| Graph library | `.hgl` | Implements operator candidates, nodes, and graphs over the runtime contract. |
| User application | `.hgl` | Composes the public library and extension modules. |

This separation is deliberate. HGL remains a graph DSL, while `.hgspec` is an
IDL plus executable-contract language for runtime implementors.

## Semantic model before physical layout

The contract describes a sum type such as `ValueShape` or `TimeSeriesShape`.
A backend maps that sum type to its preferred representation:

- C may use tagged structs, generated function tables, and explicit owner
  handles;
- C++ may use interned metadata, storage plans, passive operation tables, and
  RAII façades;
- Rust may use enums for construction-time descriptions and thin erased
  handles at runtime;
- Swift may use value descriptors plus protocols and internal storage boxes.

The specification does not expose `std::vector`, `Vec`, `Array`, raw pointers,
reference counting, or a particular ABI. A separate profile may constrain a
binary boundary when independently built modules must interoperate.

The same rule applies to memory layout. `record`, `choice`, and `ops` describe
semantics. `derive storage_plan` asks a backend to provide a layout satisfying
the declared capabilities and laws; it does not prescribe field offsets.

## Provisional source model

Every new construct below is **provisional**. The examples deliberately make
the proposed syntax concrete; acceptance requires a parser spike and at least
two backend mappings.

```ebnf
spec            = "spec", qualified_name, "version", string, { declaration };
declaration     = scalar | enum | flags | record | choice | interned
                | ops | derive | law | scenario;
record          = "record", name, [ generic_params ], "{", { field }, "}";
choice          = "choice", name, [ generic_params ], "{", { variant }, "}";
interned        = "interned", "schema", name, [ generic_params ], block;
ops             = "ops", name, [ generic_params ], block;
derive          = "derive", name, "for", type, block;
law             = "law", name, parameter_list, block;
scenario        = "scenario", name, block;
ownership_type  = "owned", "<", type, ">"
                | "borrowed", "<", type, [ ",", lifetime ], ">"
                | "inout", "<", type, [ ",", lifetime ], ">"
                | "shared", "<", type, ">";
```

The syntax borrows familiar shapes from Rust, Swift, and schema IDLs, but the
semantics are narrower:

- `record` is product data with no inheritance or methods;
- `choice` is a closed tagged sum;
- `interned schema` has canonical structural or nominal identity;
- `ops` is a passive operation table, not object inheritance;
- `derive` lists required generated surfaces, not arbitrary macros;
- `law` is a universally quantified conformance property;
- `scenario` is a finite trace with observable assertions;
- `owned`, `borrowed`, and `shared` make lifetime transfer visible.

## Identity and interning

Schema identity must be explicit because pointer identity in C++ is an
implementation optimization, not a portable semantic definition. Each
`interned schema` declares a `key` expression built from stable values. A
backend may represent a canonical schema with an address, index, hash-consed
handle, or another token, but must satisfy:

```text
same canonical key  => same schema identity within one registry generation
different nominal key => different identity even when structure is equal
registry reset => new generation, equivalent semantic descriptions
                 still compare by canonical key
```

Nominal and structural identity are separate constructors. A named Bundle, for
example, retains its qualified name and ancestry while pointing to a structural
Bundle with the same ordered fields.

## Operations and capabilities

An operation is present only when its schema declares the corresponding
capability. This avoids nullable function pointers and keeps unsupported
behavior out of hot paths. The generated façade performs capability checks at
cold boundaries; a planned runtime path receives a non-null operation table
whose shape is already selected.

Operation declarations separate:

- `cold` registry, planning, conversion, and diagnostic work;
- `hot` evaluation operations that must not allocate or resolve types unless
  the contract explicitly permits it;
- `fallible` operations that return a typed error;
- `noexcept` operations that cannot cross the runtime failure boundary;
- mutation of `owned` storage from observation through `borrowed` views.

These terms are semantic annotations. Backends map them to their own error and
calling conventions.

## Ownership and lifetimes

The first prototype has four ownership constructors:

- `owned<T>` transfers or uniquely controls the represented runtime value;
- `borrowed<T, 'a>` cannot outlive the owner or evaluation scope named by
  `'a`;
- `inout<T, 'a>` is an exclusive mutable borrow for the named scope and does
  not transfer ownership;
- `shared<T>` retains a stable immutable value independently of a graph slot.

Passing an owned value consumes it. An ordinary borrow never mutates. Repeated
lifecycle and evaluation calls therefore receive `inout` access to an object;
they do not consume and replace it on every cycle. Generated erased-operation
references are non-null passive tables and carry no implementation inheritance.

Borrowed runtime collection iterators carry the evaluation lifetime and cannot
be stored in node state, returned across the API, or retained by callbacks.
Provider code and metadata use leases: a graph plan or instance retaining a
provider prevents physical unloading even after logical deregistration.

The prototype deliberately does not specify raw references, weak ownership,
pinning, tracing garbage collection, or a universal foreign pointer.

## Generated artifacts

A conforming generator should emit a backend package with four layers:

1. **Descriptors** — kind enums, records, stable names, fingerprints, and
   serialization.
2. **Contracts** — operation tables or protocols plus owned/borrowed façades.
3. **Registration** — canonical constructors, provider lifecycle, and operator
   manifests.
4. **Conformance** — generated law/property cases and finite lifecycle traces.

Generated source should remain readable and checked in where the consuming
project requires reviewable implementation inputs. Backend-specific code fills
named extension points; regeneration must not overwrite those implementations.

## Relationship to HGL library extraction

Real HGL library migrations should feed concrete blockers into this runtime
specification. For example:

- `pass_through_node` needs a portable capture/apply-delta operation;
- collection nodes need borrowed full-value and delta-range views;
- scheduler-driven nodes need an explicit scheduler capability;
- higher-order graphs need child-plan, binding, and keyed-lifecycle contracts;
- record/replay and module providers need leases and transactional removal.

The runtime DSL should gain a construct only after one of those migrations
requires a backend-neutral contract. It must not become a transcription of
every current C++ class.

## Validation strategy

The first useful generator must target two structurally different backends.
C++ plus Rust is the recommended pair because it exercises both passive erased
tables and an ownership-checked implementation. Swift is the next useful
mapping for protocol/value semantics; C is the ABI stress test.

Acceptance for a contract slice is:

1. the `.hgspec` parses into a deterministic semantic model;
2. at least two backends generate equivalent descriptor identities;
3. generated conformance cases observe the same value/delta/lifecycle traces;
4. hand-written backend code is isolated behind explicit extension points;
5. HGL generated against either backend needs no source changes.

## Open decisions

- Whether `.hgspec` is the final extension and whether specifications are one
  file per domain or one package-level unit.
- Whether ABI profiles belong in the core language or in backend manifests.
- How laws quantify over generated nominal schemas and finite collection
  shapes without turning the DSL into a theorem prover.
- How version evolution distinguishes compatible field additions from semantic
  changes requiring a new contract version.
- Whether operator resolution is fully specified as portable rules or exposed
  as a required service with shared conformance vectors.
- Which runtime debug/introspection views are stable public contracts.
