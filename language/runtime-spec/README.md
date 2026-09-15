# hgraph semantic specification

Status: proposed foundation, revision 0. No conformance version is released.

The immediate purpose is to specify a C++ cleanup or simplification in small,
reviewable units that can be implemented and checked reliably. The specification
describes behavior, ownership, layout, and the rules connecting them. It should
also support an independent implementation, without requiring one as the first
objective. A reader should understand each unit from diagrams, focused text,
and expected traces, without reconstructing the design from C++ source.

This is a fresh start from the concepts in
[PR #796](https://github.com/hhenson/hgraph/pull/796). It does not adopt the
proposed `.hgspec` language or its generation machinery. The foundation uses
ordinary Markdown. The next experiment adds a small `.hgspec` notation with
diagrams, physical layout contracts, and expected traces; its vocabulary is
defined independently of the original prototype. No parser or generator exists.

This foundation is incomplete. It establishes the conceptual boundaries and a
small worked contract, not a specification from which the whole runtime can
already be implemented. It changes no existing runtime or HGL behavior.

The experiment is isolated to `language/runtime-spec/`. Existing runtime and
HGL guides are read-only evidence during experimentation; this folder is not
included in their navigation or builds. Corrections and proposed contracts are
recorded here until a separate integration change is appropriate.

## Reading order

Start with the [visual review and notation examples](notation/README.md). They
show the proposed authoring form through an atomic cell, a two-cell owner, and
single-level validity. The broader foundation remains useful background:

1. [Core model](core-model.md): values, types, temporal observation, wiring,
   evaluation, ownership, and their relationship to HGL.
2. [Contracts and conformance](conformance.md): how to write precise clauses,
   a worked scalar contract, and what it means to validate an implementation.
3. [Extraction and decisions](extraction-and-decisions.md): provenance from
   #796, current evidence, unresolved semantics, and the next specification
   slices.

## One semantic foundation, several authoring languages

| Document or layer | Responsibility |
| --- | --- |
| hgraph semantic specification | Logical types, graph construction, temporal behavior, lifecycle, observable effects, and conformance. |
| HGL language specification | Source syntax and how a well-formed HGL program denotes those types and computations. |
| C++ and Python authoring contracts | How native declarations and Python authoring denote the same computations, including host-value conversion. |
| Standard-library contracts | The domain, activation, state, output, error, and algebraic behavior of each operator family. |
| Target and interoperability profiles | Representation, calling convention, binary loading, serialization formats, and platform-specific capabilities. |

The core semantic model belongs to hgraph even though this work is colocated
with HGL under `language/`. HGL consumes the model; core hgraph does not depend
on the HGL compiler or syntax. Another runtime implementation need not use C++
or reproduce its classes, allocation strategy, or operation-table layout.

HGL's existing [language model](../docs/design/language-model.md),
[type extensions](../docs/design/type-extensions.md), and
[ADR 0008](../docs/design/decisions/0008-temporal-contracts-and-target-mappings.md)
are inputs to this work. This proposal does not supersede those records. As
clauses are accepted, shared behavior should have one owning specification
clause and links from the authoring-language documents.

## How to read requirements

Every clause in this folder is **proposed** unless explicitly labelled
otherwise. `MUST`, `MUST NOT`, and `MAY` describe a proposed obligation,
prohibition, and permitted choice respectively. They do not claim that an
existing implementation has been validated against the clause.

We distinguish three things throughout:

- **Proposed contract:** the behavior under discussion for the specification.
- **Evidence:** an existing design decision, implementation, or test that
  informs the contract. Evidence alone does not settle a conflicting rule.
- **Open decision:** behavior not yet specified. This blocks a completeness
  claim for the affected feature; it is not permission for arbitrary behavior.

Accepted requirements will have stable identifiers. Editorial revisions keep
the identifier; a semantic change records compatibility and version impact.
Examples illustrate their referenced rules and cannot silently add exceptions.

## Scope and completeness

The intended specification includes the internal **logical** type system,
wiring and overload resolution, all time-series families, graph execution,
dynamic graphs, state and recovery, provider lifetimes, and observable failure
and effect behavior. "Internal" does not mean that a storage layout becomes a
portable language requirement.

Physical layout is nevertheless part of the design we want to specify. A named
C++ layout contract can require exact placement, alignment, allocation ownership,
address stability, object lifetime, and resource bounds. An implementation may
satisfy the behavioral contract while failing that particular layout contract;
the [notation experiment](notation/README.md) makes both reviewable separately.

Runtime semantics alone cannot specify every user program: the operator and
native capabilities it invokes also need contracts. A conformance claim must
name a semantic version, feature profile, library contracts, and relevant
target profiles. Partial support must be stated as partial support.

A completed slice needs:

- a defined domain, legal and illegal operations, and all relevant boundaries;
- transition and observation rules, including failure and progress;
- a human explanation and finite examples with explicit expected outcomes;
- conformance cases independent of any one engine's internal representation;
- evidence against the current runtime and a recorded resolution of conflicts;
- a check that an implementor can decide each covered case without consulting
  an unspecified helper such as `canonical`, `compatible`, or `rank`.

Code generation, proof tools, and machine-readable fixture formats can be
introduced when a completed behavioral slice demonstrates their need. They
must preserve the human-readable contract and expose their coverage limits.
