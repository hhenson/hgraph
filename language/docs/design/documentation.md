# Documentation architecture

Status: accepted direction

## Audiences and authority

HGL documentation has four distinct jobs:

1. The **User Guide** teaches existing, observable source behavior through
   examples. It does not describe compiler internals.
2. The **Language Reference** is the normative contract for grammar, types,
   phases, visibility, and diagnostics. Until it is split into its own folder,
   the normative sections live in the developer guide's
   `syntax-and-semantics.md` and are linked from the User Guide.
3. The **Compiler Guide** describes passes, IR, lowering, tooling, invariants,
   and tests. It distinguishes the target architecture from the current
   prototype.
4. **Design records and ADRs** explain boundaries and decisions. They do not
   silently establish new source syntax.

Observable language behavior wins over implementation notes. Accepted RFCs,
once the language has them, supersede all four. Generated C++ is inspectable
compiler output, not a source-compatibility contract.

## Feature status

Every proposed feature is identified as one of:

- **implemented**: parsed, checked, lowered through every applicable backend,
  and covered by behavior tests;
- **partial**: the implemented subset and fail-closed boundary are stated;
- **provisional**: syntax or semantics are still under discussion and examples
  are not accepted programs;
- **blocked**: an unresolved language decision or missing public hgraph
  contract is named.

A user-guide example must not be described as implemented merely because it
parses. Status summaries are derived from executable example and feature-matrix
tests wherever practical. Duplicate hand-written checkpoint paragraphs are
kept short and linked to one current matrix to reduce drift.

## Executable documentation

Examples owned by the User Guide are extracted or mirrored into the checked-in
example corpus. Depending on their declared status, CI must:

- parse and resolve them;
- compare their expected diagnostics;
- run them through direct wiring;
- emit and compile their generated C++;
- compare observable ticks between supported backends.

Provisional examples use clearly invalid or fenced documentation fixtures and
are not silently weakened until they happen to parse.

## Code documentation

Each compiler component has a nearby README describing:

- its input and output representations;
- ownership and lifetime rules;
- allowed and forbidden dependencies;
- which diagnostics it owns;
- how to dump and test its result.

Public compiler interfaces document preconditions, postconditions, recovery,
and source-range behavior. Comments inside a pass explain non-obvious
invariants and language rules; they do not restate each line of code.

The source tree is expected to remain readable without opening the complete
design guide. Conversely, detailed rationale belongs in an ADR or design record
and is linked from the code rather than copied into several implementations.

## Architecture decisions

Decisions that constrain several passes use numbered files under
`design/decisions/`. Each records status, context, decision, consequences,
alternatives, and the acceptance evidence required before an implementation is
declared complete.

An ADR can accept a direction while leaving a named implementation choice open.
It cannot invent syntax to hide an unresolved language question.

## Required compiler views

The compiler provides readable views at its durable boundaries:

- source-accurate syntax, including error and missing nodes;
- typed HIR;
- hgraph semantic IR;
- formatted generated C++ and its source map;
- module descriptor, build manifest, and provider fingerprint.

These views are test and diagnostic formats during the prototype. Any format
that becomes an interchange or cache contract receives its own version and
compatibility policy first.
