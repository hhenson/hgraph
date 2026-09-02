# Developer Guide

This guide explains how to implement the source contract in the
[User Guide](../user-guide/README.md). The compiler is an authoring frontend
for hgraph, not a second runtime.

> **Implementation status:** the current project contains the `hgl` command
> scaffold but no lexer, semantic IR, function classifier, or C++ generator.

## Guide map

1. [Syntax and semantics](syntax-and-semantics.md) defines `fn` syntax,
   `const` parameters, canonical types, recursive temporalization, and
   `atomic<T>`, plus provisional state, injectable, lifecycle, activation, and
   output semantics.
2. [Compiler and C++ lowering](compiler-and-lowering.md) defines the frontend
   pipeline, function classification, public SDK lowering,
   source mapping, and build manifests.
3. [Testing and compatibility](testing-and-compatibility.md) defines syntax,
   type-shape, classification, generated-code, installed-SDK, and cross-mode
   acceptance.

The design records provide project boundaries and rationale:

- [Architecture](../design/architecture.md)
- [Language model](../design/language-model.md)
- [Modules and native extensions](../design/modules.md)
- [Roadmap](../design/roadmap.md)

## Sources of truth

When documents disagree, resolve them in this order:

1. accepted language RFCs, once the RFC process exists;
2. the User Guide for observable behavior;
3. this guide for compiler and tooling behavior;
4. design records for architectural constraints;
5. examples, which must be updated with their owning guide.

Generated C++ is an implementation artifact, not a source compatibility
surface.

## Current invariants

- `fn` is the only user-facing callable declaration.
- Ordinary parameters and results use canonical recursively temporal types.
- `atomic<T>` stops recursive temporalization at `T`.
- `const` parameters are wiring-time values and use their canonical value type
  directly.
- Source does not spell hgraph `TS`, `TSB`, `TSL`, or `TSD` wrappers.
- Source does not expose endpoint `.value`, `.valid`, or `.modified` members.
- Imported calls use hgraph's registry, normalization, ranking, and candidate
  diagnostics.
- A body without runtime-only constructs is classified as composition;
  `state`, `inject`, `start`, `when`, or `stop` classifies the complete `fn` as
  a runtime node.
- Runtime `when` predicates are decomposed into activation, validity admission,
  and residual per-evaluation logic where possible.
- State declarations aggregate into one recordable state value; grouped
  inject declarations map approved capabilities to native selectors.
- `return value` is terminating output, while `inject out` enables persistent
  output inspection and incremental mutation.
- Classification remains a semantic stage over common syntax and HIR; the
  parser must not directly emit graph- or node-specific declarations.
- Scripted, REPL, and ahead-of-time workflows consume the same classifier,
  checked IR, and C++ backend.
- Hgraph core has no dependency on the language project.
