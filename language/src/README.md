# Compiler source architecture

The normative pass boundaries are in
[`docs/design/compiler-architecture.md`](../docs/design/compiler-architecture.md).
This file records the current source layout while the prototype migrates to
them.

| Directory | Owns now | Target input and output |
| --- | --- | --- |
| `syntax/` | source buffers, diagnostics, temporal literals, lexer, parser, arena AST | source text to source-accurate syntax |
| `semantics/` | bindings, nominal structs, generic checks, function classification | syntax plus descriptors to typed HIR |
| `ir/` | not created yet | typed HIR to backend-neutral hgraph semantic IR |
| `wiring/` | direct walk over `ResolvedModule` | hgraph IR to public erased wiring calls |
| `codegen/` | direct walk over `ResolvedModule` | hgraph IR to formatted C++ and build artifacts |
| `driver/` | commands, native build/cache/load, REPL orchestration | assemble inputs and invoke passes |

During migration, `ResolvedModule` is a compatibility boundary only. New
language semantics belong in HIR construction, not in either backend. Temporary
adapters must be named and removed when both backends consume hgraph IR.

Every new pass documents:

- who owns its input and output storage;
- whether it continues after diagnostics;
- the invariant established by successful completion;
- how its source ranges and debug dump are tested;
- its allowed dependencies.

The parser implementation and any parsing library remain private to `syntax/`.
Once migrated, backend targets must not include syntax AST headers. Generated
C++ remains formatted, readable output, but it is not used as an intermediate
representation by another compiler pass.
