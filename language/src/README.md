# Compiler source architecture

The normative pass boundaries are in
[`docs/design/compiler-architecture.md`](../docs/design/compiler-architecture.md).
This file records the current source layout while the prototype migrates to
them.

| Directory | Owns now | Target input and output |
| --- | --- | --- |
| `syntax/` | source buffers, diagnostics, temporal literals, lexer, parser, arena AST | source text to source-accurate syntax |
| `semantics/` | name binding, nominal hierarchy, generic argument roles, function classification | syntax plus descriptors to resolved names and shapes |
| `ir/` | source-ranged HIR, canonical types, substitutions, constraint solving, phase/effect completion | resolved frontend state to typed HIR |
| `hgraph_ir/` | canonical execution-facing types, compile-time expressions, constraints, typed source-order declaration handles, struct contracts, operator and callable interfaces | typed HIR to executable composition and runtime-node plans |
| `descriptor/` | versioned module/interface schema, deterministic HGraph-IR snapshot, and canonical JSON serialization | hgraph IR to reviewable package metadata |
| `wiring/` | direct walk over hgraph IR | hgraph IR to public erased wiring calls |
| `codegen/` | hgraph-IR declaration, interface, dependency, composition-body, and runtime-body emission | hgraph IR to formatted C++ and build artifacts |
| `driver/` | commands, native build/cache/load, REPL orchestration | assemble inputs and invoke passes |

Hgraph IR now retains typed struct, operator, callable, and test handles in
source order; each referenced record owns its source range. `codegen` consumes
those handles directly for declaration grouping and source comments and no
longer accepts a `ResolvedModule` or syntax module. Scalar and operator
representations used by execution backends are HIR-owned, so neither backend
includes syntax AST or resolver headers.
Module identity, callable visibility and classification, operator binding,
exports, registration planning, callable/operator interfaces, supported
callable parameter defaults, nominal struct declarations and field layouts,
local/state binding types, construction defaults, and internal dependency order
already come from hgraph IR. Concise composition bodies and concise anonymous
functions also walk graph-IR values, operations, and lexical bindings directly;
composition and runtime block bodies additionally consume graph-IR statements,
blocks, lifecycle plans, capabilities, and lexical bindings. New language
semantics belong in HIR construction, not in either backend. The executable
backend architecture test rejects restored AST or resolver dependencies.

Every new pass documents:

- who owns its input and output storage;
- whether it continues after diagnostics;
- the invariant established by successful completion;
- how its source ranges and debug dump are tested;
- its allowed dependencies.

The parser implementation and any parsing library remain private to `syntax/`.
Backend targets must not include syntax AST headers. Generated C++ remains
formatted, readable output, but it is not used as an intermediate representation
by another compiler pass.
