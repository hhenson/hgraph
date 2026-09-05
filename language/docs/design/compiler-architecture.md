# Compiler architecture

Status: accepted direction; implementation is staged in the roadmap

## Purpose

This record defines the compiler's internal boundaries. It complements
[Architecture](architecture.md), which defines the project and runtime
boundaries, and the [Compiler and C++ lowering](../developer-guide/compiler-and-lowering.md)
guide, which describes the current implementation.

The compiler is expected to change incompatibly while the language is a
prototype. That freedom is used to establish explicit pass contracts now,
before additional syntax and backend behavior make the resolved syntax tree a
de facto intermediate representation.

## Parsing direction

HGL uses the declarative [lexy](https://github.com/foonathan/lexy) production
DSL selected in [ADR 0001](decisions/0001-declarative-parser.md). PEG is a
suitable family of techniques, but conformance to a particular parsing
formalism is not the objective. The selection came from an executable
comparison of at least:

- a programming-language-oriented C++ parsing DSL with explicit lookahead,
  precedence, recovery, and parse-tree support;
- a C++ PEG implementation whose grammar can be kept as a standalone,
  reviewable artifact.

The comparison and the production grammar cover the language's difficult
cases rather than a toy expression grammar:

- significant newlines and continuation after selected tokens;
- comments and blank lines without losing source ranges;
- nested generic applications and adjacent closing `>` tokens;
- contextual words such as `in` and the collection/type constructors;
- expression precedence, named arguments, and concise functions;
- recovery from several independent errors in one file;
- preservation of unexpected and missing syntax for diagnostics and future
  editor tooling;
- debug and release build cost, including MSVC object-size behavior.

The selected parser is private to the syntax target. Its headers do not enter
the public compiler interfaces or every test translation unit. A parser
migration first preserves the existing `ast::Module` projection for semantic
clients while retaining richer recovered syntax internally, so parser selection
does not force simultaneous semantic and backend rewrites.

The parser produces syntax, not graph or node declarations. Function
classification, type resolution, phase checking, and native binding remain
later semantic passes.

## Source representation

The source manager owns immutable source buffers, file identities, byte
offsets, line and column lookup, and excerpts. Every token, syntax node,
diagnostic, HIR instruction, and hgraph IR operation refers to a source range
owned by it.

The syntax layer must retain enough information to reproduce the source and to
represent malformed input. A valid token may not disappear merely because it
is unexpected, and an expected token may be represented as missing without
inventing source text. This permits one parser to serve the compiler, formatter,
REPL diagnostics, and later editor tooling. Whether the implementation calls
this representation a concrete syntax tree or a source-accurate syntax arena is
an implementation choice; byte fidelity and recovery are the contract.

The typed AST interface is a view or lowering over that source representation.
Semantic annotations do not mutate syntax nodes.

## Pass pipeline

```text
SourceManager + DiagnosticEngine
                 |
                 v
       source-accurate syntax
                 |
                 v
       declarations and names <----- package closure and module descriptors
                 |
                 v
    canonical types and constraints <--- hgraph type/resolution contracts
                 |
                 v
             typed HIR
                 |
                 v
         hgraph semantic IR
             /          \
            v            v
     direct wiring    generated C++ + build manifest
```

The driver assembles the inputs and invokes these passes. It does not implement
their semantics.

### Syntax

Input: source buffers.

Output: a source-accurate syntax module containing declarations, expressions,
statements, comments, unexpected syntax, missing syntax, and precise ranges.

The syntax layer may diagnose lexical and grammatical errors only. It has no
dependency on hgraph, module descriptors, operator registration, or a backend.

### Declaration and name resolution

Input: syntax modules, package target, and importable declarations from module
descriptors.

Output: stable declaration and symbol identities, lexical bindings, nominal
module identities, visibility, and the candidate universe selected by the
locked target closure.

Imports control spelling and visibility. The package closure controls which
implementation providers participate. Resolution never discovers installed
packages or loads executable code.

### Type and constraint checking

Input: resolved declarations plus canonical hgraph type and operator metadata.

Output: complete substitutions, canonical value and temporal types, selected
nominal operator identities, candidate resolution results, constant values,
and typed diagnostics.

HGL adapts hgraph's `TypePattern`, `ResolutionMap`, and constraint/resolver
contracts. It does not maintain a language-local overload matcher with subtly
different ranking.

### Typed HIR

HIR is the last representation of HGL as a language. It contains:

- stable declaration and symbol identities rather than unresolved names;
- canonical types and complete generic substitutions;
- constants as typed values rather than syntax literals;
- resolved exact calls and nominal operator calls;
- function kind, phase, effects, and admitted capabilities;
- structured control flow and lexical ownership;
- source ranges on every operation that can diagnose.

HIR contains no emitted C++ text, C++ identifier escaping, hgraph node indices,
or direct-wiring runtime objects. Invalid phase transitions and unsupported
language constructs are rejected before HIR is considered complete.

`hgl check --dump-hir` is the stable debugging view for this pass during the
prototype. The serialization is diagnostic, not a compatibility format.

### Hgraph semantic IR

Hgraph IR is the lowering boundary between language semantics and execution.
It distinguishes:

- wiring-time constants, exact calls, and nominal operator calls;
- runtime node state layout and initialization;
- injected capabilities;
- start, ordered activation, evaluation, and stop operations;
- activation, validity admission, and residual guards;
- borrowed collection traversal;
- terminating returns and explicit output mutation;
- provider identities and leases required by imported native code.

Both execution backends consume this representation. Neither backend may
include syntax AST headers or redo name lookup, type inference, function
classification, phase checking, or generic substitution.

`hgl check --dump-hgraph-ir` exposes a readable, source-ranged form for tests
and compiler diagnosis.

### Backends

The direct-wiring backend turns composition operations into calls to hgraph's
public erased wiring API. It does not evaluate runtime bodies.

The C++ backend turns the same composition operations and the runtime-node
operations into readable public hgraph C++. Formatting, source mapping, and
build-manifest production are backend responsibilities; semantic decisions are
not.

A program accepted by the complete semantic and hgraph-IR pipeline must not be
rejected later merely because one backend failed to implement a language
construct. During migration, such gaps remain explicit diagnostics and are
tracked as architecture debt. The end state makes backend parity an invariant.

## Dependency rules

The CMake targets enforce an acyclic graph:

```text
hgl_source
    -> hgl_syntax
    -> hgl_semantics
    -> hgl_ir
       -> hgl_wiring
       -> hgl_codegen_cpp
    -> hgl_driver
```

Source management and diagnostics may initially remain in `hgl_syntax`, but
their interfaces must not depend on parser or AST node classes. Module loading
and descriptor decoding are separate services supplied to semantics by the
driver.

Architecture tests reject these wrong-layer dependencies:

- parser code referring to graph/node classification or native bindings;
- HIR or hgraph IR storing emitted C++ fragments;
- a backend including syntax AST headers after its IR migration;
- the C++ emitter resolving types or overloads;
- the direct backend implementing tick-time behavior;
- compiler code reaching into hgraph registry storage or private headers.

## Migration sequence

1. Freeze representative valid and malformed syntax cases and select the parser
   implementation using the criteria above.
2. Replace parsing behind the existing syntax result and keep semantic tests
   unchanged.
3. Introduce typed HIR beside `ResolvedModule`, with dumps and focused tests.
4. Move semantic validation into HIR construction until a successful `check`
   denotes a completely typed program.
5. Introduce hgraph semantic IR and migrate direct wiring.
6. Migrate C++ generation, then remove backend access to resolved syntax.
7. Make backend parity and architecture dependency tests required gates.

Each step is a reviewable pull request. Temporary adapters are removed by the
end of the stack rather than becoming a third representation that backends can
continue to consume.
