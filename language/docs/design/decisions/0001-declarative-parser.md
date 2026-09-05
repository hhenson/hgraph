# ADR 0001: Declarative parser and source-accurate syntax

Status: accepted direction; parser library pending measured comparison

## Context

The prototype uses a hand-written recursive-descent parser. It is small and has
useful recovery, but grammar, recovery, AST construction, and newline policy are
encoded together. HGL syntax is still changing and will need formatting, REPL,
diagnostic, and editor support.

PEG is not treated as synonymous with a modern compiler frontend. The current
[Swift parser](https://github.com/swiftlang/swift-syntax/blob/main/Sources/SwiftParser/Parser.swift)
and [Clang parser](https://clang.llvm.org/features.html) are documented
recursive-descent parsers. Cppfront likewise keeps a custom token parser in a
[linear lexer/parser/sema/lowering stack](https://github.com/hsutter/cppfront/discussions/762).
Their useful precedents are source fidelity, diagnostics, inspectable trees,
and strict layer ownership.

## Decision

Use a declarative C++ parsing grammar or parsing DSL which produces
source-accurate, recoverable syntax. Select the concrete implementation using
the representative corpus and build criteria in
[Compiler architecture](../compiler-architecture.md#parsing-direction).

The first comparison uses
[lexy](https://github.com/foonathan/lexy) and
[cpp-peglib](https://github.com/yhirose/cpp-peglib). The
[PEGTL](https://github.com/taocpp/PEGTL) is retained as the lower-level PEG
reference if neither candidate meets the recovery and source contracts.

The implementation is private to `hgl_syntax`. PEG semantics are acceptable
but are not a requirement when a parser DSL provides better explicit recovery,
precedence, and source fidelity.

## Consequences

- The grammar becomes independently reviewable and testable.
- Unexpected and missing syntax are retained for diagnostics and tooling.
- Parser-library template costs cannot spread through public headers.
- The parser does not classify functions or perform name and type resolution.
- The current AST contract is preserved during the initial parser replacement.

## Alternatives

- Retain the current parser permanently: rejected as the default direction,
  although hand-written code remains available for narrow rules a parser DSL
  cannot express cleanly.
- Require a strict PEG implementation without evaluation: rejected because
  recovery and source fidelity are more important than formalism.
- Use an editor parser as the compiler's first parser: deferred; incremental
  parsing is not yet the dominant requirement.
