# ADR 0001: Declarative parser and source-accurate syntax

Status: accepted

## Context

At the time of this decision, the prototype used a hand-written
recursive-descent parser. It was small and had
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

Use [lexy](https://github.com/foonathan/lexy), initially pinned to its
`v2025.05.0` release, as a private implementation dependency of `hgl_syntax`.
Its production DSL is the canonical grammar. Parse into a lossless parse tree,
then project that tree into the existing `ast::Module` contract while later
passes migrate to the source-accurate representation.

lexy's explicit branch conditions are intentional. They make lookahead and
recovery points visible at each ambiguity rather than relying on global parser
backtracking. Its expression productions give precedence and associativity a
dedicated representation, and recovered parses retain error tokens in the
tree. Those properties line up with HGL's compiler, formatter, REPL, and future
editor requirements.

The implementation is private to `hgl_syntax`. PEG semantics are acceptable
but are not a requirement when a parser DSL provides better explicit recovery,
precedence, and source fidelity.

## Shortlist evaluation

The initial executable spike used one source file containing a module declaration, an
aliased import, a generic function, a nested generic type, a `const` parameter,
state, `when`, boolean precedence, qualified calls, assignment, and `return`.
A second source omitted two independent type colons. Each candidate was tested
for tree construction, source positions, and recovery; standalone debug builds
also provided a compile-cost signal.

The measurements below were taken on 2026-09-05 with Apple Clang 21.0.0,
C++23, `-O0`, and five clean compilations. They are a shortlist signal, not the
complete qualification required by the testing guide and not cross-machine
performance promises.

| Candidate | Version | Grammar form | Recovery result | Median compile | Debug executable |
| --- | --- | --- | --- | ---: | ---: |
| lexy | `v2025.05.0` | typed C++ production DSL | two errors and a recovered lossless tree | 0.50 s | 616,200 B |
| cpp-peglib | `v1.17.0` | compact external PEG text | two structured errors; no recovered AST | 1.45 s | 2,923,504 B |
| PEGTL | `4.0.1` | typed C++ PEG rules | first failure only without custom recovery control | 1.42 s | 6,492,536 B |

The source spikes were 201, 124, and 141 lines respectively. That count
correctly highlights lexy's main cost: its grammar is more verbose than a text
PEG. Repeated parse timings were not used to rank the tools because the spikes
built different tree shapes: a lossless lexy tree, cpp-peglib's optimized AST,
and PEGTL's unfiltered rule tree.

The spike did **not** by itself accept the parser. Acceptance required the
production grammar to record all of the following evidence:

- the complete valid syntax and documentation-example corpus, including exact
  newline continuation and nested generic closers;
- a malformed corpus producing at least three independent useful diagnostics
  while retaining unexpected and missing syntax;
- isolated debug and release compiler-time and object-size measurements;
- representative Clang, GCC, and MSVC builds.

Keeping selection and qualification separate allowed the implementation to
proceed without presenting a toy grammar as conclusive evidence.

## Production qualification

The production grammar passed the acceptance gates on 2026-09-05:

- `hgl_syntax_tests` passed 131 cases and 2,894 assertions. The corpus covers
  every checked-in HGL guide example, exact newline continuation, nested
  generic closers, the complete parser-independent AST projection, source
  ranges, comments, and lossless source reconstruction.
- One malformed module produces three independent missing-colon diagnostics
  while retaining a recovered lossless tree. Additional cases retain
  unexpected tokens, explicit missing syntax, and the complete source after a
  fatal parse.
- An isolated, compiler-cache-free Apple Clang 21.0.0 build of
  `token_grammar.cpp` took 1.43 s at `-O0` and produced a 7,291,544-byte object;
  the release compile took 3.84 s and produced a 736,976-byte object.
- The syntax target and suite passed with warnings as errors under Apple Clang
  21.0.0, GCC 14.3.0, and MSVC 19.51. The GCC release grammar object was
  603,848 bytes. MSVC qualification used `/W4 /WX` with the independently
  optional analytics extension disabled, proving the language project can
  configure and build without that extension.

Compiler time and object size are local observations for regression tracking,
not comparisons across machines or promises to downstream users. GCC's lexy
instantiation requires two source-scoped suppressions for overloaded-parser-DSL
`-Wparentheses` and parse-handler `-Wdangling-pointer` false positives; all
other language sources and diagnostics remain under the normal warning policy.

### Why not cpp-peglib

cpp-peglib has the neatest reviewable grammar and the strongest structured
recovery labels of the alternatives. Its generated AST stores byte positions,
but it is a semantic tree: literals, comments, whitespace, and recovered input
are not preserved as a lossless syntax tree. Building a second token/trivia
model beside it would move complexity out of the grammar rather than remove
it. Its runtime grammar compilation and larger standalone footprint were
additional, but secondary, disadvantages.

### Why not PEGTL

PEGTL is a capable low-level PEG toolkit with source positions, grammar
analysis, and configurable parse-tree selection. It does not provide the
multi-error recovery and programming-language expression layer HGL needs.
Those could be built through custom control rules, but that would recreate
substantial parser infrastructure already supplied by lexy.

## Integration constraints

- lexy headers are included by syntax implementation files only. Public syntax
  and AST headers do not expose lexy types.
- Grammar productions are split into bounded subgrammars when a single
  translation unit becomes expensive. HGL tests include the public syntax API,
  not the parser DSL.
- The dependency is fetched only when the opt-in language project is enabled.
- Warnings originating in third-party headers are treated as system-header
  concerns. Any compiler false-positive suppression caused by the parser DSL
  is documented and scoped to the grammar implementation translation unit;
  projection code and every other HGL source remain under normal warnings.
- Recovery tests must assert both diagnostics and the retained source shape.
- A future replacement remains possible behind the source-accurate syntax
  contract; downstream passes must not depend on lexy node types.

## Consequences

- The grammar becomes independently reviewable and testable.
- Unexpected and missing syntax are retained for diagnostics and tooling.
- Parser-library template costs cannot spread through public headers. Grammar
  verbosity and template diagnostics are accepted implementation costs.
- The parser does not classify functions or perform name and type resolution.
- The current AST contract is preserved during the initial parser replacement.

## Implementation status

The replacement is complete. Lexy materializes the source-accurate HGL syntax
arena, HGL-owned code translates recovery issues and projects complete
productions into `ast::Module`, and the recursive-descent parser has been
removed. This does not complete the frontend migration: `ast::Module` remains
an interim input until typed HIR becomes the semantic-pass output.

## Alternatives

- Retain the current parser permanently: rejected as the default direction,
  although hand-written code remains available for narrow rules a parser DSL
  cannot express cleanly.
- Require a strict PEG implementation without evaluation: rejected because
  recovery and source fidelity are more important than formalism.
- Use an editor parser as the compiler's first parser: deferred; incremental
  parsing is not yet the dominant requirement.
