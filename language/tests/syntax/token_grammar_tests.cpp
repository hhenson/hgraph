#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/source.h"
#include "syntax/token_grammar.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

using namespace hgl::syntax;

namespace
{
    void require_grammar(std::string text) {
        SourceFile     file{"grammar.hgl", std::move(text)};
        DiagnosticSink diagnostics;
        LexResult      lexed = lex(file, diagnostics);
        INFO(diagnostics.render(file));
        REQUIRE_FALSE(diagnostics.has_errors());

        const GrammarResult result = parse_token_grammar(lexed.tokens);
        INFO("first grammar error token: " << result.first_error_token);
        std::ostringstream context;
        const std::size_t  context_begin = result.first_error_token > 3 ? result.first_error_token - 3 : 0;
        const std::size_t  context_end   = std::min(result.first_error_token + 4, lexed.tokens.size());
        for (std::size_t index = context_begin; index < context_end; ++index) {
            context << index << ':' << token_kind_name(lexed.tokens[index].kind) << "('" << lexed.tokens[index].text << "') ";
        }
        INFO(context.str());
        if (result.first_error_token < lexed.tokens.size()) {
            const Token &token = lexed.tokens[result.first_error_token];
            INFO("first grammar error at token " << result.first_error_token << " (" << token_kind_name(token.kind) << ", '"
                                                 << token.text << "')");
        }
        INFO("grammar errors: " << result.error_count);
        REQUIRE(result.accepted);
        REQUIRE(result.error_count == 0);
        REQUIRE(result.syntax_node_count > 0);
    }
}  // namespace

TEST_CASE("the declarative grammar covers the language surface", "[token-grammar]") {
    const std::vector<std::string> sources{
        R"(module examples.syntax
use hgraph.core::{add, map}
use acme.stats as stats

operator scale<T>(value: T, by: f64) -> T

export fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 =>
    (tob[0] + tob[1]) / 2.0

impl fn smooth<T, const N: i64>(value: map<str, list<T, N>>, const window: i64 = 3) -> f64
requires T in {i64, f64} && add(T, T) -> T
{
    state previous: f64 = 0.0
    inject out, logger
    start { logger("start") }
    when modified(value) && valid(value) {
        previous += stats::update(previous, value, window)
        out = previous
    }
    stop { logger("stop") }
    return previous
}
)",
        R"(module examples.types

export abstract struct Instrument<T> {
    value: T
    venue: str = null
}

export struct Future<T, const size: i64>: Instrument<T>
requires T in {i64, f64} && T is struct || add(T, T) -> T
{
    venue = "XEUR"
    history: rolling<T, size, 1>
}

fn construct() => Box<f64>(value: 1.0)
fn change() => delta<Quote>(bid: 1.0, ask: null)
)",
        R"(module examples.expressions

fn transform(xs: list<f64, unbounded>) {
    let mapped = map(xs, fn(x: f64) -> f64 => x * 2.0)
    var total = 0.0
    for i, value in items(mapped) {
        total += if value > 0.0 { value } else { 0.0 }
    }
    [0s: total, 2m: total + 1.0]
}

test doubles {
    assert eval(transform, xs: [[1.0, 2.0]]) == [[2.0, 4.0]]
}
)",
    };

    for (const std::string &source : sources) { require_grammar(source); }
}

TEST_CASE("the declarative grammar covers every checked-in HGL example", "[token-grammar][examples]") {
    namespace fs = std::filesystem;
    for (const fs::directory_entry &entry : fs::directory_iterator{HGL_EXAMPLES_DIR}) {
        if (entry.path().extension() != ".hgl") { continue; }
        std::ifstream input{entry.path()};
        REQUIRE(input.good());
        const std::string text{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
        INFO(entry.path().filename().string());
        require_grammar(text);
    }
}

TEST_CASE("the declarative grammar recovers more than one local syntax error", "[token-grammar][recovery]") {
    SourceFile      file{"recovery.hgl", R"(module recovery
fn broken(a f64, b i64, c str) -> f64 => a
)"};
    DiagnosticSink  diagnostics;
    const LexResult lexed = lex(file, diagnostics);
    REQUIRE_FALSE(diagnostics.has_errors());

    const SyntaxParseResult parsed = parse_source_syntax(file, lexed);
    REQUIRE(parsed.grammar.accepted);
    REQUIRE(parsed.grammar.recovered);
    REQUIRE(parsed.grammar.error_count == 3);
    REQUIRE(parsed.grammar.syntax_node_count > 0);
    REQUIRE(parsed.grammar.first_error_token == 7);
    REQUIRE(lexed.tokens[parsed.grammar.first_error_token].kind == TokenKind::KwF64);
    REQUIRE(parsed.tree.has_root());
    REQUIRE(parsed.tree.source_is_complete());
    REQUIRE(parsed.tree.reconstruct(file) == file.text());
    REQUIRE(parsed.tree.issues.size() == 3);
    for (const SyntaxIssue &issue : parsed.tree.issues) {
        REQUIRE(issue.kind == SyntaxIssueKind::Missing);
        REQUIRE(issue.range.empty());
        REQUIRE(issue.expected == TokenKind::Colon);
        REQUIRE(issue.context == SyntaxKind::Parameter);
    }
}

TEST_CASE("the source syntax tree preserves structure and every source byte", "[token-grammar][source-accurate]") {
    SourceFile      file{"tree.hgl", R"(module example.tree

// retained trivia
export fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 =>
    (tob[0] + tob[1]) / 2.0
)"};
    DiagnosticSink  diagnostics;
    const LexResult lexed = lex(file, diagnostics);
    REQUIRE_FALSE(diagnostics.has_errors());

    const SyntaxParseResult parsed = parse_source_syntax(file, lexed);
    REQUIRE(parsed.grammar.accepted);
    REQUIRE_FALSE(parsed.grammar.recovered);
    REQUIRE(parsed.tree.has_root());
    REQUIRE(parsed.tree.nodes[parsed.tree.root].kind == SyntaxKind::Module);
    REQUIRE(parsed.tree.nodes[parsed.tree.root].range == SourceRange{0, static_cast<std::uint32_t>(file.text().size())});
    REQUIRE(parsed.tree.source_is_complete());
    REQUIRE(parsed.tree.reconstruct(file) == file.text());
    REQUIRE(parsed.tree.issues.empty());

    REQUIRE(std::ranges::none_of(parsed.tree.nodes, [](const SyntaxNode &node) { return node.kind == SyntaxKind::Unknown; }));

    std::vector<bool> retained(lexed.tokens.size() - 1, false);
    for (const SyntaxToken &token : parsed.tree.tokens) {
        REQUIRE(token.source_token_index < retained.size());
        REQUIRE_FALSE(retained[token.source_token_index]);
        retained[token.source_token_index] = true;
        REQUIRE(token.kind == lexed.tokens[token.source_token_index].kind);
        REQUIRE(token.range == lexed.tokens[token.source_token_index].range);
        REQUIRE_FALSE(token.unexpected);
    }
    REQUIRE(std::ranges::all_of(retained, [](bool value) { return value; }));
    REQUIRE(std::ranges::count_if(parsed.tree.fragments, [](const SourceFragment &fragment) {
                return fragment.kind == SourceFragmentKind::LineComment;
            }) == 1);
}

TEST_CASE("fatal syntax still retains the complete source", "[token-grammar][source-accurate][recovery]") {
    SourceFile      file{"fatal.hgl", R"(module fatal
fn broken() => )
)"};
    DiagnosticSink  diagnostics;
    const LexResult lexed = lex(file, diagnostics);
    REQUIRE_FALSE(diagnostics.has_errors());

    const SyntaxParseResult parsed = parse_source_syntax(file, lexed);
    REQUIRE_FALSE(parsed.grammar.accepted);
    REQUIRE(parsed.tree.source_is_complete());
    REQUIRE(parsed.tree.reconstruct(file) == file.text());
    REQUIRE_FALSE(parsed.tree.issues.empty());
    REQUIRE(std::ranges::any_of(parsed.tree.issues,
                                [](const SyntaxIssue &issue) { return issue.kind == SyntaxIssueKind::Unexpected; }));
}
