#include "syntax/ast_printer.h"
#include "syntax/formatter.h"
#include "syntax/lexer.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>

using namespace hgl::syntax;

namespace
{
    std::string formatted(std::string text) {
        const SourceFile file{"format.hgl", text};
        DiagnosticSink   diagnostics;
        const auto       result = format_declarations(file, diagnostics);
        INFO(diagnostics.render(file));
        REQUIRE(result);
        const SourceFile after{"format.hgl", *result};
        DiagnosticSink   again;
        REQUIRE(format_declarations(after, again) == result);
        DiagnosticSink before_ast, after_ast;
        const auto     strip = [](const ast::Module &module) {
            return std::regex_replace(print_ast(module), std::regex{R"( \[\d+\.\.\d+\))"}, "");
        };
        REQUIRE(strip(parse(file, before_ast)) == strip(parse(after, after_ast)));
        return *result;
    }
}  // namespace

TEST_CASE("formatter groups properties with their operator") {
    REQUIRE(formatted("module demo\noperator add_<L, R, O>(lhs: L, rhs: R) -> O\n"
                      "properties<str, str, str> { associative, identity = \"\" }\n"
                      "properties<i64, i64, i64> { commutative, identity = 0 }\n"
                      "operator sub_<L, R, O>(lhs: L, rhs: R) -> O\n") ==
            "module demo\n\noperator add_<L, R, O>(lhs: L, rhs: R) -> O\n"
            "    properties<str, str, str> { associative, identity = \"\" }\n"
            "    properties<i64, i64, i64> { commutative, identity = 0 }\n\n"
            "operator sub_<L, R, O>(lhs: L, rhs: R) -> O\n");
}

TEST_CASE("formatter keeps imports together and attaches requires") {
    REQUIRE(formatted("module demo\nuse a as a\n\nuse b as b\n"
                      "impl fn add_<L,R,O>(lhs:L,rhs:R)->O requires a::add(L,R)->O {\n"
                      "    when { return a::add(lhs,rhs) }\n}\nfn next(x:i64)->i64 => x\n") ==
            "module demo\n\nuse a as a\nuse b as b\n\n"
            "impl fn add_<L,R,O>(lhs:L,rhs:R)->O\n    requires a::add(L,R)->O {\n"
            "    when { return a::add(lhs,rhs) }\n}\n\nfn next(x:i64)->i64 => x\n");
}

TEST_CASE("formatter preserves trailing and leading comments") {
    REQUIRE(formatted("module demo\nfn a(x:i64)->i64 => x # trailing\n# About b.\n"
                      "fn b(x:i64)->i64 => x\n/* About c.\n   Keep this layout. */\nfn c(x:i64)->i64 => x") ==
            "module demo\n\nfn a(x:i64)->i64 => x # trailing\n\n# About b.\n"
            "fn b(x:i64)->i64 => x\n\n/* About c.\n   Keep this layout. */\nfn c(x:i64)->i64 => x\n");
}

TEST_CASE("formatter shifts multiline property clauses") {
    REQUIRE(formatted("module demo\noperator add_<T>(lhs:T,rhs:T)->T\nproperties<str> {\n"
                      "    associative,\n    identity = \"\"\n}\n") ==
            "module demo\n\noperator add_<T>(lhs:T,rhs:T)->T\n    properties<str> {\n"
            "        associative,\n        identity = \"\"\n    }\n");
}

TEST_CASE("formatter handles nested test declarations without reformatting bodies") {
    REQUIRE(formatted("module demo\ntest {\n    fn a(x:i64)->i64 => x\n"
                      "    test works { assert a(1) == 1 }\n}\n") == "module demo\n\ntest {\n    fn a(x:i64)->i64 => x\n\n"
                                                                     "    test works { assert a(1) == 1 }\n}\n");
}

TEST_CASE("formatter preserves CRLF and literal spelling") {
    REQUIRE(formatted("module demo\r\nfn text()->str => \"properties { # \\\"quoted\\\" }\"\r\n") ==
            "module demo\r\n\r\nfn text()->str => \"properties { # \\\"quoted\\\" }\"\r\n");
}

TEST_CASE("formatter refuses invalid syntax") {
    SourceFile     file{"broken.hgl", "module demo\nfn broken(\n"};
    DiagnosticSink diagnostics;
    REQUIRE_FALSE(format_declarations(file, diagnostics));
    REQUIRE(diagnostics.has_errors());
}

TEST_CASE("formatter preserves native implementation bodies") {
    const std::string body = R"(cpp(auto value) {
  // properties<T> { } and requires are host-language text.
  return value;
})";
    const auto result      = formatted("module demo\nnative fn identity(value:i64)->i64 { " + body + " }\nfn after(x:i64)->i64 => x\n");
    REQUIRE(result.find(body) != std::string::npos);
}

TEST_CASE("formatter preserves parsed structure throughout shared examples") {
    for (const auto &entry : std::filesystem::directory_iterator{HGL_EXAMPLES_DIR}) {
        if (entry.path().extension() != ".hgl") { continue; }
        INFO(entry.path().filename().string());
        std::ifstream input{entry.path(), std::ios::binary};
        REQUIRE(input.good());
        (void)formatted(std::string{std::istreambuf_iterator<char>{input}, {}});
    }
}
