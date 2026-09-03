#include "syntax/ast_printer.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

using namespace hgl::syntax;

namespace
{
    // The module borrows `Name` text from the file, so the file outlives it.
    struct Parsed
    {
        SourceFile     file;
        DiagnosticSink diagnostics;
        ast::Module    module;

        explicit Parsed(std::string text) : file{"test.hgl", std::move(text)}, module{parse(file, diagnostics)} {}

        [[nodiscard]] std::vector<std::string> messages() const
        {
            std::vector<std::string> out;
            for (const Diagnostic &diagnostic : diagnostics.diagnostics()) { out.push_back(diagnostic.message); }
            return out;
        }
    };

    // Drop the ` [begin..end)` spans so structural expectations stay readable.
    std::string strip_ranges(const std::string &dump)
    {
        static const std::regex span{R"( \[\d+\.\.\d+\))"};
        return std::regex_replace(dump, span, "");
    }

    std::string dump(const Parsed &parsed) { return strip_ranges(print_ast(parsed.module)); }

    // Parse a whole file and dump it, requiring a clean parse.
    std::string dump_clean(const std::string &text)
    {
        Parsed parsed{text};
        INFO(parsed.diagnostics.render(parsed.file));
        REQUIRE_FALSE(parsed.diagnostics.has_errors());
        return dump(parsed);
    }

    // The subtree under the first line carrying `label` at any depth,
    // re-based to depth zero with the label removed.
    std::string subtree(const std::string &dump, std::string_view label)
    {
        std::istringstream in{dump};
        std::string        line;
        std::string        out;
        std::size_t        base = std::string::npos;
        while (std::getline(in, line))
        {
            const std::size_t indent = line.find_first_not_of(' ');
            if (base == std::string::npos)
            {
                if (indent != std::string::npos && line.compare(indent, label.size(), label) == 0)
                {
                    base = indent;
                    out += line.substr(indent + label.size());
                    out += '\n';
                }
                continue;
            }
            if (indent == std::string::npos || indent <= base) { break; }
            out += line.substr(base);
            out += '\n';
        }
        return out;
    }

    // Dump of one expression, parsed as a concise function body.
    std::string expr_dump(const std::string &expr)
    {
        return subtree(dump_clean("module t\nfn f() => " + expr + "\n"), "body: ");
    }

    // Dump of one type, parsed as a parameter type.
    std::string type_dump(const std::string &type)
    {
        return subtree(dump_clean("module t\nfn f(x: " + type + ") => x\n"), "type: ");
    }

    // Dump of a block body's statements.
    std::string body_dump(const std::string &statements)
    {
        return subtree(dump_clean("module t\nfn f() {\n" + statements + "\n}\n"), "body: ");
    }
}  // namespace

// ------------------------------------------------------------ declarations

TEST_CASE("a module declaration names a dotted path", "[parser]")
{
    Parsed parsed{"module examples.prices\n"};
    REQUIRE_FALSE(parsed.diagnostics.has_errors());
    REQUIRE(dump(parsed) == "Module\n  ModuleDecl examples.prices\n");
    const ast::Decl &decl = parsed.module.decl(parsed.module.declarations.at(0));
    REQUIRE(parsed.file.slice(decl.range) == "module examples.prices");
}

TEST_CASE("use declarations import sets and aliases", "[parser]")
{
    const std::string text = "module t\n"
                             "use a.b::{x, y}\n"
                             "use a.b as c\n"
                             "use a.b::{x}\n"
                             "use a.b::{\n"
                             "    p,\n"
                             "    q,\n"
                             "}\n";
    REQUIRE(dump_clean(text) == "Module\n"
                                "  ModuleDecl t\n"
                                "  UseDecl a.b::{x, y}\n"
                                "  UseDecl a.b as c\n"
                                "  UseDecl a.b::{x}\n"
                                "  UseDecl a.b::{p, q}\n");
}

TEST_CASE("a lexer error token stands in for a statement terminator", "[parser]")
{
    Parsed parsed{"module t\nfn f(a: f64) -> f64 {\n    let b = a; b\n}\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"';' is not a statement terminator; use a newline"});
}

TEST_CASE("an import set is always braced", "[parser]")
{
    Parsed parsed{"module t\nuse a.b::x\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected '{' after '::', found 'x'"});
}

TEST_CASE("use declarations must precede ordinary declarations", "[parser]")
{
    Parsed parsed{"module t\nfn f() => 1\nuse a::{x}\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"'use' declarations must precede other declarations"});
    REQUIRE(parsed.module.declarations.size() == 3);  // still recorded
}

TEST_CASE("an empty import set is a diagnostic", "[parser]")
{
    Parsed parsed{"module t\nuse a.b::{}\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"an import set names at least one declaration"});
}

TEST_CASE("a file starts with its module declaration", "[parser]")
{
    Parsed parsed{"fn f() => 1\n"};
    REQUIRE(parsed.messages() ==
            std::vector<std::string>{"expected a 'module' declaration at the start of the file"});
    REQUIRE(parsed.module.declarations.size() == 1);

    Parsed twice{"module a\nmodule b\n"};
    REQUIRE(twice.messages() == std::vector<std::string>{"a file has one 'module' declaration"});
}

TEST_CASE("function visibility and bodies", "[parser]")
{
    const std::string text = "module t\n"
                             "fn a() => 1\n"
                             "export fn b() => 2\n"
                             "impl fn c() {\n"
                             "    3\n"
                             "}\n";
    REQUIRE(dump_clean(text) == "Module\n"
                                "  ModuleDecl t\n"
                                "  FunctionDecl fn a\n"
                                "    body: IntLiteral 1\n"
                                "  FunctionDecl export fn b\n"
                                "    body: IntLiteral 2\n"
                                "  FunctionDecl impl fn c\n"
                                "    body: Block\n"
                                "      ExprStmt tail\n"
                                "        IntLiteral 3\n");
}

TEST_CASE("declaration ranges cover the whole declaration", "[parser]")
{
    Parsed parsed{"module t\nfn f(x: f64) -> f64 {\n    x\n}\n"};
    REQUIRE_FALSE(parsed.diagnostics.has_errors());
    const ast::Decl &decl = parsed.module.decl(parsed.module.declarations.at(1));
    REQUIRE(parsed.file.slice(decl.range) == "fn f(x: f64) -> f64 {\n    x\n}");
}

TEST_CASE("generic parameters", "[parser]")
{
    REQUIRE(dump_clean("module t\nfn f<T, const N: i64>(x: T) -> list<T, N> => x\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  FunctionDecl fn f\n"
            "    GenericParameter T\n"
            "    GenericParameter const N\n"
            "      type: Type scalar i64 (value)\n"
            "    Parameter x\n"
            "      type: Type named T\n"
            "    result: Type list\n"
            "      Type named T\n"
            "      size: NameRef N\n"
            "    body: NameRef x\n");
}

TEST_CASE("only const parameters have defaults", "[parser]")
{
    REQUIRE(dump_clean("module t\nfn f(a: f64, const n: i64 = 3) => a\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  FunctionDecl fn f\n"
            "    Parameter a\n"
            "      type: Type scalar f64\n"
            "    Parameter const n\n"
            "      type: Type scalar i64 (value)\n"
            "      default: IntLiteral 3\n"
            "    body: NameRef a\n");

    Parsed temporal_default{"module t\nfn f(s: str = \"x\") => s\n"};
    REQUIRE(temporal_default.messages() == std::vector<std::string>{"only a const parameter may have a default"});
}

TEST_CASE("struct declarations carry hierarchy, defaults, and generic requirements", "[parser]")
{
    const std::string tree = dump_clean(R"(
module t

export abstract struct Instrument<T> {
    value: T
    venue: str = null
}

export struct Future<T, const size: i64>: Instrument<T>
requires T in {i64, f64} && T is struct || add(T, T) -> T
{
    venue = "XEUR"
    history: list<T, size>
}
)");
    CHECK(tree.find("StructDecl export abstract struct Instrument") != std::string::npos);
    CHECK(tree.find("GenericParameter const size") != std::string::npos);
    CHECK(tree.find("parent: Type named Instrument") != std::string::npos);
    CHECK(tree.find("requires: ConstraintLogic ||") != std::string::npos);
    CHECK(tree.find("ConstraintRelation in") != std::string::npos);
    CHECK(tree.find("ConstraintRelation is") != std::string::npos);
    CHECK(tree.find("OperatorRequirement add") != std::string::npos);
    CHECK(tree.find("InheritedDefault venue") != std::string::npos);
    CHECK(tree.find("Type named T") != std::string::npos);
    CHECK(tree.find("size: NameRef size") != std::string::npos);
}

TEST_CASE("operator declarations have signatures and no body", "[parser]")
{
    REQUIRE(dump_clean("module t\noperator scale<T>(value: T, by: f64) -> T\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  OperatorDecl scale\n"
            "    GenericParameter T\n"
            "    Parameter value\n"
            "      type: Type named T\n"
            "    Parameter by\n"
            "      type: Type scalar f64\n"
            "    result: Type named T\n");

    Parsed with_body{"module t\noperator f(x: f64) -> f64 => x\n"};
    REQUIRE(with_body.messages() ==
            std::vector<std::string>{"an operator declaration has no body; implement it with 'impl fn'"});
}

TEST_CASE("test declarations wrap a block", "[parser]")
{
    REQUIRE(dump_clean("module t\ntest doubles {\n    assert eval(f, x: [1.0]) == [2.0]\n}\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  TestDecl doubles\n"
            "    body: Block\n"
            "      Assert\n"
            "        Binary ==\n"
            "          Eval\n"
            "            callee: NameRef f\n"
            "            Argument x\n"
            "              SequenceLiteral\n"
            "                FloatLiteral 1.0\n"
            "          SequenceLiteral\n"
            "            FloatLiteral 2.0\n");
}

// -------------------------------------------------------------------- types

TEST_CASE("scalar and named types", "[parser]")
{
    REQUIRE(type_dump("f64") == "Type scalar f64\n");
    REQUIRE(type_dump("i64") == "Type scalar i64\n");
    REQUIRE(type_dump("bool") == "Type scalar bool\n");
    REQUIRE(type_dump("str") == "Type scalar str\n");
    REQUIRE(type_dump("datetime") == "Type scalar datetime\n");
    REQUIRE(type_dump("duration") == "Type scalar duration\n");
    REQUIRE(type_dump("T") == "Type named T\n");
    REQUIRE(type_dump("Quote") == "Type named Quote\n");
    REQUIRE(type_dump("market::Box<f64, N>") == "Type named market::Box\n"
                                                "  GenericArgument\n"
                                                "    Type scalar f64 (value)\n"
                                                "  GenericArgument\n"
                                                "    Name N\n");
}

TEST_CASE("container types", "[parser]")
{
    REQUIRE(type_dump("tuple<f64, f64>") == "Type tuple\n"
                                            "  Type scalar f64\n"
                                            "  Type scalar f64\n");
    REQUIRE(type_dump("list<T>") == "Type list\n"
                                    "  Type named T\n");
    REQUIRE(type_dump("list<T, 3>") == "Type list\n"
                                       "  Type named T\n"
                                       "  size: IntLiteral 3\n");
    REQUIRE(type_dump("list<T, unbounded>") == "Type list unbounded\n"
                                               "  Type named T\n");
    REQUIRE(type_dump("set<T>") == "Type set\n"
                                   "  Type named T (value)\n");
    REQUIRE(type_dump("map<K, V>") == "Type map\n"
                                      "  Type named K (value)\n"
                                      "  Type named V\n");
    REQUIRE(type_dump("rolling<f64, 20>") == "Type rolling\n"
                                             "  Type scalar f64 (value)\n"
                                             "  size: IntLiteral 20\n");
    REQUIRE(type_dump("rolling<f64, 5m>") == "Type rolling\n"
                                             "  Type scalar f64 (value)\n"
                                             "  size: TemporalLiteral 5m\n");
    REQUIRE(type_dump("rolling<f64, max_size, min_size>") == "Type rolling\n"
                                                             "  Type scalar f64 (value)\n"
                                                             "  size: NameRef max_size\n"
                                                             "  min: NameRef min_size\n");
    REQUIRE(type_dump("atomic<tuple<f64,f64>>") == "Type atomic\n"
                                                   "  Type tuple (value)\n"
                                                   "    Type scalar f64 (value)\n"
                                                   "    Type scalar f64 (value)\n");
    REQUIRE(type_dump("map<str, list<f64, 2>>") == "Type map\n"
                                                   "  Type scalar str (value)\n"
                                                   "  Type list\n"
                                                   "    Type scalar f64\n"
                                                   "    size: IntLiteral 2\n");
}

TEST_CASE("container names are ordinary names without a generic list", "[parser]")
{
    REQUIRE(type_dump("rolling") == "Type named rolling\n");
    REQUIRE(expr_dump("list(1, 2)") == "Call\n"
                                       "  callee: NameRef list\n"
                                       "  Argument\n"
                                       "    IntLiteral 1\n"
                                       "  Argument\n"
                                       "    IntLiteral 2\n");
}

TEST_CASE("type size expressions stop before a closing '>'", "[parser]")
{
    REQUIRE(type_dump("list<f64, N + 1>") == "Type list\n"
                                             "  Type scalar f64\n"
                                             "  size: Binary +\n"
                                             "    NameRef N\n"
                                             "    IntLiteral 1\n");
    REQUIRE(type_dump("rolling<f64, N * 2, N>") == "Type rolling\n"
                                                   "  Type scalar f64 (value)\n"
                                                   "  size: Binary *\n"
                                                   "    NameRef N\n"
                                                   "    IntLiteral 2\n"
                                                   "  min: NameRef N\n");
}

TEST_CASE("type diagnostics", "[parser]")
{
    REQUIRE(Parsed{"module t\nfn f(x: tuple<>) => x\n"}.messages() ==
            std::vector<std::string>{"a tuple type has at least one element"});
    REQUIRE(Parsed{"module t\nfn f(x: map<str>) => x\n"}.messages() ==
            std::vector<std::string>{"expected ',' and the map's value type, found '>'"});
    REQUIRE(Parsed{"module t\nfn f(x: 3) => x\n"}.messages() == std::vector<std::string>{"expected a type, found '3'"});
}

// -------------------------------------------------------------- expressions

TEST_CASE("literals", "[parser]")
{
    REQUIRE(expr_dump("42") == "IntLiteral 42\n");
    REQUIRE(expr_dump("2.5") == "FloatLiteral 2.5\n");
    REQUIRE(expr_dump("1.0") == "FloatLiteral 1.0\n");
    REQUIRE(expr_dump("1e3") == "FloatLiteral 1000.0\n");
    REQUIRE(expr_dump("\"hi\\n\"") == "StringLiteral \"hi\\n\"\n");
    REQUIRE(expr_dump("true") == "BoolLiteral true\n");
    REQUIRE(expr_dump("false") == "BoolLiteral false\n");
    REQUIRE(expr_dump("null") == "NullLiteral\n");
    REQUIRE(expr_dump("_") == "Placeholder\n");
    REQUIRE(expr_dump("5m") == "TemporalLiteral 5m\n");
    REQUIRE(expr_dump("1h30m") == "TemporalLiteral 1h30m\n");
    REQUIRE(expr_dump("@2026-09-03T09:30Z") == "TemporalLiteral @2026-09-03T09:30Z\n");
}

TEST_CASE("struct and delta construction use named arguments", "[parser]")
{
    REQUIRE(expr_dump("Quote(bid: 1.0, ask: 2.0)") == "Call\n"
                                                      "  callee: NameRef Quote\n"
                                                      "  Argument bid\n"
                                                      "    FloatLiteral 1.0\n"
                                                      "  Argument ask\n"
                                                      "    FloatLiteral 2.0\n");
    REQUIRE(expr_dump("Box<f64>(value: 1.0)") == "StructConstruct\n"
                                                 "  type: Type named Box\n"
                                                 "    GenericArgument\n"
                                                 "      Type scalar f64 (value)\n"
                                                 "  Argument value\n"
                                                 "    FloatLiteral 1.0\n");
    REQUIRE(expr_dump("delta<Quote>(bid: 1.0, ask: null)") == "DeltaConstruct\n"
                                                              "  type: Type named Quote\n"
                                                              "  Argument bid\n"
                                                              "    FloatLiteral 1.0\n"
                                                              "  Argument ask\n"
                                                              "    NullLiteral\n");
}

TEST_CASE("names and qualified references", "[parser]")
{
    REQUIRE(expr_dump("x") == "NameRef x\n");
    REQUIRE(expr_dump("mc::my_op") == "QualifiedRef mc::my_op\n");
    REQUIRE(expr_dump("in") == "NameRef in\n");
    REQUIRE(expr_dump("out") == "NameRef out\n");
}

TEST_CASE("multiplicative binds tighter than additive", "[parser]")
{
    REQUIRE(expr_dump("a + b * c") == "Binary +\n"
                                      "  NameRef a\n"
                                      "  Binary *\n"
                                      "    NameRef b\n"
                                      "    NameRef c\n");
    REQUIRE(expr_dump("a * b + c") == "Binary +\n"
                                      "  Binary *\n"
                                      "    NameRef a\n"
                                      "    NameRef b\n"
                                      "  NameRef c\n");
    REQUIRE(expr_dump("a % b / c") == "Binary /\n"
                                      "  Binary %\n"
                                      "    NameRef a\n"
                                      "    NameRef b\n"
                                      "  NameRef c\n");
}

TEST_CASE("binary operators are left associative", "[parser]")
{
    REQUIRE(expr_dump("a - b - c") == "Binary -\n"
                                      "  Binary -\n"
                                      "    NameRef a\n"
                                      "    NameRef b\n"
                                      "  NameRef c\n");
    REQUIRE(expr_dump("a / b / c") == "Binary /\n"
                                      "  Binary /\n"
                                      "    NameRef a\n"
                                      "    NameRef b\n"
                                      "  NameRef c\n");
}

TEST_CASE("comparison, equality, and logical precedence", "[parser]")
{
    REQUIRE(expr_dump("a < b == c > d") == "Binary ==\n"
                                           "  Binary <\n"
                                           "    NameRef a\n"
                                           "    NameRef b\n"
                                           "  Binary >\n"
                                           "    NameRef c\n"
                                           "    NameRef d\n");
    REQUIRE(expr_dump("a || b && c != d") == "Binary ||\n"
                                             "  NameRef a\n"
                                             "  Binary &&\n"
                                             "    NameRef b\n"
                                             "    Binary !=\n"
                                             "      NameRef c\n"
                                             "      NameRef d\n");
    REQUIRE(expr_dump("a <= b + c") == "Binary <=\n"
                                       "  NameRef a\n"
                                       "  Binary +\n"
                                       "    NameRef b\n"
                                       "    NameRef c\n");
    REQUIRE(expr_dump("a >= b") == "Binary >=\n"
                                   "  NameRef a\n"
                                   "  NameRef b\n");
}

TEST_CASE("unary operators", "[parser]")
{
    REQUIRE(expr_dump("-a * b") == "Binary *\n"
                                   "  Unary -\n"
                                   "    NameRef a\n"
                                   "  NameRef b\n");
    REQUIRE(expr_dump("!a && b") == "Binary &&\n"
                                    "  Unary !\n"
                                    "    NameRef a\n"
                                    "  NameRef b\n");
    REQUIRE(expr_dump("- -a") == "Unary -\n"
                                 "  Unary -\n"
                                 "    NameRef a\n");
    REQUIRE(expr_dump("-f(x)") == "Unary -\n"
                                  "  Call\n"
                                  "    callee: NameRef f\n"
                                  "    Argument\n"
                                  "      NameRef x\n");
}

TEST_CASE("parentheses group", "[parser]")
{
    REQUIRE(expr_dump("(a + b) * c") == "Binary *\n"
                                        "  Binary +\n"
                                        "    NameRef a\n"
                                        "    NameRef b\n"
                                        "  NameRef c\n");
    REQUIRE(expr_dump("(1.0)") == "FloatLiteral 1.0\n");
}

TEST_CASE("calls with positional and named arguments", "[parser]")
{
    REQUIRE(expr_dump("f(a, b: 2, c)") == "Call\n"
                                          "  callee: NameRef f\n"
                                          "  Argument\n"
                                          "    NameRef a\n"
                                          "  Argument b\n"
                                          "    IntLiteral 2\n"
                                          "  Argument\n"
                                          "    NameRef c\n");
    REQUIRE(expr_dump("f()") == "Call\n"
                                "  callee: NameRef f\n");
    REQUIRE(expr_dump("mc::f(x)(y)") == "Call\n"
                                        "  callee: Call\n"
                                        "    callee: QualifiedRef mc::f\n"
                                        "    Argument\n"
                                        "      NameRef x\n"
                                        "  Argument\n"
                                        "    NameRef y\n");
    REQUIRE(expr_dump("f(\n    a,\n    b: 2,\n)") == "Call\n"
                                                    "  callee: NameRef f\n"
                                                    "  Argument\n"
                                                    "    NameRef a\n"
                                                    "  Argument b\n"
                                                    "    IntLiteral 2\n");
}

TEST_CASE("index and field postfix operators", "[parser]")
{
    REQUIRE(expr_dump("m[k].info.value") == "Field value\n"
                                            "  Field info\n"
                                            "    Index\n"
                                            "      NameRef m\n"
                                            "      index: NameRef k\n");
    REQUIRE(expr_dump("xs[i + 1] * 2") == "Binary *\n"
                                          "  Index\n"
                                          "    NameRef xs\n"
                                          "    index: Binary +\n"
                                          "      NameRef i\n"
                                          "      IntLiteral 1\n"
                                          "  IntLiteral 2\n");
    REQUIRE(expr_dump("q.bid(1)") == "Call\n"
                                     "  callee: Field bid\n"
                                     "    NameRef q\n"
                                     "  Argument\n"
                                     "    IntLiteral 1\n");
}

TEST_CASE("sequence literals", "[parser]")
{
    REQUIRE(expr_dump("[1.0, _, 2.0]") == "SequenceLiteral\n"
                                          "  FloatLiteral 1.0\n"
                                          "  Placeholder\n"
                                          "  FloatLiteral 2.0\n");
    REQUIRE(expr_dump("[]") == "SequenceLiteral\n");
    REQUIRE(expr_dump("[0s: 1.0, 2m: 3.0]") == "SequenceLiteral\n"
                                               "  TimedElement\n"
                                               "    key: TemporalLiteral 0s\n"
                                               "    FloatLiteral 1.0\n"
                                               "  TimedElement\n"
                                               "    key: TemporalLiteral 2m\n"
                                               "    FloatLiteral 3.0\n");
    REQUIRE(expr_dump("[@2026-09-03T09:30Z: 1.0]") == "SequenceLiteral\n"
                                                      "  TimedElement\n"
                                                      "    key: TemporalLiteral @2026-09-03T09:30Z\n"
                                                      "    FloatLiteral 1.0\n");
    REQUIRE(expr_dump("[\n    1.0,\n    2.0,\n]") == "SequenceLiteral\n"
                                                    "  FloatLiteral 1.0\n"
                                                    "  FloatLiteral 2.0\n");
    REQUIRE(expr_dump("[[1, 2], [3]]") == "SequenceLiteral\n"
                                          "  SequenceLiteral\n"
                                          "    IntLiteral 1\n"
                                          "    IntLiteral 2\n"
                                          "  SequenceLiteral\n"
                                          "    IntLiteral 3\n");
}

TEST_CASE("tuple literals need a comma", "[parser]")
{
    REQUIRE(expr_dump("(1.0, 2.0)") == "TupleLiteral\n"
                                       "  FloatLiteral 1.0\n"
                                       "  FloatLiteral 2.0\n");
    REQUIRE(expr_dump("(1.0,)") == "TupleLiteral\n"
                                   "  FloatLiteral 1.0\n");
    REQUIRE(expr_dump("(a, (b, c))") == "TupleLiteral\n"
                                        "  NameRef a\n"
                                        "  TupleLiteral\n"
                                        "    NameRef b\n"
                                        "    NameRef c\n");
    REQUIRE(Parsed{"module t\nfn f() => ()\n"}.messages() ==
            std::vector<std::string>{"expected an expression; '()' is not a value"});
}

TEST_CASE("anonymous functions", "[parser]")
{
    REQUIRE(expr_dump("fn(x) => x * 2") == "AnonymousFn\n"
                                           "  Parameter x\n"
                                           "  body: Binary *\n"
                                           "    NameRef x\n"
                                           "    IntLiteral 2\n");
    REQUIRE(expr_dump("fn(a: f64, b) -> f64 => a + b") == "AnonymousFn\n"
                                                          "  Parameter a\n"
                                                          "    type: Type scalar f64\n"
                                                          "  Parameter b\n"
                                                          "  result: Type scalar f64\n"
                                                          "  body: Binary +\n"
                                                          "    NameRef a\n"
                                                          "    NameRef b\n");
    REQUIRE(expr_dump("map(xs, fn(x) => x)") == "Call\n"
                                                "  callee: NameRef map\n"
                                                "  Argument\n"
                                                "    NameRef xs\n"
                                                "  Argument\n"
                                                "    AnonymousFn\n"
                                                "      Parameter x\n"
                                                "      body: NameRef x\n");
}

TEST_CASE("if is an expression", "[parser]")
{
    REQUIRE(expr_dump("if a > 0.0 { a } else { 0.0 }") == "If\n"
                                                          "  condition: Binary >\n"
                                                          "    NameRef a\n"
                                                          "    FloatLiteral 0.0\n"
                                                          "  then: Block\n"
                                                          "    ExprStmt tail\n"
                                                          "      NameRef a\n"
                                                          "  else: BlockExpr\n"
                                                          "    Block\n"
                                                          "      ExprStmt tail\n"
                                                          "        FloatLiteral 0.0\n");
    REQUIRE(expr_dump("if a { 1 } else if b { 2 } else { 3 }") == "If\n"
                                                                  "  condition: NameRef a\n"
                                                                  "  then: Block\n"
                                                                  "    ExprStmt tail\n"
                                                                  "      IntLiteral 1\n"
                                                                  "  else: If\n"
                                                                  "    condition: NameRef b\n"
                                                                  "    then: Block\n"
                                                                  "      ExprStmt tail\n"
                                                                  "        IntLiteral 2\n"
                                                                  "    else: BlockExpr\n"
                                                                  "      Block\n"
                                                                  "        ExprStmt tail\n"
                                                                  "          IntLiteral 3\n");
    REQUIRE(expr_dump("if a { 1 }") == "If\n"
                                       "  condition: NameRef a\n"
                                       "  then: Block\n"
                                       "    ExprStmt tail\n"
                                       "      IntLiteral 1\n");
    REQUIRE(expr_dump("(if a { 1 } else { 2 }) + 1") == "Binary +\n"
                                                        "  If\n"
                                                        "    condition: NameRef a\n"
                                                        "    then: Block\n"
                                                        "      ExprStmt tail\n"
                                                        "        IntLiteral 1\n"
                                                        "    else: BlockExpr\n"
                                                        "      Block\n"
                                                        "        ExprStmt tail\n"
                                                        "          IntLiteral 2\n"
                                                        "  IntLiteral 1\n");
}

TEST_CASE("else may start the line after the then-block", "[parser]")
{
    REQUIRE(body_dump("    let x = if a {\n"
                      "        1\n"
                      "    }\n"
                      "    else {\n"
                      "        2\n"
                      "    }\n"
                      "    x") == "Block\n"
                                  "  LocalDecl let x\n"
                                  "    init: If\n"
                                  "      condition: NameRef a\n"
                                  "      then: Block\n"
                                  "        ExprStmt tail\n"
                                  "          IntLiteral 1\n"
                                  "      else: BlockExpr\n"
                                  "        Block\n"
                                  "          ExprStmt tail\n"
                                  "            IntLiteral 2\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef x\n");
}

TEST_CASE("eval expressions", "[parser]")
{
    REQUIRE(expr_dump("eval(f, x: [1.0], y: [2.0])") == "Eval\n"
                                                        "  callee: NameRef f\n"
                                                        "  Argument x\n"
                                                        "    SequenceLiteral\n"
                                                        "      FloatLiteral 1.0\n"
                                                        "  Argument y\n"
                                                        "    SequenceLiteral\n"
                                                        "      FloatLiteral 2.0\n");
    REQUIRE(expr_dump("eval(mc::f)") == "Eval\n"
                                        "  callee: QualifiedRef mc::f\n");
    REQUIRE(expr_dump("eval(\n    f,\n    x: [1.0],\n)") == "Eval\n"
                                                           "  callee: NameRef f\n"
                                                           "  Argument x\n"
                                                           "    SequenceLiteral\n"
                                                           "      FloatLiteral 1.0\n");
}

TEST_CASE("a block in expression position is a block expression", "[parser]")
{
    REQUIRE(expr_dump("{ let y = 1\n y }") == "BlockExpr\n"
                                              "  Block\n"
                                              "    LocalDecl let y\n"
                                              "      init: IntLiteral 1\n"
                                              "    ExprStmt tail\n"
                                              "      NameRef y\n");
}

TEST_CASE("expression ranges span the whole expression", "[parser]")
{
    Parsed parsed{"module t\nfn f() => a + b * c\n"};
    REQUIRE_FALSE(parsed.diagnostics.has_errors());
    const auto &decl = std::get<ast::FunctionDecl>(parsed.module.decl(parsed.module.declarations.at(1)).node);
    const ast::Expr &body = parsed.module.expr(decl.concise_body);
    REQUIRE(parsed.file.slice(body.range) == "a + b * c");
    const auto &sum = std::get<ast::Binary>(body.node);
    REQUIRE(parsed.file.slice(parsed.module.expr(sum.rhs).range) == "b * c");
}

// --------------------------------------------------------------- statements

TEST_CASE("let and var declarations", "[parser]")
{
    REQUIRE(body_dump("    let a = 1\n"
                      "    var b: f64 = 2.0\n"
                      "    a") == "Block\n"
                                  "  LocalDecl let a\n"
                                  "    init: IntLiteral 1\n"
                                  "  LocalDecl var b\n"
                                  "    type: Type scalar f64\n"
                                  "    init: FloatLiteral 2.0\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef a\n");
    REQUIRE(Parsed{"module t\nfn f() {\n    let a\n}\n"}.messages() ==
            std::vector<std::string>{"expected '=' and an initializer, found newline"});
}

TEST_CASE("state declarations use value types", "[parser]")
{
    REQUIRE(body_dump("    state total: f64 = 0.0\n"
                      "    state seen = 0\n"
                      "    total") == "Block\n"
                                      "  StateDecl total\n"
                                      "    type: Type scalar f64 (value)\n"
                                      "    init: FloatLiteral 0.0\n"
                                      "  StateDecl seen\n"
                                      "    init: IntLiteral 0\n"
                                      "  ExprStmt tail\n"
                                      "    NameRef total\n");
}

TEST_CASE("inject declarations", "[parser]")
{
    REQUIRE(body_dump("    inject out, logger\n"
                      "    inject clock\n"
                      "    inject\n"
                      "        scheduler,\n"
                      "        state_store,\n"
                      "    0") == "Block\n"
                                  "  InjectDecl out, logger\n"
                                  "  InjectDecl clock\n"
                                  "  InjectDecl scheduler, state_store\n"
                                  "  ExprStmt tail\n"
                                  "    IntLiteral 0\n");
}

TEST_CASE("start and stop blocks", "[parser]")
{
    REQUIRE(body_dump("    start {\n"
                      "        logger.info(\"up\")\n"
                      "    }\n"
                      "    stop {\n"
                      "        logger.info(\"down\")\n"
                      "    }") == "Block\n"
                                  "  Start\n"
                                  "    Block\n"
                                  "      ExprStmt tail\n"
                                  "        Call\n"
                                  "          callee: Field info\n"
                                  "            NameRef logger\n"
                                  "          Argument\n"
                                  "            StringLiteral \"up\"\n"
                                  "  Stop\n"
                                  "    Block\n"
                                  "      ExprStmt tail\n"
                                  "        Call\n"
                                  "          callee: Field info\n"
                                  "            NameRef logger\n"
                                  "          Argument\n"
                                  "            StringLiteral \"down\"\n");
}

TEST_CASE("when blocks", "[parser]")
{
    REQUIRE(body_dump("    when modified(a) {\n"
                      "        total += a\n"
                      "    }\n"
                      "    out = total") == "Block\n"
                                            "  When\n"
                                            "    condition: Call\n"
                                            "      callee: NameRef modified\n"
                                            "      Argument\n"
                                            "        NameRef a\n"
                                            "    Block\n"
                                            "      Assign +=\n"
                                            "        place: NameRef total\n"
                                            "        value: NameRef a\n"
                                            "  Assign =\n"
                                            "    place: NameRef out\n"
                                            "    value: NameRef total\n");
}

TEST_CASE("for loops over one or two names", "[parser]")
{
    REQUIRE(body_dump("    for k, v in m {\n"
                      "        sum += v\n"
                      "    }\n"
                      "    for x in xs {\n"
                      "        sum += x\n"
                      "    }") == "Block\n"
                                  "  For k, v\n"
                                  "    in: NameRef m\n"
                                  "    Block\n"
                                  "      Assign +=\n"
                                  "        place: NameRef sum\n"
                                  "        value: NameRef v\n"
                                  "  For x\n"
                                  "    in: NameRef xs\n"
                                  "    Block\n"
                                  "      Assign +=\n"
                                  "        place: NameRef sum\n"
                                  "        value: NameRef x\n");
    REQUIRE(body_dump("    for x in modified(xs) {\n"
                      "        sum += x\n"
                      "    }") == "Block\n"
                                  "  For x\n"
                                  "    in: Call\n"
                                  "      callee: NameRef modified\n"
                                  "      Argument\n"
                                  "        NameRef xs\n"
                                  "    Block\n"
                                  "      Assign +=\n"
                                  "        place: NameRef sum\n"
                                  "        value: NameRef x\n");
    REQUIRE(Parsed{"module t\nfn f() {\n    for x xs {\n    }\n}\n"}.messages() ==
            std::vector<std::string>{"expected 'in' after the loop pattern, found 'xs'"});
}

TEST_CASE("assignments", "[parser]")
{
    REQUIRE(body_dump("    a = 1\n"
                      "    a += 1\n"
                      "    a -= 1\n"
                      "    a *= 2\n"
                      "    a /= 2\n"
                      "    m[k] = v\n"
                      "    q.bid = 1.0\n"
                      "    m[k].bid = 1.0") == "Block\n"
                                               "  Assign =\n"
                                               "    place: NameRef a\n"
                                               "    value: IntLiteral 1\n"
                                               "  Assign +=\n"
                                               "    place: NameRef a\n"
                                               "    value: IntLiteral 1\n"
                                               "  Assign -=\n"
                                               "    place: NameRef a\n"
                                               "    value: IntLiteral 1\n"
                                               "  Assign *=\n"
                                               "    place: NameRef a\n"
                                               "    value: IntLiteral 2\n"
                                               "  Assign /=\n"
                                               "    place: NameRef a\n"
                                               "    value: IntLiteral 2\n"
                                               "  Assign =\n"
                                               "    place: Index\n"
                                               "      NameRef m\n"
                                               "      index: NameRef k\n"
                                               "    value: NameRef v\n"
                                               "  Assign =\n"
                                               "    place: Field bid\n"
                                               "      NameRef q\n"
                                               "    value: FloatLiteral 1.0\n"
                                               "  Assign =\n"
                                               "    place: Field bid\n"
                                               "      Index\n"
                                               "        NameRef m\n"
                                               "        index: NameRef k\n"
                                               "    value: FloatLiteral 1.0\n");
    REQUIRE(Parsed{"module t\nfn f() {\n    f(x) = 1\n}\n"}.messages() ==
            std::vector<std::string>{"only a name, an index, or a field can be assigned to"});
    REQUIRE(Parsed{"module t\nfn f() {\n    a + b = 1\n}\n"}.messages() ==
            std::vector<std::string>{"only a name, an index, or a field can be assigned to"});
}

TEST_CASE("return and assert statements", "[parser]")
{
    REQUIRE(body_dump("    if a {\n"
                      "        return\n"
                      "    }\n"
                      "    return a + 1") == "Block\n"
                                             "  ExprStmt\n"
                                             "    If\n"
                                             "      condition: NameRef a\n"
                                             "      then: Block\n"
                                             "        Return\n"
                                             "  Return\n"
                                             "    Binary +\n"
                                             "      NameRef a\n"
                                             "      IntLiteral 1\n");
    REQUIRE(body_dump("    assert a == b\n"
                      "    a") == "Block\n"
                                  "  Assert\n"
                                  "    Binary ==\n"
                                  "      NameRef a\n"
                                  "      NameRef b\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef a\n");
    REQUIRE(body_dump("    if a { return }") == "Block\n"
                                                "  ExprStmt tail\n"
                                                "    If\n"
                                                "      condition: NameRef a\n"
                                                "      then: Block\n"
                                                "        Return\n");
}

TEST_CASE("expression statements and the block tail", "[parser]")
{
    REQUIRE(body_dump("    f(x)\n"
                      "    g(y)") == "Block\n"
                                     "  ExprStmt\n"
                                     "    Call\n"
                                     "      callee: NameRef f\n"
                                     "      Argument\n"
                                     "        NameRef x\n"
                                     "  ExprStmt tail\n"
                                     "    Call\n"
                                     "      callee: NameRef g\n"
                                     "      Argument\n"
                                     "        NameRef y\n");
    REQUIRE(body_dump("") == "Block\n");
    REQUIRE(body_dump("    let a = 1") == "Block\n"
                                          "  LocalDecl let a\n"
                                          "    init: IntLiteral 1\n");
}

TEST_CASE("statement ranges", "[parser]")
{
    Parsed parsed{"module t\nfn f() {\n    var a = 1\n    a += 2\n}\n"};
    REQUIRE_FALSE(parsed.diagnostics.has_errors());
    const auto &decl = std::get<ast::FunctionDecl>(parsed.module.decl(parsed.module.declarations.at(1)).node);
    const ast::Block &block = parsed.module.block(decl.block_body);
    REQUIRE(block.statements.size() == 2);
    REQUIRE(parsed.file.slice(parsed.module.stmt(block.statements[0]).range) == "var a = 1");
    REQUIRE(parsed.file.slice(parsed.module.stmt(block.statements[1]).range) == "a += 2");
    REQUIRE(parsed.file.slice(block.range) == "{\n    var a = 1\n    a += 2\n}");
    REQUIRE(block.tail == ast::no_node);
}

// ----------------------------------------------------------------- newlines

TEST_CASE("newlines separate statements", "[parser]")
{
    REQUIRE(Parsed{"module t\nfn f() {\n    let a = 1 let b = 2\n}\n"}.messages() ==
            std::vector<std::string>{"expected a newline after the statement, found 'let'"});
    REQUIRE(Parsed{"module t\nfn f() => 1 fn g() => 2\n"}.messages() ==
            std::vector<std::string>{"expected a newline after the declaration, found 'fn'"});
}

TEST_CASE("newlines are skipped inside brackets and after commas", "[parser]")
{
    REQUIRE(expr_dump("f(\n    a\n    ,\n    b\n)") == "Call\n"
                                                      "  callee: NameRef f\n"
                                                      "  Argument\n"
                                                      "    NameRef a\n"
                                                      "  Argument\n"
                                                      "    NameRef b\n");
    REQUIRE(expr_dump("xs[\n    0\n]") == "Index\n"
                                          "  NameRef xs\n"
                                          "  index: IntLiteral 0\n");
    REQUIRE(type_dump("map<\n    str,\n    f64\n>") == "Type map\n"
                                                      "  Type scalar str (value)\n"
                                                      "  Type scalar f64\n");
    REQUIRE(dump_clean("module t\nfn f<\n    T,\n    const N: i64\n>(x: T) => x\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  FunctionDecl fn f\n"
            "    GenericParameter T\n"
            "    GenericParameter const N\n"
            "      type: Type scalar i64 (value)\n"
            "    Parameter x\n"
            "      type: Type named T\n"
            "    body: NameRef x\n");
}

TEST_CASE("a binary operator at the end of a line continues the expression", "[parser]")
{
    REQUIRE(body_dump("    let a = b +\n"
                      "        c\n"
                      "    a") == "Block\n"
                                  "  LocalDecl let a\n"
                                  "    init: Binary +\n"
                                  "      NameRef b\n"
                                  "      NameRef c\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef a\n");
    REQUIRE(body_dump("    let ok = a &&\n"
                      "        b ||\n"
                      "        c\n"
                      "    ok") == "Block\n"
                                   "  LocalDecl let ok\n"
                                   "    init: Binary ||\n"
                                   "      Binary &&\n"
                                   "        NameRef a\n"
                                   "        NameRef b\n"
                                   "      NameRef c\n"
                                   "  ExprStmt tail\n"
                                   "    NameRef ok\n");
}

TEST_CASE("a line starting with a binary operator continues the expression", "[parser]")
{
    REQUIRE(body_dump("    let a = b\n"
                      "        + c\n"
                      "        * d\n"
                      "    a") == "Block\n"
                                  "  LocalDecl let a\n"
                                  "    init: Binary +\n"
                                  "      NameRef b\n"
                                  "      Binary *\n"
                                  "        NameRef c\n"
                                  "        NameRef d\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef a\n");
}

TEST_CASE("a line starting with a unary operator is a new statement", "[parser]")
{
    // `!` is never binary, so the second line is its own statement.
    REQUIRE(body_dump("    let a = b\n"
                      "    !a") == "Block\n"
                                   "  LocalDecl let a\n"
                                   "    init: NameRef b\n"
                                   "  ExprStmt tail\n"
                                   "    Unary !\n"
                                   "      NameRef a\n");
}

TEST_CASE("postfix operators do not cross a newline", "[parser]")
{
    REQUIRE(body_dump("    let a = b\n"
                      "    (c)") == "Block\n"
                                    "  LocalDecl let a\n"
                                    "    init: NameRef b\n"
                                    "  ExprStmt tail\n"
                                    "    NameRef c\n");
    REQUIRE(body_dump("    let a = b\n"
                      "    [c]") == "Block\n"
                                    "  LocalDecl let a\n"
                                    "    init: NameRef b\n"
                                    "  ExprStmt tail\n"
                                    "    SequenceLiteral\n"
                                    "      NameRef c\n");
}

TEST_CASE("newlines after '=', '=>', '->', and ':' are skipped", "[parser]")
{
    REQUIRE(dump_clean("module t\n"
                       "fn f(window: rolling<T, max_size, min_size>)\n"
                       "    -> rolling<T, max_size, min_size> =>\n"
                       "    window\n") == "Module\n"
                                          "  ModuleDecl t\n"
                                          "  FunctionDecl fn f\n"
                                          "    Parameter window\n"
                                          "      type: Type rolling\n"
                                          "        Type named T (value)\n"
                                          "        size: NameRef max_size\n"
                                          "        min: NameRef min_size\n"
                                          "    result: Type rolling\n"
                                          "      Type named T (value)\n"
                                          "      size: NameRef max_size\n"
                                          "      min: NameRef min_size\n"
                                          "    body: NameRef window\n");
    REQUIRE(body_dump("    let a =\n"
                      "        1\n"
                      "    let b:\n"
                      "        f64 = 2.0\n"
                      "    a") == "Block\n"
                                  "  LocalDecl let a\n"
                                  "    init: IntLiteral 1\n"
                                  "  LocalDecl let b\n"
                                  "    type: Type scalar f64\n"
                                  "    init: FloatLiteral 2.0\n"
                                  "  ExprStmt tail\n"
                                  "    NameRef a\n");
}

TEST_CASE("blank lines and comments between declarations and statements are fine", "[parser]")
{
    Parsed parsed{"// leading\n"
                  "module t\n"
                  "\n"
                  "// about f\n"
                  "fn f() {\n"
                  "\n"
                  "    let a = 1 // trailing\n"
                  "\n"
                  "    a\n"
                  "}\n"
                  "\n"};
    REQUIRE_FALSE(parsed.diagnostics.has_errors());
    REQUIRE(parsed.module.comments.size() == 3);
    REQUIRE(parsed.file.slice(parsed.module.comments[1].range) == "// about f");
    REQUIRE(dump(parsed) == "Module\n"
                            "  ModuleDecl t\n"
                            "  FunctionDecl fn f\n"
                            "    body: Block\n"
                            "      LocalDecl let a\n"
                            "        init: IntLiteral 1\n"
                            "      ExprStmt tail\n"
                            "        NameRef a\n"
                            "  Comment\n"
                            "  Comment\n"
                            "  Comment\n");
}

TEST_CASE("a file may end without a trailing newline", "[parser]")
{
    REQUIRE(dump_clean("module t\nfn f() => 1") == "Module\n"
                                                   "  ModuleDecl t\n"
                                                   "  FunctionDecl fn f\n"
                                                   "    body: IntLiteral 1\n");
    REQUIRE(dump_clean("module t") == "Module\n  ModuleDecl t\n");
    Parsed parsed{""};
    REQUIRE(parsed.messages() ==
            std::vector<std::string>{"expected a 'module' declaration at the start of the file"});
    REQUIRE(dump(parsed) == "Module\n");
}

// -------------------------------------------------------------- diagnostics

TEST_CASE("reserved words cannot be used as names", "[parser]")
{
    Parsed parsed{"module t\nfn f(let: f64) => 1\n"};
    REQUIRE(parsed.messages() ==
            std::vector<std::string>{"'let' is a reserved word and cannot be used as a parameter name"});
    REQUIRE(parsed.module.declarations.size() == 2);  // the declaration is still built

    REQUIRE(Parsed{"module t\nfn fn() => 1\n"}.messages() ==
            std::vector<std::string>{"'fn' is a reserved word and cannot be used as a function name"});
    REQUIRE(Parsed{"module t\nfn f() {\n    let state = 1\n    2\n}\n"}.messages() ==
            std::vector<std::string>{"'state' is a reserved word and cannot be used as a variable name"});
    REQUIRE(Parsed{"module t\nuse a.b::{fn}\n"}.messages() ==
            std::vector<std::string>{"'fn' is a reserved word and cannot be used as an imported name"});
}

TEST_CASE("contextual keywords are ordinary names", "[parser]")
{
    REQUIRE(dump_clean("module t\nfn f(in: f64, out: f64, unbounded: f64, atomic: f64) => in\n") ==
            "Module\n"
            "  ModuleDecl t\n"
            "  FunctionDecl fn f\n"
            "    Parameter in\n"
            "      type: Type scalar f64\n"
            "    Parameter out\n"
            "      type: Type scalar f64\n"
            "    Parameter unbounded\n"
            "      type: Type scalar f64\n"
            "    Parameter atomic\n"
            "      type: Type scalar f64\n"
            "    body: NameRef in\n");
}

TEST_CASE("diagnostics carry the offending range", "[parser]")
{
    Parsed parsed{"module t\nfn f() => )\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected an expression, found ')'"});
    REQUIRE(parsed.file.slice(parsed.diagnostics.diagnostics().at(0).range) == ")");

    Parsed missing{"module t\nfn f( => 1\n"};
    REQUIRE(missing.messages() == std::vector<std::string>{"expected a parameter name, found '=>'"});
    REQUIRE(missing.diagnostics.diagnostics().at(0).range.begin == 15);
}

TEST_CASE("a bad declaration reports once and parsing resumes at the next declaration", "[parser]")
{
    Parsed parsed{"module t\n"
                  "fn broken(x: f64 => x\n"
                  "    still junk here\n"
                  "fn ok() => 1\n"
                  "export fn also_ok() => 2\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected ',' or ')', found '=>'"});
    REQUIRE(dump(parsed) == "Module\n"
                            "  ModuleDecl t\n"
                            "  FunctionDecl fn ok\n"
                            "    body: IntLiteral 1\n"
                            "  FunctionDecl export fn also_ok\n"
                            "    body: IntLiteral 2\n");
}

TEST_CASE("recovery finds every declaration keyword", "[parser]")
{
    Parsed parsed{"module t\n"
                  "fn a( => 1\n"
                  "operator b(x: f64) -> f64\n"
                  "fn c( => 1\n"
                  "impl fn d(x: f64) -> f64 => x\n"
                  "fn e( => 1\n"
                  "test t {\n"
                  "}\n"
                  "fn g( => 1\n"
                  "use z::{q}\n"};
    REQUIRE(parsed.diagnostics.size() == 5);  // four broken fns and the late `use`
    REQUIRE(dump(parsed) == "Module\n"
                            "  ModuleDecl t\n"
                            "  OperatorDecl b\n"
                            "    Parameter x\n"
                            "      type: Type scalar f64\n"
                            "    result: Type scalar f64\n"
                            "  FunctionDecl impl fn d\n"
                            "    Parameter x\n"
                            "      type: Type scalar f64\n"
                            "    result: Type scalar f64\n"
                            "    body: NameRef x\n"
                            "  TestDecl t\n"
                            "    body: Block\n"
                            "  UseDecl z::{q}\n");
}

TEST_CASE("a bad statement reports once and the rest of the block parses", "[parser]")
{
    Parsed parsed{"module t\n"
                  "fn f() {\n"
                  "    let a = (1 + \n"
                  "    let b = 2\n"
                  "    a + b\n"
                  "}\n"
                  "fn g() => 3\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected an expression, found 'let'"});
    REQUIRE(dump(parsed) == "Module\n"
                            "  ModuleDecl t\n"
                            "  FunctionDecl fn f\n"
                            "    body: Block\n"
                            "      ExprStmt tail\n"
                            "        Binary +\n"
                            "          NameRef a\n"
                            "          NameRef b\n"
                            "  FunctionDecl fn g\n"
                            "    body: IntLiteral 3\n");
}

TEST_CASE("an unterminated block is reported at end of file", "[parser]")
{
    Parsed parsed{"module t\nfn f() {\n    let a = 1\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected '}' to close the block, found end of file"});
}

TEST_CASE("bad top-level tokens report one diagnostic", "[parser]")
{
    Parsed parsed{"module t\n42\nfn f() => 1\n"};
    REQUIRE(parsed.messages() == std::vector<std::string>{"expected a declaration, found '42'"});
    REQUIRE(parsed.module.declarations.size() == 2);
}

TEST_CASE("missing function body", "[parser]")
{
    REQUIRE(Parsed{"module t\nfn f()\nfn g() => 1\n"}.messages() ==
            std::vector<std::string>{"expected '=>' or '{' to begin the function body, found newline"});
}

TEST_CASE("lexer errors surface through the parser", "[parser]")
{
    Parsed parsed{"module t\nfn f() => \"unterminated\n"};
    REQUIRE(parsed.diagnostics.has_errors());
    // The lexer's own message comes first; the parser adds nothing for the
    // Error token it produced.
    REQUIRE(parsed.diagnostics.size() == 1);
}

// ----------------------------------------------------------------- examples

TEST_CASE("every example parses cleanly", "[parser][examples]")
{
    const std::filesystem::path directory{HGL_EXAMPLES_DIR};
    REQUIRE(std::filesystem::is_directory(directory));

    std::vector<std::filesystem::path> examples;
    for (const auto &entry : std::filesystem::directory_iterator{directory})
    {
        if (entry.path().extension() == ".hgl") { examples.push_back(entry.path()); }
    }
    std::sort(examples.begin(), examples.end());
    REQUIRE(examples.size() >= 6);

    for (const std::filesystem::path &path : examples)
    {
        std::ifstream in{path};
        REQUIRE(in.is_open());
        std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};

        SourceFile     file{path.string(), std::move(text)};
        DiagnosticSink diagnostics;
        ast::Module    module = parse(file, diagnostics);
        INFO(path.filename().string() << "\n" << diagnostics.render(file));
        CHECK_FALSE(diagnostics.has_errors());
        CHECK(!module.declarations.empty());
        CHECK(std::holds_alternative<ast::ModuleDecl>(module.decl(module.declarations.front()).node));
        // The dump must reach every node without tripping an arena bound.
        CHECK(!print_ast(module).empty());
    }
}
