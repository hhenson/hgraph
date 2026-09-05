#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

using namespace hgl::syntax;
using namespace hgl::semantics;

namespace
{
    // The resolver asks the registry one question; the tests answer it from
    // a table so they stay hgraph-free (developer guide, "Frontend
    // components").
    const std::vector<std::string_view> registry{
        "add_", "div_", "map_", "keys_", "valid", "modified", "mean", "if_then_else", "schedule", "hgraph.analytics.rolling_mean",
    };

    bool table_lookup(std::string_view name) { return std::find(registry.begin(), registry.end(), name) != registry.end(); }

    struct Resolved
    {
        SourceFile     file;
        DiagnosticSink diagnostics;
        ast::Module    module;
        ResolvedModule result;

        explicit Resolved(std::string text)
            : file{"test.hgl", std::move(text)}, module{parse(file, diagnostics)},
              result{resolve(file, module, table_lookup, diagnostics)} {}

        [[nodiscard]] std::vector<std::string> messages() const {
            std::vector<std::string> out;
            for (const Diagnostic &diagnostic : diagnostics.diagnostics()) { out.push_back(diagnostic.message); }
            return out;
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const {
            return std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(), [&](const Diagnostic &d) {
                return d.category == category && d.message.find(fragment) != std::string::npos;
            });
        }

        // The binding of the first (possibly qualified) name spelled `text`.
        [[nodiscard]] const Binding *binding_of(std::string_view text) const {
            for (ast::ExprId id = 0; id < module.exprs.size(); ++id) {
                const auto *name      = std::get_if<ast::NameRef>(&module.expr(id).node);
                const auto *qualified = std::get_if<ast::QualifiedRef>(&module.expr(id).node);
                if ((name != nullptr && name->name.text == text) || (qualified != nullptr && qualified->name.text == text)) {
                    return &result.binding(id);
                }
            }
            return nullptr;
        }

        [[nodiscard]] const ast::FunctionDecl &function(std::string_view text) const {
            for (const ast::DeclId id : result.functions) {
                const auto &fn = std::get<ast::FunctionDecl>(module.decl(id).node);
                if (fn.name.text == text) { return fn; }
            }
            FAIL("no function " << text);
            throw 0;
        }

        [[nodiscard]] FunctionKind kind_of(std::string_view text) const {
            for (const ast::DeclId id : result.functions) {
                if (std::get<ast::FunctionDecl>(module.decl(id).node).name.text == text) { return result.kind(id); }
            }
            FAIL("no function " << text);
            throw 0;
        }

        [[nodiscard]] ast::DeclId struct_id(std::string_view text) const {
            for (const ast::DeclId id : result.structs) {
                if (std::get<ast::StructDecl>(module.decl(id).node).name.text == text) { return id; }
            }
            FAIL("no struct " << text);
            throw 0;
        }
    };

    Resolved resolve_clean(std::string text) {
        Resolved resolved{std::move(text)};
        INFO(resolved.diagnostics.render(resolved.file));
        REQUIRE_FALSE(resolved.diagnostics.has_errors());
        return resolved;
    }
}  // namespace

TEST_CASE("kernel imports bind to registry names", "[semantics]") {
    const Resolved resolved = resolve_clean(R"(
module t

use hgraph.std::{map, valid, schedule}
use hgraph.analytics::{rolling_mean}
use hgraph.std as std

fn f(x: f64) -> f64 => std::add(x, 1.0)
)");
    REQUIRE(resolved.result.module_path == "t");
    REQUIRE(resolved.result.imports.size() == 4);
    CHECK(resolved.result.imports[0].registry_name == "map_");
    CHECK(resolved.result.imports[1].registry_name == "valid");
    CHECK(resolved.result.imports[2].registry_name == "schedule");
    CHECK(resolved.result.imports[3].registry_name == "hgraph.analytics.rolling_mean");
    CHECK(resolved.binding_of("add")->kind == BindingKind::Operator);
    CHECK(resolved.binding_of("add")->registry_name == "add_");
    REQUIRE(resolved.result.aliases.size() == 1);
    CHECK(resolved.result.aliases[0].module == "hgraph.std");
}

TEST_CASE("only the kernel modules link in the first pass", "[semantics]") {
    const Resolved resolved{"module t\n\nuse market.pricing::{value}\nuse "
                            "hgraph.std::{nothing_like_this}\n"};
    CHECK(resolved.has(Category::Module, "module 'market.pricing' is not available"));
    CHECK(resolved.has(Category::Module, "hgraph.std does not export 'nothing_like_this'"));
}

TEST_CASE("names resolve through the scope chain", "[semantics]") {
    const Resolved resolved = resolve_clean(R"(
module t

use hgraph.std::{map}

fn helper(x: f64) -> f64 => x

fn f(y: f64, const k: i64 = 2) -> f64 {
    let z = helper(y)
    valid(z)
    z
}
)");
    REQUIRE(resolved.binding_of("z") != nullptr);
    CHECK(resolved.binding_of("z")->kind == BindingKind::Local);
    CHECK(resolved.binding_of("y")->kind == BindingKind::Parameter);
    CHECK(resolved.binding_of("y")->index == 0);
    CHECK(resolved.binding_of("helper")->kind == BindingKind::Function);
    CHECK(resolved.binding_of("valid")->kind == BindingKind::Intrinsic);
    CHECK(resolved.binding_of("valid")->registry_name == "valid");
    CHECK(is_intrinsic("key_set"));
    CHECK_FALSE(is_intrinsic("helper"));
}

TEST_CASE("an import shadows an intrinsic and a parameter shadows both", "[semantics]") {
    const Resolved resolved = resolve_clean(R"(
module t

use hgraph.std::{valid}

fn f(values: f64) -> f64 => values

fn g(x: f64) -> f64 => valid(x)
)");
    CHECK(resolved.binding_of("values")->kind == BindingKind::Parameter);
    CHECK(resolved.binding_of("valid")->kind == BindingKind::Operator);
}

TEST_CASE("unknown names and misuse are reported", "[semantics]") {
    const Resolved resolved{R"(
module t

fn f(x: f64) -> f64 => x + nope

fn g(x: f64) -> f64 {
    assert x == 1.0
    eval(f, x: [1.0])
}

test t {
    let a = _
}
)"};
    CHECK(resolved.has(Category::Name, "unknown name 'nope'"));
    CHECK(resolved.has(Category::Phase, "'assert' is only valid inside a test body"));
    CHECK(resolved.has(Category::Phase, "eval is only valid inside a test body"));
    CHECK(resolved.has(Category::Type, "'_' is only valid in a harness sequence"));
}

TEST_CASE("functions classify from their statement forms", "[semantics]") {
    const Resolved resolved = resolve_clean(R"(
module t

fn compose(x: f64) -> f64 => x

fn runtime(x: f64) -> f64 {
    state total: f64 = 0.0
    inject out
    when x { total += x }
    out = total
}

fn lifecycle(x: f64) -> f64 {
    inject out
    start { }
    out = x
}
)");
    CHECK(resolved.kind_of("compose") == FunctionKind::Composition);
    CHECK(resolved.kind_of("runtime") == FunctionKind::Runtime);
    CHECK(resolved.kind_of("lifecycle") == FunctionKind::Runtime);
    CHECK(resolved.result.functions.size() == 3);
}

TEST_CASE("implementations bind to an operator in scope", "[semantics]") {
    const Resolved bound = resolve_clean(R"(
module t

use hgraph.std::{valid}

impl fn valid(x: f64) -> bool => true
)");
    CHECK(bound.result.functions.size() == 1);

    const Resolved unbound{"module t\n\nimpl fn nothing(x: f64) -> bool => true\n"};
    CHECK(unbound.has(Category::Module, "'impl fn nothing' has no operator named 'nothing' in scope"));

    const Resolved clash{"module t\n\nuse hgraph.std::{valid}\n\nfn valid(x: "
                         "f64) -> bool => true\n"};
    CHECK(clash.has(Category::Name, "'fn valid' conflicts with operator hgraph.std::valid"));
}

TEST_CASE("tests are declarations, not values", "[semantics]") {
    const Resolved resolved{R"(
module t

fn f(x: f64) -> f64 => x

test check_f {
    assert eval(f, x: [1.0, _]) == [1.0, _]
}

fn g(x: f64) -> f64 => check_f
)"};
    CHECK(resolved.result.tests.size() == 1);
    CHECK(resolved.has(Category::Name, "'check_f' is a test, not a value"));
}

TEST_CASE("struct hierarchy resolves effective fields and defaults", "[semantics]") {
    const Resolved    resolved   = resolve_clean(R"(
module t

abstract struct Instrument {
    symbol: str
    venue: str = "ANY"
    alias: str = null
}

struct Future: Instrument {
    venue = "XEUR"
    expiry: date
}

fn make() -> atomic<Future> => Future(symbol: "F", expiry: @2026-12-18)
)");
    const ast::DeclId instrument = resolved.struct_id("Instrument");
    const ast::DeclId future     = resolved.struct_id("Future");
    REQUIRE(resolved.result.structure(instrument).valid);
    REQUIRE(resolved.result.structure(future).valid);
    REQUIRE(resolved.result.structure(future).parents == std::vector<ast::DeclId>{instrument});
    const auto &fields = resolved.result.structure(future).fields;
    REQUIRE(fields.size() == 4);
    CHECK(fields[0].name == "symbol");
    CHECK(fields[1].name == "venue");
    CHECK(fields[1].default_value != ast::no_node);
    CHECK(fields[2].name == "alias");
    CHECK(fields[2].optional);
    CHECK(fields[3].name == "expiry");
}

TEST_CASE("struct hierarchy rejects unsafe inheritance", "[semantics]") {
    SECTION("concrete parents are final") {
        const Resolved resolved{"module t\nstruct Base {}\nstruct Child: Base {}\n"};
        CHECK(resolved.has(Category::Type, "only an abstract struct may be inherited"));
    }
    SECTION("inherited field types cannot be redeclared") {
        const Resolved resolved{"module t\nabstract struct Base { value: f64 "
                                "}\nstruct Child: Base { value: i64 }\n"};
        CHECK(resolved.has(Category::Type, "cannot be redeclared with a type"));
    }
    SECTION("required fields cannot become optional") {
        const Resolved resolved{"module t\nabstract struct Base { value: f64 "
                                "}\nstruct Child: Base { value = null }\n"};
        CHECK(resolved.has(Category::Type, "only an optional inherited field may have a null default"));
    }
    SECTION("self recursion and inheritance cycles are rejected") {
        const Resolved recursive{"module t\nstruct Node { next: Node }\n"};
        CHECK(recursive.has(Category::Type, "self-recursive struct fields are not supported"));
        const Resolved cycle{"module t\nabstract struct A: B {}\nabstract struct B: A {}\n"};
        CHECK(cycle.has(Category::Type, "struct inheritance cycle reaches"));
    }
    SECTION("multiple parent order fails closed") {
        const Resolved resolved{"module t\nabstract struct A {}\nabstract struct B "
                                "{}\nstruct C: A, B {}\n"};
        CHECK(resolved.has(Category::Type, "awaits the stable field-order rule"));
    }
}

TEST_CASE("generic structs validate applications and decidable requirements", "[semantics]") {
    const Resolved valid = resolve_clean(R"(
module t

struct Range<T>
requires T in {i64, f64}
{
    value: T
}

struct Vector<T, const size: i64> {
    values: list<T, size>
}

fn range(x: Range<f64>) -> Range<f64> => x
fn vector(x: Vector<f64, 3>) -> Vector<f64, 3> => x
)");
    CHECK(valid.result.structure(valid.struct_id("Range")).valid);

    const Resolved rejected{R"(
module t
struct Range<T> requires T in {i64, f64} { value: T }
fn bad(x: Range<str>) -> Range<str> => x
)"};
    CHECK(rejected.has(Category::Type, "generic struct 'Range' requirements are not satisfied"));

    const Resolved wrong_roles{R"(
module t
struct Vector<T, const size: i64> { values: list<T, size> }
fn bad(x: Vector<3, f64>) => x
)"};
    CHECK(wrong_roles.has(Category::Type, "type generic 'T' takes a type argument"));
    CHECK(wrong_roles.has(Category::Type, "const generic 'size' takes a value argument"));

    const Resolved temporal_argument{R"(
module t
struct Box<T> { value: T }
fn bad(x: Box<atomic<f64>>) => x
)"};
    CHECK(temporal_argument.has(Category::Type, "generic struct type arguments are canonical value types"));
}

TEST_CASE("requires clauses bind reflection and nominal operators", "[semantics]") {
    const Resolved resolved = resolve_clean(R"(
module t
use hgraph.std as std

abstract struct Record { id: i64 }

fn combine<T>(a: T, b: T) -> T
requires T is struct && has_fields(T, {"id"}) && std::add(T, T) -> T
{
    a
}
)");
    CHECK_FALSE(resolved.result.constraint_bindings.empty());

    const Resolved bad{R"(
module t
fn f<T>(x: T) -> T requires T is class && mystery(T) { x }
)"};
    CHECK(bad.has(Category::Type, "unknown type category 'class'"));
    CHECK(bad.has(Category::Type, "is not a compile-time reflection function"));
}

TEST_CASE("struct construction enforces complete and sparse forms", "[semantics]") {
    const Resolved valid = resolve_clean(R"(
module t
struct Quote {
    bid: f64
    ask: f64
    venue: str = "X"
    note: str = null
}
fn full() -> atomic<Quote> => Quote(bid: 1.0, ask: 2.0)
fn update() -> atomic<Quote> => delta<Quote>(bid: 1.5)
)");
    CHECK(valid.result.structure(valid.struct_id("Quote")).valid);

    const Resolved invalid{R"(
module t
abstract struct Base { id: i64 }
struct Quote {
    bid: f64
    note: str = null
}
fn missing() => Quote()
fn unknown() => Quote(bid: 1.0, extra: 2.0)
fn duplicate() => Quote(bid: 1.0, bid: 2.0)
fn bad_null() => Quote(bid: null)
fn positional() => Quote(1.0)
fn abstract_value() => Base(id: 1)
)"};
    CHECK(invalid.has(Category::Type, "struct 'Quote' needs field 'bid'"));
    CHECK(invalid.has(Category::Name, "has no field named 'extra'"));
    CHECK(invalid.has(Category::Name, "field 'bid' is given twice"));
    CHECK(invalid.has(Category::Type, "required field 'bid' cannot be null"));
    CHECK(invalid.has(Category::Type, "struct construction uses named arguments"));
    CHECK(invalid.has(Category::Type, "abstract struct 'Base' is not constructible"));
}

TEST_CASE("every guide example resolves", "[semantics]") {
    // The examples exercise generics, inject, impl fn, and the intrinsics;
    // the resolver must accept them all (the backend limits what runs).
    const std::vector<std::string_view> names{
        "collection-views", "midpoint", "operators-and-generics", "runtime-choice", "stateful-node", "structural-types",
    };
    for (const std::string_view name : names) {
        const std::string path = std::string{HGL_EXAMPLES_DIR} + "/" + std::string{name} + ".hgl";
        std::ifstream     in{path};
        REQUIRE(in.is_open());
        std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        Resolved    resolved{std::move(text)};
        INFO(path << "\n" << resolved.diagnostics.render(resolved.file));
        CHECK_FALSE(resolved.diagnostics.has_errors());
    }
}
