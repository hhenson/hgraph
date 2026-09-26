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

        Resolved(std::string text, const ModuleCatalog &catalog)
            : file{"test.hgl", std::move(text)}, module{parse(file, diagnostics)},
              result{resolve(file, module, catalog, table_lookup, diagnostics)} {}

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

    /// A module that exports a struct, so a qualified source type has
    /// something to resolve to (ADR 0013).
    ModuleCatalog struct_catalog(std::string support_error = {}) {
        ModuleCatalog    catalog;
        ImportableModule module;
        module.identity = "checks.shapes";
        hgl::semantics::ImportedStruct quote;
        quote.module_identity = module.identity;
        quote.name            = "Quote";
        quote.identity        = "checks.shapes.Quote";
        quote.fields          = {{"bid", hgl::semantics::ImportedScalarType::F64, false, false}};
        quote.support_error   = std::move(support_error);
        hgl::semantics::ImportedStruct base;
        base.module_identity = module.identity;
        base.name            = "Base";
        base.identity        = "checks.shapes.Base";
        base.abstract        = true;
        base.fields          = {{"at", hgl::semantics::ImportedScalarType::I64, false, false}};
        hgl::semantics::ImportedStruct root;
        root.module_identity = module.identity;
        root.name            = "Root";
        root.identity        = "checks.shapes.Root";
        root.abstract        = true;
        root.fields          = {{"id", hgl::semantics::ImportedScalarType::I64, false, false}};
        hgl::semantics::ImportedStruct mid;
        mid.module_identity = module.identity;
        mid.name            = "Mid";
        mid.identity        = "checks.shapes.Mid";
        mid.abstract        = true;
        mid.fields          = {{"seq", hgl::semantics::ImportedScalarType::I64, false, false}};
        hgl::semantics::ImportedType root_ref;
        root_ref.kind             = hgl::semantics::ImportedTypeKind::Symbol;
        root_ref.nominal_identity = "checks.shapes.Root";
        mid.parents               = {root_ref};
        hgl::semantics::ImportedStruct expr;
        expr.module_identity = module.identity;
        expr.name            = "Expr";
        expr.identity        = "checks.shapes.Expr";
        expr.abstract        = true;
        hgl::semantics::ImportedStruct pair;
        pair.module_identity = module.identity;
        pair.name            = "Pair";
        pair.identity        = "checks.shapes.Pair";
        pair.generics        = {{.name = "T", .binding_identity = "checks.shapes.Pair::T"}};
        module.structs       = {std::move(quote), std::move(base), std::move(root),
                                std::move(mid),   std::move(expr), std::move(pair)};
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }

    ModuleCatalog scalar_catalog(std::string support_error = {}) {
        ModuleCatalog    catalog;
        ImportableModule module;
        module.identity = "checks.reader";
        module.functions.push_back(ImportedFunction{.module_identity = module.identity,
                                                    .name            = "blend",
                                                    .identity        = "checks.reader::blend",
                                                    .cpp_symbol      = "checks::reader::blend",
                                                    .parameters      = {{"value", hgl::semantics::ImportedScalarType::F64, false}},
                                                    .result          = hgl::semantics::ImportedScalarType::F64,
                                                    .phases          = {NativeCallPhase::Evaluation},
                                                    .support_error   = std::move(support_error)});
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
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
    CHECK(resolved.binding_of("add")->operator_identity == "hgraph.std.add");
    REQUIRE(resolved.result.aliases.size() == 1);
    CHECK(resolved.result.aliases[0].module == "hgraph.std");
}

TEST_CASE("test contexts share module helpers without exposing them to production", "[resolve][test-context]") {
    SECTION("separate contexts and ordinary tests share forward-declared helpers") {
        Resolved unit{R"(
module t
test { test first { assert eval(helper, value: [1]) == [2] } }
test ordinary { assert eval(helper, value: [2]) == [3] }
test {
    fn helper(value: i64) -> i64 { when { return value + 1 } }
    test second { assert eval(helper, value: [3]) == [4] }
}
)"};
        INFO(unit.diagnostics.render(unit.file));
        CHECK_FALSE(unit.diagnostics.has_errors());
        CHECK(unit.result.tests.size() == 3);
    }
    SECTION("production cannot reference a test helper") {
        Resolved unit{R"(
module t
fn leak(value: i64) -> i64 { helper(value) }
test { fn helper(value: i64) -> i64 { when { return value } } }
)"};
        CHECK(unit.has(Category::Name, "helper"));
    }
    SECTION("helper names are unique across contexts") {
        Resolved unit{R"(
module t
test { fn helper(value: i64) -> i64 => value }
test { fn helper(value: i64) -> i64 => value }
)"};
        CHECK(unit.diagnostics.has_errors());
    }
    SECTION("test helpers cannot be exported") {
        Resolved unit{R"(
module t
test { export fn helper(value: i64) -> i64 => value }
)"};
        CHECK(unit.has(Category::Module, "private fn"));
    }
    SECTION("test helpers cannot register operator implementations") {
        Resolved unit{"module t\noperator op(value: i64) -> i64\ntest { impl fn op(value: i64) -> i64 => value }\n"};
        CHECK(unit.has(Category::Module, "private fn"));
    }
    SECTION("empty contexts introduce no test cases") {
        Resolved unit{"module t\ntest {}\n"};
        CHECK_FALSE(unit.diagnostics.has_errors());
        CHECK(unit.result.tests.empty());
    }
    SECTION("test names are unique across contexts") {
        Resolved unit{"module t\ntest { test same { assert true } }\ntest { test same { assert true } }\n"};
        CHECK(unit.has(Category::Name, "same"));
    }
    SECTION("a context contains declarations rather than assertions") {
        Resolved unit{"module t\ntest { assert true }\n"};
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("external imports require an explicitly supplied module catalog", "[semantics]") {
    const Resolved resolved{"module t\n\nuse market.pricing::{value}\nuse "
                            "hgraph.std::{nothing_like_this}\n"};
    CHECK(resolved.has(Category::Module, "module 'market.pricing' is not available"));
    CHECK(resolved.has(Category::Module, "hgraph.std does not export 'nothing_like_this'"));
}

TEST_CASE("supplied native modules support selective and qualified imports", "[semantics][native]") {
    const ModuleCatalog catalog = scalar_catalog();
    const Resolved      resolved{R"(
module checks.uses_native
use checks.reader::{blend}
use checks.reader as reader

fn one(value: f64) -> f64 => blend(value)
fn two(value: f64) -> f64 => reader::blend(value)
)",
                                 catalog};
    INFO(resolved.diagnostics.render(resolved.file));
    REQUIRE_FALSE(resolved.diagnostics.has_errors());
    REQUIRE(resolved.result.imported_functions.size() == 1U);
    CHECK(resolved.result.imported_functions.front().identity == "checks.reader::blend");
    CHECK(resolved.result.imported_functions.front().cpp_symbol == "checks::reader::blend");
    CHECK(resolved.binding_of("blend")->kind == BindingKind::ImportedFunction);
}

TEST_CASE("unsupported native declarations remain visible at the import site", "[semantics][native]") {
    const ModuleCatalog catalog = scalar_catalog("native scalar calls with declared effects are not supported yet");
    const Resolved      resolved{"module checks.unsupported\nuse checks.reader::{blend}\n", catalog};
    CHECK(resolved.has(Category::Module,
                       "native function 'checks.reader::blend' is unavailable: native scalar calls with declared effects"));
}

TEST_CASE("source native overloads form one local family", "[semantics][native]") {
    const Resolved resolved = resolve_clean(R"(
module checks.source_native

native fn len<T, const size: i64>(value: list<T, size>) -> i64 {
    cpp(const hgraph::TSLInputView &value) { return static_cast<hgraph::Int>(value.size()); }
}

native fn len<T>(value: set<T>) -> i64 {
    cpp(const hgraph::TSSInputView &value) { return static_cast<hgraph::Int>(value.size()); }
}

fn size(value: set<i64>) -> i64 {
    when modified(value) && valid(value) {
        return len(value)
    }
}
)");
    REQUIRE(resolved.result.native_functions.size() == 2U);
    REQUIRE(resolved.result.native_families.size() == 1U);
    CHECK(resolved.result.native_families.front() == resolved.result.native_functions);
    REQUIRE(resolved.binding_of("len") != nullptr);
    CHECK(resolved.binding_of("len")->kind == BindingKind::NativeFunction);
}

TEST_CASE("source native parameters have no HGL defaults", "[semantics][native]") {
    const Resolved resolved{R"(
module checks.native_default
native fn offset(const value: i64 = 1) -> i64 {
    cpp(hgraph::Int value) { return value; }
}

)"};
    CHECK(resolved.has(Category::Type, "a native function parameter cannot have a default value"));
}

TEST_CASE("parameter-pack placement and type-pack use fail closed", "[semantics][parameter-pack]") {
    SECTION("fixed parameter after pack") {
        Resolved resolved{"module packs\noperator bad<T>(values: ...T, tail: T) -> T\n"};
        CHECK(resolved.has(Category::Type, "a fixed parameter cannot follow a parameter pack"));
    }
    SECTION("type pack used as a singular type") {
        Resolved resolved{"module packs\noperator bad<...Ts>(value: Ts) -> i64\n"};
        CHECK(resolved.has(Category::Type, "a type pack is used through a positional"));
    }
    SECTION("named pack needs a type pack") {
        Resolved resolved{"module packs\noperator bad<T>(values: ...{T}) -> i64\n"};
        CHECK(resolved.has(Category::Type, "a named pack uses a heterogeneous type pack"));
    }
    SECTION("source native pack") {
        Resolved resolved{"module packs\nnative fn bad(values: ...i64) -> i64 { cpp() { return 0; } }\n"};
        CHECK(resolved.has(Category::Type, "a source-native function cannot declare a parameter pack"));
    }
    SECTION("runtime function with both pack kinds") {
        Resolved resolved{R"(
module packs
operator bad<...Ts, ...Fields>(values: ...Ts, named: ...{Fields}) -> i64
impl fn bad<...Ts, ...Fields>(values: ...Ts, named: ...{Fields}) -> i64 {
    when { return 0 }
}
)"};
        CHECK(resolved.has(
            Category::Type,
            "a runtime function currently supports one aggregate parameter pack, not both positional and named packs"));
    }
    SECTION("cardinality maximum before minimum") {
        Resolved resolved{"module packs\noperator bad<T>(values: ...T{3:2}) -> T\n"};
        CHECK(resolved.has(Category::Type, "a parameter pack maximum cannot be less than its minimum"));
    }
}

TEST_CASE("source native requirements fail closed at the descriptor boundary", "[semantics][native]") {
    const Resolved resolved{R"(
module checks.native_requirement
native fn numeric<T>(value: T) -> i64
requires T in {i64, f64}
{
    cpp(hgraph::Int value) { return value; }
}
)"};
    CHECK(resolved.has(Category::Type,
                       "a native function cannot have a requires clause until descriptor constraints are importable"));
}

TEST_CASE("reference types are temporal shapes with value-position restrictions", "[semantics][ref]") {
    const Resolved valid = resolve_clean(R"(
module checks.references

fn forward(value: ref<f64>) -> ref<f64> => value
fn select(values: list<ref<f64>, 3>, const index: i64) -> ref<f64> => values[index]
)");
    CHECK_FALSE(valid.diagnostics.has_errors());

    const Resolved key{R"(
module checks.reference_key
fn invalid(value: map<ref<i64>, str>) => value
)"};
    CHECK(key.has(Category::Type, "'ref' is a temporal shape, not a canonical value type"));

    const Resolved mapped{R"(
module checks.reference_value
fn invalid(value: map<i64, ref<str>>) => value
)"};
    CHECK(mapped.has(Category::Type, "map values wrapped in 'ref' require the collection-reference mapping to be resolved"));

    const Resolved nested{R"(
module checks.nested_reference
fn invalid(value: ref<ref<i64>>) => value
)"};
    CHECK(nested.has(Category::Type, "nested 'ref' boundaries are not supported"));
}

TEST_CASE("signal is a lowercase input-only type marker", "[semantics][signal]") {
    const Resolved valid = resolve_clean(R"(
module checks.signals

fn observe(pulse: signal) -> bool {
    when modified(pulse) {
        return valid(pulse)
    }
}
)");
    CHECK_FALSE(valid.diagnostics.has_errors());
    const ast::FunctionDecl &observe = valid.function("observe");
    REQUIRE(observe.signature.parameters.size() == 1U);
    CHECK(valid.module.type(observe.signature.parameters.front().type).kind == ast::TypeKind::Signal);

    const auto rejects = [](std::string source) {
        const Resolved resolved{std::move(source)};
        CHECK(
            resolved.has(Category::Type, "'signal' is an input-only type marker and is only valid as a non-const parameter type"));
    };

    rejects("module checks.signal_result\nfn invalid(value: f64) -> signal => value\n");
    rejects("module checks.signal_const\nfn invalid(const value: signal) => value\n");
    rejects("module checks.signal_field\nstruct Invalid { value: signal }\n");
    rejects("module checks.signal_nested\nfn invalid(value: list<signal>) => value\n");

    const Resolved defaulted{"module checks.signal_default\nfn invalid(value: signal = true) => value\n"};
    CHECK(defaulted.has(Category::Type, "a 'signal' input cannot have a default value"));

    const Resolved uppercase{"module checks.signal_case\nfn invalid(value: SIGNAL) => value\n"};
    CHECK(uppercase.has(Category::Type, "unknown type 'SIGNAL'"));
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
    CHECK(is_intrinsic("insert"));
    CHECK(is_intrinsic("push"));
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

fn iterate(samples: list<f64, 2>) {
    for value in elements(samples) { value }
}
)");
    CHECK(resolved.kind_of("compose") == FunctionKind::Composition);
    CHECK(resolved.kind_of("runtime") == FunctionKind::Runtime);
    CHECK(resolved.kind_of("lifecycle") == FunctionKind::Runtime);
    CHECK(resolved.kind_of("iterate") == FunctionKind::Composition);
    CHECK(resolved.result.functions.size() == 4);
}

TEST_CASE("implementations bind to an operator in scope", "[semantics]") {
    const Resolved bound = resolve_clean(R"(
module t

use hgraph.std::{valid}

impl fn valid(x: f64) -> bool => true
)");
    CHECK(bound.result.functions.size() == 1);
    const Binding &imported = bound.result.implementation_binding(bound.result.functions.front());
    CHECK(imported.kind == BindingKind::Operator);
    CHECK(imported.registry_name == "valid");
    CHECK(imported.operator_identity == "hgraph.std.valid");

    const Resolved local    = resolve_clean(R"(
module t

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
)");
    const Binding &declared = local.result.implementation_binding(local.result.functions.front());
    CHECK(declared.kind == BindingKind::LocalOperator);
    CHECK(declared.operator_identity == "t.choose");

    const Resolved unbound{"module t\n\nimpl fn nothing(x: f64) -> bool => true\n"};
    CHECK(unbound.has(Category::Module, "'impl fn nothing' has no operator named 'nothing' in scope"));

    const Resolved clash{"module t\n\nuse hgraph.std::{valid}\n\nfn valid(x: "
                         "f64) -> bool => true\n"};
    CHECK(clash.has(Category::Name, "'fn valid' conflicts with operator hgraph.std::valid"));
}

TEST_CASE("instantiations bind only to a local operator contract", "[semantics][generics][operators]") {
    const Resolved local = resolve_clean(R"(
module t

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T => value
instantiate choose<i64>, choose<f64>
)");
    REQUIRE(local.result.instantiation_bindings.size() == local.module.decls.size());
    const auto declaration = std::ranges::find_if(local.module.declarations, [&](ast::DeclId id) {
        return std::holds_alternative<ast::InstantiateDecl>(local.module.decl(id).node);
    });
    REQUIRE(declaration != local.module.declarations.end());
    const std::vector<Binding> &bindings = local.result.instantiation_binding(*declaration);
    REQUIRE(bindings.size() == 2);
    CHECK(bindings[0].kind == BindingKind::LocalOperator);
    CHECK(bindings[0].operator_identity == "t.choose");
    CHECK(bindings[1].operator_identity == "t.choose");

    const Resolved imported{R"(
module t

use hgraph.std::{valid}
instantiate valid<f64>
)"};
    CHECK(imported.has(Category::Module, "'instantiate valid<...>' of an imported operator requires external contract metadata"));
}

TEST_CASE("tests are declarations, not values", "[semantics]") {
    const Resolved resolved{R"(
module t

fn f(x: f64) -> f64 => x

test check_f {
    assert eval(f, x: [1.0, _]) == [1.0, _]
    let not_a_value = check_f
}

fn g(x: f64) -> f64 => check_f
)"};
    CHECK(resolved.result.tests.size() == 1);
    CHECK(resolved.has(Category::Name, "'check_f' is a test, not a value"));
    CHECK(resolved.has(Category::Name, "unknown name 'check_f'"));
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
    REQUIRE(resolved.result.structure(future).parents ==
            std::vector<StructSource>{StructSource{.decl = instrument}});
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
        // A self edge is admitted only as an optional atomic boundary (ADR 0012).
        const Resolved recursive{"module t\nstruct Node { next: Node }\n"};
        CHECK(recursive.has(Category::Type, "must be an atomic boundary (ADR 0012, rule 3): declare it 'atomic<Node>'"));
        CHECK(recursive.has(Category::Type, "must be optional (ADR 0012, rule 2): declare it '= null'"));
        const Resolved cycle{"module t\nabstract struct A: B {}\nabstract struct B: A {}\n"};
        CHECK(cycle.has(Category::Type, "struct inheritance cycle reaches"));
    }
    SECTION("multiple parent order fails closed") {
        const Resolved resolved{"module t\nabstract struct A {}\nabstract struct B "
                                "{}\nstruct C: A, B {}\n"};
        CHECK(resolved.has(Category::Type, "awaits the stable field-order rule"));
    }
}

namespace
{
    /// The effective field `field` of struct `name`.
    const StructField &field_of(const Resolved &resolved, std::string_view name, std::string_view field) {
        const auto &fields = resolved.result.structure(resolved.struct_id(name)).fields;
        const auto  found  = std::ranges::find(fields, field, &StructField::name);
        REQUIRE(found != fields.end());
        return *found;
    }
}  // namespace

// ADR 0012: a field through which a value of a struct can contain another value
// of the same struct is a recursive edge, admitted as an optional atomic
// boundary. The resolver finds every edge, by any path, and marks the ones it
// admits; the rest are rejected with the rule they break.
TEST_CASE("recursive struct edges are admitted under ADR 0012", "[semantics][recursive]") {
    SECTION("a direct edge") {
        const Resolved resolved = resolve_clean("module t\nstruct Node {\n value: i64\n next: atomic<Node> = null\n}\n");
        CHECK(field_of(resolved, "Node", "next").recursive);
        CHECK_FALSE(field_of(resolved, "Node", "value").recursive);
    }
    SECTION("a same-module mutual pair") {
        const Resolved resolved = resolve_clean("module t\nstruct A { b: atomic<B> = null }\nstruct B { a: atomic<A> = null }\n");
        CHECK(field_of(resolved, "A", "b").recursive);
        CHECK(field_of(resolved, "B", "a").recursive);
    }
    SECTION("an edge through an abstract parent") {
        const Resolved resolved = resolve_clean("module t\nabstract struct Expr {}\nstruct Lit: Expr { value: i64 }\n"
                                                "struct Add: Expr {\n lhs: atomic<Expr> = null\n rhs: atomic<Expr> = null\n}\n");
        CHECK(field_of(resolved, "Add", "lhs").recursive);
        CHECK(field_of(resolved, "Add", "rhs").recursive);
        CHECK_FALSE(field_of(resolved, "Lit", "value").recursive);
    }
    SECTION("an edge declared on an abstract struct, inherited by its family") {
        const Resolved resolved =
            resolve_clean("module t\nabstract struct Expr { next: atomic<Expr> = null }\nstruct Lit: Expr { value: i64 }\n");
        CHECK(field_of(resolved, "Expr", "next").recursive);
        CHECK(field_of(resolved, "Lit", "next").recursive);
    }
    SECTION("a cycle through another struct and an abstract family") {
        const Resolved resolved = resolve_clean("module t\nabstract struct Expr {}\nstruct Lit: Expr { h: atomic<Holder> = null }\n"
                                                "struct Holder { e: atomic<Expr> = null }\n");
        CHECK(field_of(resolved, "Lit", "h").recursive);
        CHECK(field_of(resolved, "Holder", "e").recursive);
    }
    SECTION("a generic self edge") {
        const Resolved resolved = resolve_clean("module t\nstruct Tree<T> {\n value: T\n next: atomic<Tree<T>> = null\n}\n");
        CHECK(field_of(resolved, "Tree", "next").recursive);
    }
    SECTION("generic cycles that reach finitely many specializations") {
        // Rule 4 as clarified 2026-09-19: whatever hgraph can register is admitted.
        const Resolved mutual = resolve_clean("module t\nstruct Tree<T> { forest: atomic<Forest<T>> = null }\n"
                                              "struct Forest<T> { tree: atomic<Tree<T>> = null }\n");
        CHECK(field_of(mutual, "Tree", "forest").recursive);
        CHECK(field_of(mutual, "Forest", "tree").recursive);
        const Resolved family =
            resolve_clean("module t\nabstract struct Expr<T> {}\nstruct Add<T>: Expr<T> { lhs: atomic<Expr<T>> = null }\n");
        CHECK(field_of(family, "Add", "lhs").recursive);
        const Resolved concrete = resolve_clean("module t\nstruct Node { tree: atomic<Tree<i64>> = null }\n"
                                                "struct Tree<T> { node: atomic<Node> = null }\n");
        CHECK(field_of(concrete, "Node", "tree").recursive);
        CHECK(field_of(concrete, "Tree", "node").recursive);
        const Resolved swapped =
            resolve_clean("module t\nstruct Pair<A, B> {\n first: A\n swapped: atomic<Pair<B, A>> = null\n}\n");
        CHECK(field_of(swapped, "Pair", "swapped").recursive);
    }
    SECTION("an edge through an intermediate generic parent at the same specialization") {
        const Resolved resolved = resolve_clean("module t\nabstract struct Event<T> { payload: T }\n"
                                                "abstract struct Middle<T>: Event<T> {}\n"
                                                "struct IntEvent: Middle<i64> { inner: atomic<Event<i64>> = null }\n");
        CHECK(field_of(resolved, "IntEvent", "inner").recursive);
    }
    SECTION("a field naming a recursive struct from outside its cycle is not an edge") {
        const Resolved resolved =
            resolve_clean("module t\nstruct Node { next: atomic<Node> = null }\nstruct Holder { first: Node }\n");
        CHECK_FALSE(field_of(resolved, "Holder", "first").recursive);
    }
}

TEST_CASE("recursive struct edges that break ADR 0012 are rejected by rule", "[semantics][recursive]") {
    const auto rejected = [](std::string text, std::string_view message) {
        const Resolved resolved{std::move(text)};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK(resolved.has(Category::Type, message));
    };
    SECTION("a required edge (rule 2)") {
        rejected("module t\nstruct Node { next: atomic<Node> }\n",
                 "recursive edge 'next' of 'Node' must be optional (ADR 0012, rule 2): declare it '= null'");
    }
    SECTION("an edge whose inherited null default is replaced (rule 2)") {
        rejected("module t\nabstract struct Expr { next: atomic<Expr> = null }\nstruct Lit: Expr { next = Lit() }\n",
                 "recursive edge 'next' of 'Expr' keeps a null default (ADR 0012, rule 2)");
    }
    SECTION("a non-atomic edge, directly or through a ref (rule 3)") {
        rejected("module t\nstruct Node { next: Node = null }\n",
                 "recursive edge 'next' of 'Node' must be an atomic boundary (ADR 0012, rule 3): declare it 'atomic<Node>'");
        rejected("module t\nabstract struct Expr {}\nstruct Neg: Expr { operand: Expr = null }\n",
                 "recursive edge 'operand' of 'Neg' must be an atomic boundary (ADR 0012, rule 3): declare it 'atomic<Expr>'");
        rejected("module t\nstruct Node { next: ref<Node> }\n",
                 "recursive edge 'next' of 'Node' must be an atomic boundary (ADR 0012, rule 3): declare it 'atomic<Node>'");
        rejected("module t\nabstract struct Event<T> { payload: T }\nabstract struct Middle<T>: Event<T> {}\n"
                 "struct IntEvent: Middle<i64> { inner: Event<i64> = null }\n",
                 "recursive edge 'inner' of 'IntEvent' must be an atomic boundary (ADR 0012, rule 3): declare it "
                 "'atomic<Event<i64>>'");
    }
    SECTION("a generic argument that wraps a parameter (rule 4)") {
        rejected("module t\nstruct Tree<T> { next: atomic<Tree<list<T>>> = null }\n",
                 "recursive edge 'next' of 'Tree' passes 'list<T>' to 'Tree'; in a cycle a generic argument is a parameter "
                 "of 'Tree' or mentions none, since a wrapped parameter denotes an unbounded family of specializations "
                 "(ADR 0012, rule 4)");
        rejected("module t\nstruct Tree<T> { forest: atomic<Forest<list<T>>> = null }\n"
                 "struct Forest<T> { tree: atomic<Tree<T>> = null }\n",
                 "recursive edge 'forest' of 'Tree' passes 'list<T>' to 'Forest'");
        // A constant expression that *does* mention the parameter is still an
        // unbounded family: `Tree<N + 1>` reaches a new specialization each step.
        rejected("module t\nstruct Tree<const N: i64> { next: atomic<Tree<N + 1>> = null }\n",
                 "recursive edge 'next' of 'Tree' passes 'N + 1' to 'Tree'");
    }
    SECTION("a container edge, or an edge through a generic argument (rule 8)") {
        rejected("module t\nstruct Node { children: atomic<list<Node>> = null }\n",
                 "recursive edge 'children' of 'Node' reaches 'Node' again through a collection element; recursion through "
                 "a container or a generic argument is not supported (ADR 0012, rule 8)");
        rejected("module t\nstruct Box<T> { value: T }\nstruct Node { boxed: atomic<Box<Node>> = null }\n",
                 "recursive edge 'boxed' of 'Node' reaches 'Node' again through a generic argument");
    }
    SECTION("a cross-module edge (rule 5)") {
        // A source type may now name another module's struct (ADR 0013), so
        // rule 5 rests on the other half of its reason: module imports are
        // acyclic, so an edge that leaves the module can never lead back and
        // no cycle crosses a boundary. A module absent from the supplied
        // package target is reported as such.
        const Resolved resolved{"module t\nuse other as other\nstruct Node { next: atomic<other::Node> = null }\n"};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK(resolved.has(Category::Module, "module 'other' is not available in the supplied package target"));
        CHECK(resolved.has(Category::Name, "unknown module alias 'other'"));
    }
    SECTION("a cycle that runs through inheritance") {
        // hgraph declares a parent before its children; a parent's field that
        // names its own descendant cannot be registered.
        rejected("module t\nabstract struct Base { child: atomic<Leaf> = null }\nstruct Leaf: Base {}\n",
                 "recursive edge 'child' of 'Base' closes a cycle through inheritance: 'Leaf' inherits from 'Base'");
    }
    SECTION("through an intermediate generic parent that permutes its parameters") {
        // `Swapped<X, Y>` passes its parameters to `Two` reversed, so `Leaf` is
        // a `Two<f64, i64>` and the field leads back to it. The rule 3 section
        // covers the parameter-preserving parent, `Middle<T>: Event<T>`.
        rejected("module t\nabstract struct Two<A, B> { at: i64 }\nabstract struct Swapped<X, Y>: Two<Y, X> {}\n"
                 "struct Leaf: Swapped<i64, f64> { inner: Two<f64, i64> = null }\n",
                 "recursive edge 'inner' of 'Leaf' must be an atomic boundary (ADR 0012, rule 3): declare it "
                 "'atomic<Two<f64, i64>>'");
    }
}

// An importer rebuilds an exported struct from its layout, so everything that
// layout reaches has to be exported too (ADR 0013, "Exports are closed under
// reachability"). The check is on the exporting module, so the error lands on
// whoever broke the contract rather than on a consumer.
TEST_CASE("an exported struct may only reach exported types", "[semantics][export-closure]") {
    const auto rejected = [](std::string text, std::string_view message) {
        const Resolved resolved{std::move(text)};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK(resolved.has(Category::Type, message));
    };
    SECTION("a field naming a module-internal struct") {
        rejected("module t\nstruct Venue { name: str }\nexport struct Quote { venue: Venue }\n",
                 "exported struct 'Quote' reaches module-internal struct 'Venue' through field 'venue'");
    }
    SECTION("through a collection element") {
        rejected("module t\nstruct Leg { size: i64 }\nexport struct Order { legs: list<Leg> }\n",
                 "exported struct 'Order' reaches module-internal struct 'Leg' through field 'legs'");
    }
    SECTION("through a generic argument") {
        rejected("module t\nstruct Key { id: i64 }\nstruct Box<T> { value: T }\n"
                 "export struct Holder { boxed: Box<Key> }\n",
                 "exported struct 'Holder' reaches module-internal struct 'Box' through field 'boxed'");
    }
    SECTION("through a recursive edge (ADR 0012)") {
        rejected("module t\nstruct Node { next: atomic<Node> = null }\n"
                 "export struct Chain { head: atomic<Node> = null }\n",
                 "exported struct 'Chain' reaches module-internal struct 'Node' through field 'head'");
    }
    SECTION("an inherited abstract parent") {
        rejected("module t\nabstract struct Base { at: i64 }\nexport struct Leaf: Base {}\n",
                 "exported struct 'Leaf' inherits module-internal struct 'Base'");
    }
    SECTION("an exported struct reaching exported types is fine") {
        const Resolved resolved =
            resolve_clean("module t\nexport struct Venue { name: str }\n"
                          "export abstract struct Base { at: i64 }\n"
                          "export struct Quote: Base { venue: Venue\n legs: list<Venue> }\n");
        CHECK(resolved.result.structure(resolved.struct_id("Quote")).valid);
    }
    SECTION("an internal struct may reach internal structs freely") {
        // The closure rule applies only from an exported root: a module-internal
        // leaf or chain stays unconstrained.
        const Resolved resolved = resolve_clean("module t\nstruct Venue { name: str }\n"
                                                "struct Quote { venue: Venue }\n"
                                                "struct Book { quote: Quote\n more: list<Quote> }\n");
        CHECK(resolved.result.structure(resolved.struct_id("Book")).valid);
    }
}

// A qualified source type names a struct another module exports (ADR 0013).
// The identity stays the owner's: the importing module binds the name and
// copies nothing into its own namespace.
TEST_CASE("a qualified type resolves to an imported struct", "[semantics][struct-imports]") {
    SECTION("a field may name one") {
        const ModuleCatalog catalog  = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Quote }\n", catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK_FALSE(resolved.diagnostics.has_errors());
        REQUIRE(resolved.result.imported_structs.size() == 1U);
        CHECK(resolved.result.imported_structs.front().identity == "checks.shapes.Quote");
    }
    SECTION("the unqualified use form binds the name") {
        // ADR 0013: a struct imports exactly as a function does, so
        // `use m::{Quote}` must work and not only the alias spelling.
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes::{Quote}\nstruct Book { top: Quote }\n", catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK_FALSE(resolved.diagnostics.has_errors());
        REQUIRE(resolved.result.imported_structs.size() == 1U);
        CHECK(resolved.result.imported_structs.front().identity == "checks.shapes.Quote");
    }
    SECTION("an unqualified imported generic checks its arity too") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes::{Pair}\nstruct Book { top: Pair }\n", catalog};
        CHECK(resolved.has(Category::Type, "imported generic struct 'checks.shapes.Pair' expects 1 arguments, got 0"));
    }
    SECTION("repeated mentions share one binding") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\n"
                                     "struct Book { top: shapes::Quote\n next: shapes::Quote }\n",
                          catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        CHECK_FALSE(resolved.diagnostics.has_errors());
        CHECK(resolved.result.imported_structs.size() == 1U);
    }
    SECTION("an unknown alias is reported") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nstruct Book { top: shapes::Quote }\n", catalog};
        CHECK(resolved.has(Category::Name, "unknown module alias 'shapes'"));
    }
    SECTION("a name the module does not export is reported") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Missing }\n", catalog};
        CHECK(resolved.has(Category::Module, "checks.shapes does not export struct 'Missing'"));
    }
    SECTION("a bare-name argument is resolved, not skipped") {
        // A single identifier lands in GenericArgument::name with neither
        // `type` nor `value` set, so a loop over those two skips it entirely.
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Pair<Typo> }\n",
                          catalog};
        CHECK(resolved.has(Category::Type, "unknown generic argument 'Typo'"));
    }
    SECTION("a value passed to a type generic is reported") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Pair<3> }\n",
                          catalog};
        CHECK(resolved.has(Category::Type, "type generic 'T' takes a type argument"));
    }
    SECTION("a generic arity mismatch is reported") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Pair }\n", catalog};
        CHECK(resolved.has(Category::Type, "imported generic struct 'checks.shapes.Pair' expects 1 arguments, got 0"));
    }
    SECTION("an unavailable struct is reported with its support error") {
        const ModuleCatalog catalog = struct_catalog("field type is not supported by the catalog");
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book { top: shapes::Quote }\n", catalog};
        CHECK(resolved.has(Category::Module, "struct 'checks.shapes.Quote' is unavailable"));
    }
}

// Extending a family a library publishes is why a library is worth having
// (ADR 0013). The imported parent is referenced, never absorbed: this module
// gains no declaration for it, and each inherited field keeps the exporting
// struct as its source rather than becoming an anonymous local copy.
TEST_CASE("a local struct may inherit an imported abstract parent", "[semantics][struct-imports]") {
    SECTION("the parent is referenced and its fields keep their source") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\n"
                                     "struct Tick: shapes::Base { bid: f64 }\n",
                          catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        REQUIRE_FALSE(resolved.diagnostics.has_errors());
        const ast::DeclId  tick = resolved.struct_id("Tick");
        const StructInfo  &info = resolved.result.structure(tick);
        REQUIRE(info.valid);

        // One parent, and it is the imported struct -- not a local declaration.
        REQUIRE(info.parents.size() == 1U);
        CHECK(info.parents.front().is_imported());
        CHECK(info.parents.front().decl == ast::no_node);
        REQUIRE(resolved.result.imported_structs.size() == 1U);
        CHECK(resolved.result.imported_structs.front().identity == "checks.shapes.Base");

        // The inherited field is visible for construction and keeps the
        // exporting struct as its source; its type lives in that module's
        // descriptor, not in this module's AST.
        REQUIRE(info.fields.size() == 2U);
        CHECK(info.fields[0].name == "at");
        CHECK(info.fields[0].origin.is_imported());
        CHECK(info.fields[0].type == ast::no_node);
        CHECK(info.fields[1].name == "bid");
        CHECK_FALSE(info.fields[1].origin.is_imported());
        CHECK(info.fields[1].origin.decl == tick);
    }
    SECTION("the imported parent's own inherited fields are included") {
        // The catalog records only what a struct DECLARES, keeping what it
        // inherits in its parents, so reading `parent.fields` alone loses a
        // grandparent's fields entirely.
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Leaf: shapes::Mid { own: f64 }\n",
                          catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        REQUIRE_FALSE(resolved.diagnostics.has_errors());
        const StructInfo &info = resolved.result.structure(resolved.struct_id("Leaf"));
        REQUIRE(info.fields.size() == 3U);
        // Ancestors first, so a field keeps the position it has in the family.
        CHECK(info.fields[0].name == "id");
        CHECK(info.fields[0].origin.is_imported());
        CHECK(info.fields[1].name == "seq");
        CHECK(info.fields[2].name == "own");
    }
    SECTION("a field naming the imported family reaches this module's members") {
        // Inheriting an imported family makes this struct a member of it, so a
        // field typed by the family can hold this struct: the cycle is local
        // and needs the ADR 0012 atomic boundary.
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\n"
                                     "struct Node: shapes::Expr { next: shapes::Expr }\n",
                          catalog};
        CHECK(resolved.has(Category::Type, "recursive edge 'next' of 'Node' must be an atomic boundary"));
    }
    SECTION("an atomic edge onto the imported family is a recursive edge") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\n"
                                     "struct Node: shapes::Expr { next: atomic<shapes::Expr> = null }\n",
                          catalog};
        INFO(resolved.diagnostics.render(resolved.file));
        REQUIRE_FALSE(resolved.diagnostics.has_errors());
        CHECK(field_of(resolved, "Node", "next").recursive);
    }
    SECTION("a concrete imported struct is not inheritable") {
        const ModuleCatalog catalog = struct_catalog();
        Resolved            resolved{"module t\nuse checks.shapes as shapes\nstruct Book: shapes::Quote {}\n", catalog};
        CHECK(resolved.has(Category::Type,
                           "only an abstract struct may be inherited; 'checks.shapes.Quote' is concrete"));
    }
}

TEST_CASE("struct fields may name other structs that do not lead back", "[semantics]") {
    SECTION("a const generic argument that mentions no parameter (rule 4)") {
        // Rule 4 admits an argument that is a parameter of the declaring struct
        // *or mentions none*. A constant expression names one specialization
        // however it is spelled, so `Tree<1 + 1>` is the `Tree<2>` cycle.
        const Resolved literal = resolve_clean("module t\nstruct Tree<const N: i64> { next: atomic<Tree<2>> = null }\n");
        CHECK(literal.result.structure(literal.struct_id("Tree")).valid);
        const Resolved computed =
            resolve_clean("module t\nstruct Tree<const N: i64> { next: atomic<Tree<1 + 1>> = null }\n");
        CHECK(computed.result.structure(computed.struct_id("Tree")).valid);
        const Resolved negated =
            resolve_clean("module t\nstruct Tree<const N: i64> { next: atomic<Tree<-(-2)>> = null }\n");
        CHECK(negated.result.structure(negated.struct_id("Tree")).valid);
    }
    SECTION("a struct declared later in the module") {
        const Resolved resolved = resolve_clean("module t\nstruct A { b: B = null }\nstruct B { x: i64 }\n");
        CHECK(resolved.result.structure(resolved.struct_id("A")).valid);
    }
    SECTION("an abstract family whose members hold no path back") {
        const Resolved resolved = resolve_clean("module t\nabstract struct Expr {}\nstruct Lit: Expr { value: i64 }\n"
                                                "struct Holder { first: Expr\n rest: list<Expr> = null }\n");
        CHECK(resolved.result.structure(resolved.struct_id("Holder")).valid);
    }
    SECTION("another specialization of a generic family") {
        const Resolved resolved = resolve_clean("module t\nabstract struct Event<T> { payload: T }\n"
                                                "struct IntEvent: Event<i64> { inner: Event<f64> = null }\n"
                                                "struct FloatEvent: Event<f64> {}\n");
        CHECK(resolved.result.structure(resolved.struct_id("IntEvent")).valid);
        CHECK_FALSE(field_of(resolved, "IntEvent", "inner").recursive);
    }
    SECTION("another specialization through an intermediate generic parent") {
        // `Middle<T>` passes its parameter through, so `IntEvent` is an
        // `Event<i64>` only and `Event<f64>` does not lead back to it. The
        // module resolves, and the field is not marked a recursive edge.
        const Resolved nested = resolve_clean("module t\nabstract struct Event<T> { payload: T }\n"
                                              "abstract struct Middle<T>: Event<T> {}\n"
                                              "struct IntEvent: Middle<i64> { inner: Event<f64> = null }\n"
                                              "struct FloatEvent: Middle<f64> {}\n");
        CHECK(nested.result.structure(nested.struct_id("IntEvent")).valid);
        CHECK_FALSE(field_of(nested, "IntEvent", "inner").recursive);
        const Resolved permuted = resolve_clean("module t\nabstract struct Two<A, B> { at: i64 }\n"
                                                "abstract struct Swapped<X, Y>: Two<Y, X> {}\n"
                                                "struct Leaf: Swapped<i64, f64> { inner: Two<i64, f64> = null }\n");
        CHECK(permuted.result.structure(permuted.struct_id("Leaf")).valid);
        CHECK_FALSE(field_of(permuted, "Leaf", "inner").recursive);
    }
}

TEST_CASE("generic structs resolve complete applications and argument roles", "[semantics]") {
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

    const Resolved reference_argument{R"(
module t
struct Box<T> { value: T }
fn bad(x: Box<ref<f64>>) => x
)"};
    CHECK(reference_argument.has(Category::Type, "generic struct type arguments are canonical value types"));
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

TEST_CASE("requires clauses accept parameter-pack reflection intrinsics", "[semantics][parameter-pack]") {
    const Resolved resolved = resolve_clean(R"(
module t

fn positional<...Ts>(values: ...Ts) -> i64
requires len(Ts) == 2 && type_at(Ts, 0) in {i64} && type_at(types(Ts), 1) in {str}
=> 2

fn keyword<...Fields>(values: ...{Fields}) -> i64
requires "price" in keys(Fields) && type_at(Fields, "price") in {f64}
=> 1
)");
    CHECK_FALSE(resolved.result.constraint_bindings.empty());
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

TEST_CASE("only typed var declarations may omit an initializer", "[semantics][locals]") {
    const Resolved valid = resolve_clean(R"(
module t
fn choose(condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        result = value
    } else {
        result = value + 1
    }
    result
}
)");
    CHECK_FALSE(valid.diagnostics.has_errors());

    const Resolved immutable{"module t\nfn invalid() {\n    let value: i64\n}\n"};
    CHECK(immutable.has(Category::Type, "'let' requires an initializer"));

    const Resolved inferred{"module t\nfn invalid() {\n    var value\n}\n"};
    CHECK(inferred.has(Category::Type, "an uninitialized 'var' requires an explicit type"));
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
