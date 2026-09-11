#include "ir/hir_printer.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace
{
    namespace ast = hgl::syntax::ast;
    namespace hir = hgl::ir::hir;

    bool has_operator(std::string_view) {
        // The resolver only needs descriptor membership at this layer. Exact
        // registry typing belongs to HIR type completion.
        return true;
    }

    struct Lowered
    {
        hgl::syntax::SourceFile        file;
        hgl::syntax::DiagnosticSink    diagnostics{};
        ast::Module                    ast{};
        hgl::semantics::ResolvedModule resolved{};
        hir::Module                    hir{};

        explicit Lowered(std::string text, std::string path = "test.hgl")
            : file{std::move(path), std::move(text)}, ast{hgl::syntax::parse(file, diagnostics)} {
            if (diagnostics.has_errors()) { return; }
            resolved = hgl::semantics::resolve(file, ast, has_operator, diagnostics);
            if (!diagnostics.has_errors()) { hir = hgl::ir::lower_to_hir(ast, resolved, diagnostics); }
        }

        Lowered(std::string text, const hgl::semantics::ModuleCatalog &catalog, std::string path = "test.hgl")
            : file{std::move(path), std::move(text)}, ast{hgl::syntax::parse(file, diagnostics)} {
            if (diagnostics.has_errors()) { return; }
            resolved = hgl::semantics::resolve(file, ast, catalog, has_operator, diagnostics);
            if (!diagnostics.has_errors()) { hir = hgl::ir::lower_to_hir(ast, resolved, diagnostics); }
        }

        [[nodiscard]] std::optional<hir::SymbolId> referenced_symbol(std::string_view spelling) const {
            for (ast::ExprId expression = 0; expression < ast.exprs.size(); ++expression) {
                const ast::Expr &source  = ast.expr(expression);
                bool             matches = false;
                if (const auto *name = std::get_if<ast::NameRef>(&source.node)) {
                    matches = name->name.text == spelling;
                } else if (const auto *qualified = std::get_if<ast::QualifiedRef>(&source.node)) {
                    matches = qualified->name.text == spelling;
                }
                if (!matches) { continue; }
                if (const auto *reference = std::get_if<hir::SymbolRef>(&hir.expr(hir::ExprId{expression}).node)) {
                    return reference->symbol;
                }
            }
            return std::nullopt;
        }
    };

    void require_clean(const Lowered &lowered) {
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE_FALSE(lowered.diagnostics.has_errors());
        REQUIRE(lowered.hir.completion == hir::Completion::Resolved);
    }

    bool complete(Lowered &lowered) {
        const hgl::ir::OperatorResolver resolver = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
            hgl::ir::OperatorSelection result;
            result.result          = query.expected_result;
            result.candidate_label = query.identity + "(<typed>)";
            result.deferred        = true;
            return result;
        };
        return hgl::ir::complete_hir(lowered.hir, resolver, lowered.diagnostics);
    }

    hgl::semantics::ModuleCatalog native_catalog(std::vector<hgl::semantics::NativeCallPhase> phases = {
                                                     hgl::semantics::NativeCallPhase::Evaluation}) {
        hgl::semantics::ModuleCatalog    catalog;
        hgl::semantics::ImportableModule module;
        module.identity = "acme.stats";
        module.functions.push_back(hgl::semantics::ImportedFunction{
            .module_identity        = module.identity,
            .name                   = "blend",
            .identity               = "acme.stats::blend",
            .cpp_symbol             = "acme::stats::blend",
            .parameters             = {{"value", hgl::semantics::ImportedScalarType::F64, false},
                                       {"window", hgl::semantics::ImportedScalarType::I64, true}},
            .result                 = hgl::semantics::ImportedScalarType::F64,
            .phases                 = std::move(phases),
            .public_headers         = {"acme/stats.h"},
            .cmake_packages         = {"acme"},
            .imported_targets       = {"acme::stats"},
            .runtime_images         = {"libacme_stats.so"},
            .descriptor_fingerprint = "sha256:test",
        });
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }

    hgl::semantics::ModuleCatalog rolling_native_catalog() {
        using namespace hgl::semantics;
        ImportedType element;
        element.kind             = ImportedTypeKind::Symbol;
        element.binding_identity = "acme.windows::len::T";
        ImportedType rolling;
        rolling.kind     = ImportedTypeKind::Rolling;
        rolling.children = {element};
        rolling.size     = ImportedConstant{.kind = ImportedConstantKind::Parameter, .binding_identity = "acme.windows::len::N"};

        ImportableModule module;
        module.identity = "acme.windows";
        module.functions.push_back(ImportedFunction{
            .module_identity        = module.identity,
            .name                   = "len",
            .identity               = "acme.windows::len",
            .cpp_symbol             = "acme::windows::len",
            .generics               = {{"T", "acme.windows::len::T", false, std::nullopt},
                                       {"N", "acme.windows::len::N", true, ImportedType{ImportedScalarType::I64}}},
            .parameters             = {{"value", rolling, false, NativeParameterAccess::InputView}},
            .result                 = ImportedType{ImportedScalarType::I64},
            .phases                 = {NativeCallPhase::Evaluation},
            .public_headers         = {"acme/windows.h"},
            .cmake_packages         = {"acme"},
            .imported_targets       = {"acme::windows"},
            .runtime_images         = {"libacme_windows.so"},
            .descriptor_fingerprint = "sha256:test",
        });

        ModuleCatalog catalog;
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }
}  // namespace

TEST_CASE("HIR owns backend operator spellings", "[ir][architecture]") {
    static constexpr std::array expected{"*", "/", "//", "%", "+", "-", "<", "<=", ">", ">=", "==", "!=", "&&", "||"};
    static constexpr std::array names{"mul_", "div_", "floordiv_", "mod_", "add_", "sub_", "lt_",
                                      "le_",  "gt_",  "ge_",       "eq_",  "ne_",  "and_", "or_"};
    for (std::size_t index = 0; index < expected.size(); ++index) {
        CHECK(hir::binary_op_spelling(static_cast<hir::BinaryOp>(index)) == expected[index]);
        CHECK(hir::system_operator_name(static_cast<hir::BinaryOp>(index)) == names[index]);
    }
    CHECK(hir::system_operator_name(hir::UnaryOp::Negate) == "neg_");
    CHECK(hir::system_operator_name(hir::UnaryOp::Not) == "not_");
}

TEST_CASE("operator laws bind explicit type domains", "[ir][operators][properties]") {
    Lowered lowered{R"(module checks.properties
operator join_<T>(lhs: T, rhs: T) -> T
properties<str> { associative, identity = "" }
properties<i64> { commutative, identity = 0 }
operator compare_<L, R>(lhs: L, rhs: R) -> bool
properties<i64, i64> { commutative }
)"};
    require_clean(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(complete(lowered));
    const auto &op = std::get<hir::OperatorDecl>(lowered.hir.declarations[1].node);
    REQUIRE(op.properties.size() == 2U);
    REQUIRE(op.properties[0].entries.size() == 2U);
    CHECK(op.properties[0].entries[0].name == "associative");
    CHECK(std::get<std::string>(*lowered.hir.expr(op.properties[0].entries[1].value).constant).empty());
}

TEST_CASE("invalid operator law contracts are rejected", "[ir][operators][properties]") {
    const std::vector<std::pair<std::string, std::string>> cases{
        {"properties<i64> { inverse = 1 }", "unknown operator property"},
        {"properties<i64> { identity }", "identity requires"},
        {"properties<i64> { identity = \"\" }", "operator identity"},
        {"properties<i64> { associative = true }", "flags do not take a value"},
        {"properties<i64> { associative, associative }", "duplicate operator property"},
        {"properties<i64> { identity = 0 }\nproperties<i64> { commutative }", "duplicate properties domain"},
        {"properties<T> { associative }", "concrete value types"},
        {"properties<i64, f64> { associative }", "bind each operator generic"},
        {"properties<i64> { identity = lhs }", "compile-time scalar constant"},
    };
    for (const auto &[clause, diagnostic] : cases) {
        CAPTURE(clause);
        Lowered lowered{"module checks.properties\noperator op<T>(lhs: T, rhs: T) -> T\n" + clause + "\n"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find(diagnostic) != std::string::npos);
    }
}

TEST_CASE("associativity requires closure and the declared constraint domain", "[ir][operators][properties]") {
    Lowered reference{"module checks.properties\noperator op<T>(lhs:T, rhs:T)->T\nproperties<ref<i64>> { associative }\n"};
    CHECK(reference.diagnostics.render(reference.file).find("temporal shape") != std::string::npos);
    Lowered widening{R"(module checks.properties
operator divide_<T>(lhs: T, rhs: T) -> f64
properties<i64> { associative }
)"};
    require_clean(widening);
    CHECK_FALSE(complete(widening));
    CHECK(widening.diagnostics.render(widening.file).find("also require result T") != std::string::npos);
    Lowered constrained{R"(module checks.properties
operator op<T>(lhs: T, rhs: T) -> T requires T in {i64, f64}
properties<str> { associative }
)"};
    require_clean(constrained);
    CHECK_FALSE(complete(constrained));

    Lowered variadic{"module checks.properties\noperator op<T>(lhs:T, rhs:...T)->T\n"
                     "properties<i64> { associative }\n"};
    require_clean(variadic);
    CHECK_FALSE(complete(variadic));
    CHECK(variadic.diagnostics.render(variadic.file).find("binary (T, T) domain") != std::string::npos);
}

TEST_CASE("parameter packs bind homogeneous positional and heterogeneous arguments", "[ir][parameter-pack]") {
    Lowered lowered{"module packs\n"
                    "operator first<T>(values: ...T{2}) -> T\n"
                    "operator positional_count<...Ts>(values: ...Ts{1:*}) -> i64\n"
                    "operator named_count<...Fields>(values: ...{Fields}{1:4}) -> i64\n"
                    "fn same(a: f64, b: f64) -> f64 => first(a, b)\n"
                    "fn mixed(a: f64, b: str) -> i64 => positional_count(a, b)\n"
                    "fn named(a: f64, b: str) -> i64 => named_count(a: a, b: b)\n"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));
    CHECK_FALSE(lowered.diagnostics.has_errors());

    const auto &first = std::get<hir::OperatorDecl>(lowered.hir.declarations[1].node);
    CHECK(first.signature.parameters[0].pack == hir::ParameterPack::Positional);
    CHECK(first.signature.parameters[0].cardinality == hir::PackCardinality{2U, 2U});
    CHECK_FALSE(first.generics[0].is_pack);
    const auto &positional = std::get<hir::OperatorDecl>(lowered.hir.declarations[2].node);
    CHECK(positional.generics[0].is_pack);
    CHECK(positional.signature.parameters[0].cardinality == hir::PackCardinality{1U, std::nullopt});
    const auto &named = std::get<hir::OperatorDecl>(lowered.hir.declarations[3].node);
    CHECK(named.generics[0].is_pack);
    CHECK(named.signature.parameters[0].pack == hir::ParameterPack::Keyword);
    CHECK(named.signature.parameters[0].cardinality == hir::PackCardinality{1U, 4U});
}

TEST_CASE("parameter-pack cardinality rejects invalid calls", "[ir][parameter-pack][cardinality]") {
    SECTION("too few") {
        Lowered lowered{"module packs\noperator pair<T>(values: ...T{2}) -> T\nfn bad(value: f64) -> f64 => pair(value)\n"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("pack 'values' expects exactly 2 argument(s)") != std::string::npos);
    }
    SECTION("too many named") {
        Lowered lowered{"module packs\noperator fields<...Fields>(values: ...{Fields}{1:2}) -> i64\n"
                        "fn bad(a: f64, b: str, c: bool) -> i64 => fields(a: a, b: b, c: c)\n"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("pack 'values' expects 1 to 2 argument(s)") != std::string::npos);
    }
    SECTION("an incompatible forwarded range") {
        Lowered lowered{R"(
module packs
fn target<T>(values: ...T{2:*}) -> i64 => 0
fn bad<T>(values: ...T{1:*}) -> i64 => target(values)
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("pack 'values' expects at least 2 argument(s)") != std::string::npos);
    }
    SECTION("a compatible forwarded range") {
        Lowered lowered{R"(
module packs
fn target<T>(values: ...T{2:*}) -> i64 => 0
fn apply<T>(values: ...T{2:4}) -> i64 => target(values)
)"};
        require_clean(lowered);
        REQUIRE(complete(lowered));
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK_FALSE(lowered.diagnostics.has_errors());
    }
}

TEST_CASE("homogeneous parameter packs reject mixed types", "[ir][parameter-pack]") {
    Lowered lowered{"module packs\n"
                    "operator first<T>(values: ...T) -> T\n"
                    "fn mixed(a: f64, b: str) -> f64 => first(a, b)\n"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("operator argument does not match its contract") != std::string::npos);
}

TEST_CASE("local operator implementations accept homogeneous parameter packs", "[ir][parameter-pack]") {
    Lowered lowered{R"(
module packs

operator all_(values: ...bool) -> bool
impl fn all_(values: ...bool) -> bool => true

fn apply(a: bool, b: bool) -> bool => all_(a, b)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));
    CHECK_FALSE(lowered.diagnostics.has_errors());

    bool selected = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator) { continue; }
        if (!expression.operation.candidate.valid()) { continue; }
        selected = true;
        CHECK(lowered.hir.symbol(expression.operation.candidate).name == "all_");
    }
    CHECK(selected);
}

TEST_CASE("parameter-pack reflection constrains concrete calls", "[ir][parameter-pack][constraints]") {
    Lowered lowered{R"(
module packs.reflection

fn positional<...Ts>(values: ...Ts) -> i64
requires len(Ts) == 2
      && type_at(Ts, 0) in {i64}
      && type_at(types(Ts), 1) in {str}
=> 2

fn keyword<...Fields>(values: ...{Fields}) -> i64
requires "price" in keys(Fields)
      && type_at(Fields, "price") in {f64}
=> 1

fn arity<...Ts, const N: i64>(values: ...Ts) -> i64
requires N == len(Ts)
=> N

operator retain(const value: i64) -> i64
impl fn retain(const value: i64) -> i64 => value

fn checked_arity<...Ts, const N: i64>(values: ...Ts) -> i64
requires N == len(Ts) && retain(N) -> i64
=> N

fn sized<...Ts, const N: i64>(values: ...Ts) -> list<i64, N>
requires N == len(Ts)
{
    inject out
    when {}
}

fn apply(number: i64, text: str, price: f64) -> i64 {
    positional(number, text)
    keyword(price: price, text: text)
    arity(number, text)
    sized(number, text)
    checked_arity(number, text)
}
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    CHECK_FALSE(lowered.diagnostics.has_errors());

    bool inferred_arity = false;
    bool inferred_size  = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.identity == "packs.reflection.sized") {
            const hir::Type &result = lowered.hir.type(expression.type);
            REQUIRE(result.kind == hir::TypeKind::List);
            REQUIRE(result.size.valid());
            REQUIRE(lowered.hir.expr(result.size).constant);
            CHECK(std::get<std::int64_t>(*lowered.hir.expr(result.size).constant) == 2);
            inferred_size = true;
        }
        if (expression.operation.identity != "packs.reflection.arity") { continue; }
        REQUIRE(expression.operation.substitutions.size() == 2U);
        REQUIRE(expression.operation.substitutions[1].constant);
        CHECK(std::get<std::int64_t>(*expression.operation.substitutions[1].constant) == 2);
        REQUIRE(expression.operation.substitutions[1].value.valid());
        CHECK(std::get<std::int64_t>(*lowered.hir.expr(expression.operation.substitutions[1].value).constant) == 2);
        inferred_arity = true;
    }
    CHECK(inferred_arity);
    CHECK(inferred_size);
}

TEST_CASE("parameter-pack reflection rejects non-matching calls", "[ir][parameter-pack][constraints]") {
    SECTION("positional type") {
        Lowered lowered{R"(
module packs.reflection_rejected
fn positional<...Ts>(values: ...Ts) -> i64 requires type_at(Ts, 0) in {i64} => 1
fn bad(value: str) -> i64 => positional(value)
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("requirements are not satisfied") != std::string::npos);
    }
    SECTION("missing keyword") {
        Lowered lowered{R"(
module packs.reflection_rejected
fn keyword<...Fields>(values: ...{Fields}) -> i64 requires "price" in keys(Fields) => 1
fn bad(value: f64) -> i64 => keyword(value: value)
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("requirements are not satisfied") != std::string::npos);
    }
}

TEST_CASE("parameter-pack reflection follows compatible forwarded packs", "[ir][parameter-pack][constraints]") {
    Lowered lowered{R"(
module packs.reflection_forwarding

fn inner<...Us>(values: ...Us) -> i64
requires len(Us) == 2 && type_at(Us, 0) in {i64}
=> 2

fn outer<...Ts>(values: ...Ts{2}) -> i64
requires type_at(Ts, 0) in {i64}
=> inner(values)

fn apply(number: i64, text: str) -> i64 => outer(number, text)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
}

TEST_CASE("parameter-pack each requires every member operation", "[ir][parameter-pack][constraints]") {
    const std::string declarations = R"(
operator format_value<T>(value: T) -> str
impl fn format_value(value: i64) -> str => "i64"
impl fn format_value(value: str) -> str => "str"

fn format_all<...Ts>(values: ...Ts) -> i64
requires each T in types(Ts) {
    format_value(T) -> str
}
=> 1
)";

    SECTION("concrete pack") {
        Lowered lowered{"module packs.each_concrete\n" + declarations + R"(
fn apply(number: i64, text: str) -> i64 => format_all(number, text)
)"};
        require_clean(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(complete(lowered));
    }

    SECTION("empty pack is a vacuous conjunction") {
        Lowered lowered{"module packs.each_empty\n" + declarations + R"(
fn apply() -> i64 => format_all()
)"};
        require_clean(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(complete(lowered));
    }

    SECTION("unsupported member") {
        Lowered lowered{"module packs.each_rejected\n" + declarations + R"(
fn apply(number: i64, price: f64) -> i64 => format_all(number, price)
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("requirements are not satisfied") != std::string::npos);
    }

    SECTION("forwarded premise") {
        Lowered lowered{"module packs.each_forwarded\n" + declarations + R"(
fn forward<...Us>(values: ...Us) -> i64
requires each U in types(Us) {
    format_value(U) -> str
}
=> format_all(values)

fn apply(number: i64, text: str) -> i64 => forward(number, text)
)"};
        require_clean(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(complete(lowered));
    }

    SECTION("source must be a type sequence") {
        Lowered lowered{R"(
module packs.each_invalid_source
fn invalid<...Ts>(values: ...Ts) -> i64
requires each T in len(Ts) {
    T in {i64}
}
=> 1
fn apply(value: i64) -> i64 => invalid(value)
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("each requires a compile-time type sequence") != std::string::npos);
    }
}

TEST_CASE("every guide example lowers to resolved HIR", "[ir][examples]") {
    const std::filesystem::path directory{HGL_EXAMPLES_DIR};
    REQUIRE(std::filesystem::is_directory(directory));

    std::size_t count = 0;
    for (const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator{directory}) {
        if (entry.path().extension() != ".hgl") { continue; }
        ++count;
        std::ifstream input{entry.path()};
        REQUIRE(input.good());
        Lowered lowered{std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}},
                        entry.path().string()};
        INFO(entry.path().filename().string());
        require_clean(lowered);
        CHECK(lowered.hir.declarations.size() == lowered.ast.decls.size());
        CHECK(lowered.hir.stmts.size() == lowered.ast.stmts.size());
        CHECK(lowered.hir.blocks.size() == lowered.ast.blocks.size());
        CHECK(lowered.hir.constraints.size() == lowered.ast.constraints.size());
        CHECK(lowered.hir.exprs.size() >= lowered.ast.exprs.size());

        for (ast::ExprId expression = 0; expression < lowered.ast.exprs.size(); ++expression) {
            const ast::ExprNode &node = lowered.ast.expr(expression).node;
            if (std::holds_alternative<ast::NameRef>(node) || std::holds_alternative<ast::QualifiedRef>(node)) {
                const auto *reference = std::get_if<hir::SymbolRef>(&lowered.hir.expr(hir::ExprId{expression}).node);
                REQUIRE(reference != nullptr);
                CHECK(reference->symbol.valid());
            }
        }
        for (ast::TypeId type = 0; type < lowered.ast.types.size(); ++type) {
            if (lowered.ast.type(type).kind != ast::TypeKind::Named) { continue; }
            const hir::Type &resolved_type = lowered.hir.type(hir::TypeId{type});
            CHECK(resolved_type.kind == hir::TypeKind::Symbol);
            CHECK(resolved_type.symbol.valid());
        }
        for (ast::ConstraintId constraint = 0; constraint < lowered.ast.constraints.size(); ++constraint) {
            const ast::ConstraintNode &source = lowered.ast.constraint(constraint).node;
            const hir::ConstraintNode &target = lowered.hir.constraint(hir::ConstraintId{constraint}).node;
            if (std::holds_alternative<ast::ConstraintName>(source)) {
                const auto *symbol = std::get_if<hir::ConstraintSymbol>(&target);
                REQUIRE(symbol != nullptr);
                CHECK(symbol->symbol.valid());
            } else if (std::holds_alternative<ast::ConstraintCall>(source)) {
                const auto *call = std::get_if<hir::ConstraintCall>(&target);
                REQUIRE(call != nullptr);
                CHECK(call->function.valid());
            } else if (std::holds_alternative<ast::OperatorRequirement>(source)) {
                const auto *requirement = std::get_if<hir::OperatorRequirement>(&target);
                REQUIRE(requirement != nullptr);
                CHECK(requirement->op.valid());
            }
        }
        for (ast::DeclId declaration = 0; declaration < lowered.ast.decls.size(); ++declaration) {
            if (std::holds_alternative<ast::UseDecl>(lowered.ast.decl(declaration).node) ||
                std::holds_alternative<ast::CppIncludeDecl>(lowered.ast.decl(declaration).node) ||
                std::holds_alternative<ast::InstantiateDecl>(lowered.ast.decl(declaration).node)) {
                continue;
            }
            CHECK(lowered.hir.declaration(hir::DeclarationId{declaration}).symbol.valid());
        }
    }
    CHECK(count >= 6);
}

TEST_CASE("HIR string constants stay on one escaped line", "[ir][printer]") {
    Lowered lowered{R"(module checks.strings
fn escaped(const value: str = "a\nb\r\t\"\\") -> str => value
)"};
    require_clean(lowered);

    const std::string printed = hgl::ir::print_hir(lowered.hir);
    CHECK(printed.find(R"(literal "a\nb\r\t\"\\")") != std::string::npos);
}

TEST_CASE("native scalar imports lower to owned HIR and complete exact calls", "[ir][native]") {
    const hgl::semantics::ModuleCatalog catalog = native_catalog();
    Lowered                             lowered{R"(
module checks.native
use acme.stats as stats

fn smooth(value: f64) -> f64 {
    when modified(value) {
        return stats::blend(value, 3)
    }
}
)",
                                                catalog};
    require_clean(lowered);
    REQUIRE(lowered.hir.native_functions.size() == 1U);
    const hir::NativeFunction &native = lowered.hir.native_functions.front();
    CHECK(native.identity == "acme.stats::blend");
    CHECK(native.cpp_symbol == "acme::stats::blend");
    CHECK(native.public_headers == std::vector<std::string>{"acme/stats.h"});
    REQUIRE(native.parameters.size() == 2U);
    CHECK(native.parameters[1].is_const);

    REQUIRE(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));
    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.identity != "acme.stats::blend") { continue; }
        found = true;
        CHECK(expression.operation.kind == hir::OperationKind::ExactFunction);
        CHECK(expression.operation.target == native.symbol);
        CHECK(expression.phase == hir::Phase::Runtime);
    }
    CHECK(found);
}

TEST_CASE("native const generics enforce their declared value type", "[ir][native]") {
    const hgl::semantics::ModuleCatalog catalog = rolling_native_catalog();
    Lowered                             lowered{R"(
module checks.native_const_type
use acme.windows::{len}

fn recent(value: rolling<f64, 5m>) -> i64 {
    when modified(value) && valid(value) { return len(value) }
}
)",
                                                catalog};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("no native overload") != std::string::npos);
}

TEST_CASE("source native overloads retain C++ bodies and select an exact candidate", "[ir][native]") {
    Lowered lowered{R"(
module checks.source_native

cpp include <cstdint>
cpp include "native/helpers.h"
cpp include <cstdint>

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
)"};
    require_clean(lowered);
    CHECK(lowered.hir.cpp_includes == std::vector<std::string>{"<cstdint>", "\"native/helpers.h\""});
    REQUIRE(lowered.hir.native_functions.size() == 2U);
    const hir::NativeFunction &list_len = lowered.hir.native_functions[0];
    const hir::NativeFunction &set_len  = lowered.hir.native_functions[1];
    CHECK(list_len.source_defined);
    CHECK(set_len.source_defined);
    CHECK(list_len.family == set_len.family);
    CHECK(list_len.family != list_len.symbol);
    CHECK(list_len.candidate_identity == "checks.source_native::len#0");
    CHECK(set_len.candidate_identity == "checks.source_native::len#1");
    CHECK(list_len.cpp_parameters == "const hgraph::TSLInputView &value");
    CHECK(set_len.cpp_body.find("value.size()") != std::string::npos);
    REQUIRE(list_len.parameters.size() == 1U);
    CHECK(list_len.parameters.front().access == hir::NativeParameterAccess::InputView);

    REQUIRE(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));
    const auto call = std::ranges::find_if(lowered.hir.exprs, [](const hir::Expr &expression) {
        return expression.operation.identity == "checks.source_native::len";
    });
    REQUIRE(call != lowered.hir.exprs.end());
    CHECK(call->operation.target == set_len.symbol);
    REQUIRE(call->operation.substitutions.size() == 1U);
}

TEST_CASE("native input-view arguments require live runtime inputs", "[ir][native][signal]") {
    Lowered lowered{R"(
module checks.native_input_view

cpp include <hgraph/types/time_series/ts_input/base_view.h>

native fn endpoint_valid(value: signal) -> bool {
    cpp(const hgraph::TSInputView &value) { return value.valid(); }
}

fn invalid(value: f64) -> bool {
    when {
        return endpoint_valid(value + 1.0)
    }
}
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    const std::string diagnostics = lowered.diagnostics.render(lowered.file);
    INFO(diagnostics);
    CHECK(diagnostics.find("native input-view argument requires a live runtime input") != std::string::npos);
}

TEST_CASE("native calls enforce exact scalar and descriptor phase contracts", "[ir][native]") {
    SECTION("no implicit scalar conversion") {
        const hgl::semantics::ModuleCatalog catalog = native_catalog();
        Lowered                             lowered{R"(
module checks.native_type
use acme.stats::{blend}
fn smooth(value: i64) -> f64 {
    when modified(value) { return blend(value, 3) }
}
)",
                                                    catalog};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("expected exactly f64") != std::string::npos);
    }

    SECTION("phase is declared") {
        const hgl::semantics::ModuleCatalog catalog = native_catalog({hgl::semantics::NativeCallPhase::Start});
        Lowered                             lowered{R"(
module checks.native_phase
use acme.stats::{blend}
fn smooth(value: f64) -> f64 {
    when modified(value) { return blend(value, 3) }
}
)",
                                                    catalog};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("not available during evaluation") != std::string::npos);
    }
}

TEST_CASE("every guide example completes typed HIR", "[ir][examples][typed]") {
    const std::filesystem::path directory{HGL_EXAMPLES_DIR};
    REQUIRE(std::filesystem::is_directory(directory));

    for (const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator{directory}) {
        if (entry.path().extension() != ".hgl") { continue; }
        std::ifstream input{entry.path()};
        REQUIRE(input.good());
        Lowered lowered{std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}},
                        entry.path().string()};
        INFO(entry.path().filename().string());
        require_clean(lowered);
        const bool completed = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(completed);
        REQUIRE(lowered.hir.completion == hir::Completion::Typed);
        for (const hir::Expr &expression : lowered.hir.exprs) {
            CHECK(expression.phase != hir::Phase::Unknown);
            CHECK(expression.value_kind != hir::ValueKind::Unknown);
            if (expression.value_kind != hir::ValueKind::Function && expression.value_kind != hir::ValueKind::Operator &&
                expression.value_kind != hir::ValueKind::Type) {
                CHECK(expression.type.valid());
            }
        }
    }
}

TEST_CASE("typed HIR canonicalizes types and resolves exact calls", "[ir][typed][calls]") {
    Lowered lowered{R"(
module checks.calls

fn identity<T>(value: T) -> T => value

fn apply_it(value: f64) -> f64 => identity(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    const hir::FunctionDecl *use = nullptr;
    for (const hir::Declaration &declaration : lowered.hir.declarations) {
        const auto *fn = std::get_if<hir::FunctionDecl>(&declaration.node);
        if (fn && declaration.symbol.valid() && lowered.hir.symbol(declaration.symbol).name == "apply_it") { use = fn; }
    }
    REQUIRE(use != nullptr);
    const hir::Expr &call = lowered.hir.expr(use->concise_body);
    CHECK(call.operation.kind == hir::OperationKind::ExactFunction);
    CHECK(call.operation.target.valid());
    REQUIRE(call.operation.substitutions.size() == 1);
    CHECK(call.operation.substitutions.front().type.valid());
    CHECK(call.type == use->signature.result);
    CHECK(call.phase == hir::Phase::Wiring);
    CHECK(hir::has_effect(call.effects, hir::Effect::WireGraph));

    const auto *call_node = std::get_if<hir::Call>(&call.node);
    REQUIRE(call_node != nullptr);
    const hir::Expr &argument = lowered.hir.expr(call_node->arguments.front().value);
    CHECK(argument.type == call.type);
}

TEST_CASE("typed HIR rejects incompatible block tails", "[ir][typed][results]") {
    Lowered lowered{R"(
module checks.block_result

fn wrong() -> str {
    1
}
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("function result has type i64, expected str") != std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR gives a consumed temporal if without else its true-branch type", "[ir][typed][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_omitted_else

fn choose(condition: bool, value: i64) -> i64 {
    if condition {
        value + 1
    }
}
)"};
    require_clean(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(complete(lowered));

    const hir::FunctionDecl *fn = nullptr;
    for (const hir::Declaration &declaration : lowered.hir.declarations) {
        const auto *candidate = std::get_if<hir::FunctionDecl>(&declaration.node);
        if (candidate && declaration.symbol.valid() && lowered.hir.symbol(declaration.symbol).name == "choose") { fn = candidate; }
    }
    REQUIRE(fn != nullptr);
    const hir::Expr &conditional = lowered.hir.expr(lowered.hir.block(fn->block_body).tail);
    CHECK(conditional.type == fn->signature.result);
    CHECK(conditional.phase == hir::Phase::Wiring);
    CHECK(conditional.value_kind == hir::ValueKind::Signal);
}

TEST_CASE("typed HIR checks definite assignment across conditional paths", "[ir][typed][locals][control-flow]") {
    SECTION("both reaching branches assign the variable") {
        Lowered lowered{R"(
module checks.assigned

fn choose(condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        result = value
    } else {
        result = value + 1
    }
    result
}
)"};
        require_clean(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK(complete(lowered));
    }

    SECTION("a reaching unassigned path is rejected") {
        Lowered lowered{R"(
module checks.unassigned

fn choose(condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        result = value
    }
    result
}
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("'result' may be used before it is assigned") != std::string::npos);
    }

    SECTION("a branch that returns does not reach the later use") {
        Lowered lowered{R"(
module checks.returned

fn choose(condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        return value
    } else {
        result = value + 1
    }
    result
}
)"};
        require_clean(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK(complete(lowered));
    }

    SECTION("compound assignment reads the prior value") {
        Lowered lowered{R"(
module checks.compound

fn invalid(value: i64) -> i64 {
    var result: i64
    result += value
    result
}
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("'result' may be used before it is assigned") != std::string::npos);
    }
}

TEST_CASE("typed HIR enforces const arguments at exact calls", "[ir][typed][calls][phase]") {
    Lowered lowered{R"(
module checks.const_call

fn configured(const value: f64) -> f64 => value
fn wrong(value: f64) -> f64 => configured(value)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("a const parameter requires a compile-time value") != std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR infers generic constructors from fields", "[ir][typed][structs][generics]") {
    Lowered lowered{R"(
module checks.constructor_inference

struct Box<T> {
    value: T
}

struct Vector<T, const N: i64> {
    values: list<T, N>
}

fn unbox(value: f64) -> f64 {
    let boxed = Box(value: value)
    boxed.value
}

fn unvector(values: list<f64, 3>) -> list<f64, 3> {
    let vector = Vector(values: values)
    vector.values
}
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::Constructor) { continue; }
        const hir::Type &type = lowered.hir.type(expression.type);
        if (type.kind != hir::TypeKind::Symbol || type.arguments.empty()) { continue; }
        found = true;
        REQUIRE(type.arguments.front().kind == hir::TypeArgumentKind::Type);
        CHECK(lowered.hir.type(type.arguments.front().type).scalar == hir::ScalarType::F64);
    }
    CHECK(found);
}

TEST_CASE("typed HIR substitutes generic struct fields", "[ir][typed][structs][generics]") {
    Lowered lowered{R"(
module checks.generic_field

struct Box<T> {
    value: T
}

abstract struct Pair<A, B> {
    first: A
    second: B
}

struct Swap<X, Y>: Pair<Y, X> {}

fn unwrap(box: Box<f64>) -> f64 => box.value

fn swapped(first: f64, second: i64) -> Swap<i64, f64> =>
    Swap<i64, f64>(first: first, second: second)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("typed HIR infers remapped inherited generic struct fields", "[ir][typed][structs][generics]") {
    Lowered lowered{R"(
module checks.remapped_constructor

abstract struct Pair<A, B> {
    first: A
    second: B
}

struct Swap<X, Y>: Pair<Y, X> {}

fn swapped(first: f64, second: i64) -> Swap<i64, f64> {
    let value = Swap(first: first, second: second)
    value
}
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("typed HIR completes constant expressions used by types", "[ir][typed][types][const]") {
    Lowered lowered{R"(
module checks.type_constants

fn fixed(values: list<f64, 1 + 2>) -> list<f64, 3> => values
fn generic<const N: i64>(values: list<f64, N + 1>) -> f64 => 1.0
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("typed HIR diagnoses temporal constant overflow", "[ir][typed][const][temporal]") {
    Lowered lowered{R"(
module checks.temporal_overflow

fn overflow() -> duration => 106751991d4h54s775ms807us + 1us
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("overflow in a temporal constant expression") != std::string::npos);
}

TEST_CASE("typed HIR folds integer constants without floating-point loss", "[ir][typed][const][integer]") {
    Lowered lowered{R"(
module checks.integer_constants

fn exact() -> i64 => 9007199254740993 + 0
fn ordered() -> bool => 9007199254740993 > 9007199254740992
fn distinct() -> bool => 9007199254740993 != 9007199254740992
fn floor_negative() -> i64 => -7 // 3
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool exact    = false;
    bool ordered  = false;
    bool distinct = false;
    bool floored  = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        const auto *binary = std::get_if<hir::Binary>(&expression.node);
        if (binary == nullptr || !expression.constant) { continue; }
        if (binary->op == hir::BinaryOp::Add) {
            const auto *value = std::get_if<std::int64_t>(&*expression.constant);
            exact             = value != nullptr && *value == 9'007'199'254'740'993;
        } else if (binary->op == hir::BinaryOp::Greater) {
            const auto *value = std::get_if<bool>(&*expression.constant);
            ordered           = value != nullptr && *value;
        } else if (binary->op == hir::BinaryOp::NotEqual) {
            const auto *value = std::get_if<bool>(&*expression.constant);
            distinct          = value != nullptr && *value;
        } else if (binary->op == hir::BinaryOp::FloorDiv) {
            const auto *value = std::get_if<std::int64_t>(&*expression.constant);
            floored           = value != nullptr && *value == -3;
        }
    }
    CHECK(exact);
    CHECK(ordered);
    CHECK(distinct);
    CHECK(floored);
}

TEST_CASE("typed HIR folds floating modulo without forming a quotient", "[ir][typed][const][float]") {
    // Include an infinite divisor, overflowing finite quotient, underflowing
    // quotient and both signs of zero. The folded result must match runtime %.
    for (const auto &[source, expected] : std::vector<std::pair<std::string, double>>{{"1.0 % (1e308 * 2.0)", 1.0},
                                                                                      {"-1.0 % (1e308 * 2.0)", INFINITY},
                                                                                      {"1.0 % (-1e308 * 2.0)", -INFINITY},
                                                                                      {"1e308 % 1e-308", std::fmod(1e308, 1e-308)},
                                                                                      {"-1e-308 % 1e308", 1e308},
                                                                                      {"0.0 % -2.0", -0.0},
                                                                                      {"-0.0 % 2.0", 0.0},
                                                                                      {"4.0 % -2.0", -0.0},
                                                                                      {"-4.0 % 2.0", 0.0}}) {
        Lowered lowered{"module checks.modulo\nfn value() -> f64 => " + source + "\n"};
        require_clean(lowered);
        INFO(source);
        REQUIRE(complete(lowered));
        bool found = false;
        for (const hir::Expr &expression : lowered.hir.exprs) {
            const auto *binary = std::get_if<hir::Binary>(&expression.node);
            if (binary == nullptr || binary->op != hir::BinaryOp::Rem) { continue; }
            REQUIRE(expression.constant);
            const double actual = std::get<double>(*expression.constant);
            CHECK(actual == expected);
            CHECK(std::signbit(actual) == std::signbit(expected));
            found = true;
        }
        REQUIRE(found);
    }
}

TEST_CASE("typed HIR diagnoses integer constant overflow", "[ir][typed][const][integer]") {
    Lowered lowered{R"(
module checks.integer_overflow

fn overflow() -> i64 => 9223372036854775807 + 1
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("overflow in an integer constant expression") != std::string::npos);
}

TEST_CASE("typed HIR does not implicitly widen time-series collection elements", "[ir][typed][types][collection]") {
    Lowered lowered{R"(
module checks.collection_widening

fn widen(xs: list<i64>) -> list<f64> => xs
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file)
              .find("function result requires an implicit conversion inside a time-series collection") != std::string::npos);
}

TEST_CASE("typed HIR preserves fixed list sizes during assignment", "[ir][typed][types][list]") {
    Lowered valid{R"(
module checks.fixed_list_assignment

fn pair() -> list<f64, 2> => [1.0, 2.0]
fn dynamic() -> list<f64> => pair()
)"};
    require_clean(valid);
    REQUIRE(complete(valid));

    Lowered wrong_literal{R"(
module checks.fixed_list_literal

fn triple() -> list<f64, 3> => [1.0, 2.0]
)"};
    require_clean(wrong_literal);
    CHECK_FALSE(complete(wrong_literal));
    CHECK(wrong_literal.diagnostics.render(wrong_literal.file).find("list literal has 2 elements, expected 3") !=
          std::string::npos);

    Lowered wrong_result{R"(
module checks.fixed_list_result

fn pair() -> list<f64, 2> => [1.0, 2.0]
fn triple() -> list<f64, 3> => pair()
)"};
    require_clean(wrong_result);
    CHECK_FALSE(complete(wrong_result));
    CHECK(wrong_result.diagnostics.render(wrong_result.file).find("function result has type list, expected list") !=
          std::string::npos);
}

TEST_CASE("typed HIR selects a source operator implementation", "[ir][typed][operators]") {
    Lowered lowered{R"(
module checks.operators

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
fn apply_it(value: f64) -> f64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool implementation_found = false;
    for (const hir::Declaration &declaration : lowered.hir.declarations) {
        const auto *implementation = std::get_if<hir::FunctionDecl>(&declaration.node);
        if (implementation == nullptr || implementation->visibility != hir::Visibility::Implementation) { continue; }
        implementation_found = true;
        REQUIRE(implementation->operator_contract.valid());
        const hir::Symbol &contract = lowered.hir.symbol(implementation->operator_contract);
        CHECK(contract.kind == hir::SymbolKind::Operator);
        CHECK(contract.canonical_name == "checks.operators.choose");
    }
    CHECK(implementation_found);

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator || !expression.operation.target.valid() ||
            lowered.hir.symbol(expression.operation.target).name != "choose") {
            continue;
        }
        found = true;
        CHECK(expression.operation.candidate.valid());
        CHECK(lowered.hir.symbol(expression.operation.candidate).name == "choose");
        CHECK_FALSE(expression.operation.deferred);
    }
    CHECK(found);
}

TEST_CASE("HIR preserves an imported implementation's nominal operator identity", "[ir][operators][imports]") {
    Lowered lowered{R"(
module checks.imported_impl

use hgraph.std::{valid}

impl fn valid(value: f64) -> bool => true
)"};
    require_clean(lowered);

    const hir::FunctionDecl *implementation = nullptr;
    for (const hir::Declaration &declaration : lowered.hir.declarations) {
        const auto *candidate = std::get_if<hir::FunctionDecl>(&declaration.node);
        if (candidate != nullptr && candidate->visibility == hir::Visibility::Implementation) { implementation = candidate; }
    }
    REQUIRE(implementation != nullptr);
    REQUIRE(implementation->operator_contract.valid());
    const hir::Symbol &contract = lowered.hir.symbol(implementation->operator_contract);
    CHECK(contract.kind == hir::SymbolKind::ImportedOperator);
    CHECK(contract.name == "valid");
    CHECK(contract.external_name == "valid");
    CHECK(contract.canonical_name == "hgraph.std.valid");
}

TEST_CASE("typed HIR defers source overload ranking to hgraph", "[ir][typed][operators]") {
    Lowered lowered{R"(
module checks.overloads

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
impl fn choose(value: i64) -> i64 => value
fn apply_it(value: f64) -> f64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator || !expression.operation.target.valid() ||
            lowered.hir.symbol(expression.operation.target).name != "choose") {
            continue;
        }
        found = true;
        CHECK_FALSE(expression.operation.candidate.valid());
        CHECK(expression.operation.deferred);
    }
    CHECK(found);
}

TEST_CASE("typed HIR does not select an inapplicable sole source implementation", "[ir][typed][operators]") {
    Lowered lowered{R"(
module checks.inapplicable

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
fn apply_it(value: i64) -> i64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator || !expression.operation.target.valid() ||
            lowered.hir.symbol(expression.operation.target).name != "choose") {
            continue;
        }
        found = true;
        CHECK_FALSE(expression.operation.candidate.valid());
        CHECK(expression.operation.deferred);
    }
    CHECK(found);
}

TEST_CASE("typed HIR rejects an uninferred generic substitution", "[ir][typed][generics]") {
    Lowered lowered{R"(
module checks.uninferred

operator make<T>() -> T
fn wrong() -> f64 {
    make()
    1.0
}
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("cannot infer generic 'T' for operator call") != std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR unifies structured generics through reference boundaries", "[ir][typed][generics][ref]") {
    Lowered lowered{R"(
module checks.reference_substitution

fn route3<T>(values: list<ref<T>, 3>, const index: i64) -> ref<T> => values[index]
fn select_plain(values: list<f64, 3>) -> ref<f64> => route3(values, 0)
)"};
    require_clean(lowered);
    CHECK(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));
    CHECK_FALSE(lowered.diagnostics.has_errors());
}

TEST_CASE("typed HIR rejects nested references formed by generic substitution", "[ir][typed][generics][ref]") {
    Lowered lowered{R"(
module checks.nested_reference_substitution

fn wrap<T>(value: T) -> ref<T> => value
fn invalid(value: ref<f64>) -> ref<f64> => wrap(value)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("generic substitution produces an unsupported reference shape") !=
          std::string::npos);
}

TEST_CASE("typed HIR admits and rejects closed callable requirements", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.constraints

fn add_numeric<T>(value: T) -> T
requires T in {i64, f64}
=> value + value

fn accepted(value: i64) -> i64 => add_numeric(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
    CHECK(lowered.hir.completion == hir::Completion::Typed);

    Lowered rejected{R"(
module checks.rejected_constraint

fn add_numeric<T>(value: T) -> T
requires T in {i64, f64}
=> value + value

fn rejected(value: str) -> str => add_numeric(value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("requirements are not satisfied") != std::string::npos);
    CHECK(rejected.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR equality constraints infer reflected field types", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.reflected_constraint

abstract struct Priced {
    bid: f64
}

struct Quote: Priced {
    size: i64
}

fn reflected<U, V>(value: U, const name: str) -> V
requires U is struct
      && name in fields(U)
      && V == field_type(U, name)
=> null

fn apply_reflected(value: Quote) -> i64 {
    let result = reflected(value, "size")
    result
}

fn read_bid<U>(value: U) -> f64
requires U is struct
      && has_fields(U, {"bid"})
      && field_type(U, "bid") == f64
=> value.bid

fn apply_read_bid(value: Quote) -> f64 => read_bid(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::ExactFunction || !expression.operation.target.valid() ||
            lowered.hir.symbol(expression.operation.target).name != "reflected") {
            continue;
        }
        found = true;
        CHECK(expression.type.valid());
        CHECK(lowered.hir.type(expression.type).kind == hir::TypeKind::Scalar);
        CHECK(lowered.hir.type(expression.type).scalar == hir::ScalarType::I64);
        REQUIRE(expression.operation.substitutions.size() == 2U);
        CHECK(expression.operation.substitutions[1].type == expression.type);
    }
    CHECK(found);
}

TEST_CASE("typed HIR operator requirements admit generic body operations", "[ir][typed][constraints][operators]") {
    Lowered lowered{R"(
module checks.operator_constraint

operator add<T>(lhs: T, rhs: T) -> T
impl fn add(lhs: f64, rhs: f64) -> f64 => lhs + rhs

fn double<T>(value: T) -> T
requires add(T, T) -> T
=> add(value, value)

fn apply_double(value: f64) -> f64 => double(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);

    Lowered rejected{R"(
module checks.operator_constraint_rejected

operator add<T>(lhs: T, rhs: T) -> T
impl fn add(lhs: f64, rhs: f64) -> f64 => lhs + rhs

fn double<T>(value: T) -> T
requires add(T, T) -> T
=> add(value, value)

fn apply_double(value: i64) -> i64 => double(value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("operator requirement has no implementation") != std::string::npos);
    CHECK(rejected.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("generic symbols require their system operator contract", "[ir][typed][operators]") {
    Lowered native{R"(module checks.system_requirement
use hgraph.std::{add_}
fn double<T>(value: T) -> T requires add_(T, T) -> T => value + value
)"};
    require_clean(native);
    REQUIRE(complete(native));
    Lowered local{R"(module checks.local_requirement
operator add_<T>(lhs: T, rhs: T) -> T
fn double<T>(value: T) -> T requires add_(T, T) -> T => value + value
)"};
    require_clean(local);
    CHECK_FALSE(complete(local));
}

TEST_CASE("implementation requirements provide body premises without constraining the operator",
          "[ir][typed][constraints][operators]") {
    Lowered lowered{R"(
module checks.implementation_constraint

operator add<T>(lhs: T, rhs: T) -> T
impl fn add(lhs: f64, rhs: f64) -> f64 => lhs + rhs

operator double<T>(value: T) -> T

impl fn double(value: f64) -> f64
requires add(f64, f64) -> f64
=> value + value
fn apply_double(value: f64) -> f64 => double(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("operator requirements apply the target contract constraints", "[ir][typed][constraints][operators]") {
    Lowered rejected{R"(
module checks.effective_operator_constraint

operator choose<T>(value: T) -> T
requires T in {f64}

impl fn choose(value: i64) -> i64 => value

fn requires_choose<T>(value: T) -> T
requires choose(T) -> T
=> value

fn apply(value: i64) -> i64 => requires_choose(value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("function call requirements are not satisfied") != std::string::npos);
}

TEST_CASE("operator requirements carry const arguments", "[ir][typed][constraints][operators]") {
    Lowered lowered{R"(
module checks.const_operator_requirement

operator retain(const value: i64) -> i64
impl fn retain(const value: i64) -> i64 => value

fn accepts(const value: i64) -> i64
requires retain(value) -> i64
=> value

fn apply() -> i64 => accepts(3)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("operator implementations must conform to their contract", "[ir][typed][constraints][operators]") {
    Lowered wrong_name{R"(
module checks.operator_parameter_name

operator choose<T>(value: T) -> T
impl fn choose(other: f64) -> f64 => other
)"};
    require_clean(wrong_name);
    CHECK_FALSE(complete(wrong_name));
    CHECK(wrong_name.diagnostics.render(wrong_name.file).find("implementation signature does not conform") != std::string::npos);

    Lowered wrong_result{R"(
module checks.operator_result_type

operator choose<T>(value: T) -> T
impl fn choose(value: i64) -> f64 => 1.0
)"};
    require_clean(wrong_result);
    CHECK_FALSE(complete(wrong_result));
    CHECK(wrong_result.diagnostics.render(wrong_result.file).find("implementation signature does not conform") !=
          std::string::npos);
}

TEST_CASE("source candidates with unresolved generics remain deferred", "[ir][typed][constraints][operators]") {
    Lowered lowered{R"(
module checks.unresolved_candidate_generic

operator choose<T>(value: T) -> T
impl fn choose<T, U>(value: T) -> T => value
fn apply_it(value: f64) -> f64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool found = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator || !expression.operation.target.valid() ||
            lowered.hir.symbol(expression.operation.target).name != "choose") {
            continue;
        }
        found = true;
        CHECK_FALSE(expression.operation.candidate.valid());
        CHECK(expression.operation.deferred);
    }
    CHECK(found);
}

TEST_CASE("an unmaterialized generic implementation is not a concrete source candidate", "[ir][typed][generics][operators]") {
    Lowered lowered{R"(
module checks.unmaterialized_candidate

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T => value
fn apply_it(value: f64) -> f64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    const auto call = std::ranges::find_if(lowered.hir.exprs, [&](const hir::Expr &expression) {
        return expression.operation.kind == hir::OperationKind::NominalOperator && expression.operation.target.valid() &&
               lowered.hir.symbol(expression.operation.target).name == "choose";
    });
    REQUIRE(call != lowered.hir.exprs.end());
    CHECK_FALSE(call->operation.candidate.valid());
    CHECK(call->operation.deferred);
}

TEST_CASE("typed HIR materializes constrained generic operator implementations", "[ir][typed][generics][operators]") {
    Lowered lowered{R"(
module checks.materializations

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T
requires T in {i64, f64}
=> value

instantiate choose<i64>, choose<f64>

fn choose_i64(value: i64) -> i64 => choose(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    const auto declaration = std::ranges::find_if(lowered.hir.declarations, [](const hir::Declaration &candidate) {
        return std::holds_alternative<hir::InstantiateDecl>(candidate.node);
    });
    REQUIRE(declaration != lowered.hir.declarations.end());
    const auto &instantiate = std::get<hir::InstantiateDecl>(declaration->node);
    REQUIRE(instantiate.entries.size() == 2);
    for (const hir::Instantiation &entry : instantiate.entries) {
        REQUIRE(entry.materializations.size() == 1);
        REQUIRE(entry.materializations.front().substitutions.size() == 1);
        CHECK(entry.materializations.front().substitutions.front().type.valid());
    }

    const auto call = std::ranges::find_if(lowered.hir.exprs, [&](const hir::Expr &expression) {
        return expression.operation.kind == hir::OperationKind::NominalOperator && expression.operation.target.valid() &&
               lowered.hir.symbol(expression.operation.target).name == "choose";
    });
    REQUIRE(call != lowered.hir.exprs.end());
    CHECK(call->operation.candidate.valid());
    CHECK_FALSE(call->operation.deferred);
}

TEST_CASE("typed HIR retains selected implementation generics in a materialized candidate", "[ir][typed][generics][operators]") {
    Lowered lowered{R"(
module checks.partial_materialization

operator preserve<T, const N: i64>(value: list<T, N>) -> list<T, N>
impl fn preserve<T, const N: i64>(value: list<T, N>) -> list<T, N> => value

instantiate preserve<i64, _>

fn preserve_three(value: list<i64, 3>) -> list<i64, 3> => preserve(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);

    const auto declaration = std::ranges::find_if(lowered.hir.declarations, [](const hir::Declaration &candidate) {
        return std::holds_alternative<hir::InstantiateDecl>(candidate.node);
    });
    REQUIRE(declaration != lowered.hir.declarations.end());
    const auto &entry = std::get<hir::InstantiateDecl>(declaration->node).entries.front();
    REQUIRE(entry.arguments.size() == 2);
    CHECK_FALSE(entry.arguments[0].retained);
    CHECK(entry.arguments[1].retained);
    REQUIRE(entry.materializations.size() == 1);
    REQUIRE(entry.materializations.front().substitutions.size() == 2);
    CHECK(entry.materializations.front().substitutions[0].type.valid());
    CHECK(entry.materializations.front().substitutions[1].retained);

    const auto call = std::ranges::find_if(lowered.hir.exprs, [&](const hir::Expr &expression) {
        return expression.operation.kind == hir::OperationKind::NominalOperator && expression.operation.target.valid() &&
               lowered.hir.symbol(expression.operation.target).name == "preserve";
    });
    REQUIRE(call != lowered.hir.exprs.end());
    CHECK(call->operation.candidate.valid());
    CHECK_FALSE(call->operation.deferred);
}

TEST_CASE("typed HIR rejects invalid and duplicate operator materializations", "[ir][typed][generics][operators]") {
    Lowered unsupported{R"(
module checks.materialization_constraint

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T
requires T in {i64, f64}
=> value

instantiate choose<str>
)"};
    require_clean(unsupported);
    CHECK_FALSE(complete(unsupported));
    CHECK(unsupported.diagnostics.render(unsupported.file).find("instantiate matches no local generic operator implementation") !=
          std::string::npos);

    Lowered duplicate{R"(
module checks.duplicate_materialization

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T => value

instantiate choose<i64>, choose<i64>
)"};
    require_clean(duplicate);
    CHECK_FALSE(complete(duplicate));
    const std::string diagnostics = duplicate.diagnostics.render(duplicate.file);
    CHECK(diagnostics.find("operator implementation is instantiated more than once with the same arguments") != std::string::npos);
    CHECK(diagnostics.find("matches no local generic operator implementation") == std::string::npos);

    Lowered constrained_retained{R"(
module checks.constrained_retained_materialization

operator sized<const N: i64>(value: list<i64, N>) -> list<i64, N>
impl fn sized<const N: i64>(value: list<i64, N>) -> list<i64, N>
requires N == 3
=> value

instantiate sized<_>
)"};
    require_clean(constrained_retained);
    CHECK_FALSE(complete(constrained_retained));
    CHECK(constrained_retained.diagnostics.render(constrained_retained.file)
              .find("instantiate matches no local generic operator implementation") != std::string::npos);
}

TEST_CASE("constraint logic admits resolved alternatives without inferring through them", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.constraint_logic

fn scalar_value<T>(value: T) -> T
requires T in {i64} || T in {str}
=> value

fn not_text<T>(value: T) -> T
requires !(T in {str})
=> value

fn apply_scalar(value: str) -> str => scalar_value(value)
fn apply_not(value: i64) -> i64 => not_text(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
}

TEST_CASE("unresolved constraint dependencies fail closed", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.unresolved_constraint

fn unresolved<U, V>(value: U) -> V
requires V == field_type(U, "missing")
=> null

fn apply_unresolved(value: i64) -> i64 {
    unresolved(value)
    1
}
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("requirements could not be resolved") != std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("generic requirements are premises for nested constrained calls", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.nested_constraint_premise

fn inner<T>(value: T) -> T
requires T in {i64, f64}
=> value

fn outer<U>(value: U) -> U
requires U in {i64, f64}
=> inner(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    Lowered rejected{R"(
module checks.insufficient_constraint_premise

fn inner<T>(value: T) -> T
requires T in {i64}
=> value

fn outer<U>(value: U) -> U
requires U in {i64, f64}
=> inner(value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("function call requirements are not satisfied") != std::string::npos);
}

TEST_CASE("generic struct construction evaluates structural requirements", "[ir][typed][constraints][structs]") {
    Lowered lowered{R"(
module checks.struct_constraint

struct WithId<T>
requires T is struct && has_fields(T, {"id"})
{
    value: T
}

struct Record { id: i64 }
fn make(value: Record) -> WithId<Record> => WithId<Record>(value: value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    Lowered rejected{R"(
module checks.struct_constraint_rejected

struct WithId<T>
requires T is struct && has_fields(T, {"id"})
{
    value: T
}

struct Missing { name: str }
fn make(value: Missing) -> WithId<Missing> => WithId<Missing>(value: value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("generic struct 'WithId' requirements are not satisfied") !=
          std::string::npos);
    CHECK(rejected.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR validates constrained structs in every type position", "[ir][typed][constraints][structs]") {
    Lowered concrete{R"(
module checks.constrained_type_position

struct Range<T>
requires T in {i64, f64}
{
    value: T
}

fn bad(value: Range<str>) -> Range<str> => value
)"};
    require_clean(concrete);
    CHECK_FALSE(complete(concrete));
    CHECK(concrete.diagnostics.render(concrete.file).find("generic struct 'Range' requirements are not satisfied") !=
          std::string::npos);

    Lowered field{R"(
module checks.constrained_field_position

struct Range<T>
requires T in {i64, f64}
{
    value: T
}

struct Holder {
    value: Range<str>
}
)"};
    require_clean(field);
    CHECK_FALSE(complete(field));
    CHECK(field.diagnostics.render(field.file).find("generic struct 'Range' requirements are not satisfied") != std::string::npos);

    Lowered generic{R"(
module checks.constrained_generic_position

struct Range<T>
requires T in {i64, f64}
{
    value: T
}

fn admitted<U>(value: Range<U>) -> Range<U>
requires U in {i64, f64}
=> value

operator inherited<T>(value: Range<T>) -> Range<T>
requires T in {i64, f64}

impl fn inherited<U>(value: Range<U>) -> Range<U> => value
)"};
    require_clean(generic);
    REQUIRE(complete(generic));

    Lowered missing_premise{R"(
module checks.constrained_generic_position_rejected

struct Range<T>
requires T in {i64, f64}
{
    value: T
}

fn rejected<U>(value: Range<U>) -> Range<U> => value
)"};
    require_clean(missing_premise);
    CHECK_FALSE(complete(missing_premise));
    CHECK(missing_premise.diagnostics.render(missing_premise.file).find("generic struct 'Range' requirements are not satisfied") !=
          std::string::npos);

    Lowered parent{R"(
module checks.constrained_parent_position

abstract struct Range<T>
requires T in {i64, f64}
{
    value: T
}

struct Invalid: Range<str> {}
)"};
    require_clean(parent);
    CHECK_FALSE(complete(parent));
    CHECK(parent.diagnostics.render(parent.file).find("generic struct 'Range' requirements are not satisfied") !=
          std::string::npos);
}

TEST_CASE("typed HIR records runtime state and capability effects", "[ir][typed][effects]") {
    Lowered lowered{R"(
module checks.effects

fn total(value: f64) -> f64 {
    state current: f64 = 0.0
    inject out, logger
    when modified(value) {
        current += value
        logger.info("updated")
        out = current
    }
}
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    const hir::FunctionDecl *fn = nullptr;
    for (const hir::Declaration &declaration : lowered.hir.declarations) {
        if (const auto *candidate = std::get_if<hir::FunctionDecl>(&declaration.node)) { fn = candidate; }
    }
    REQUIRE(fn != nullptr);
    CHECK(fn->kind == hir::FunctionKind::Runtime);
    CHECK(fn->capabilities.size() == 2);
    CHECK(hir::has_effect(fn->effects, hir::Effect::ReadRuntimeInput));
    CHECK(hir::has_effect(fn->effects, hir::Effect::WriteState));
    CHECK(hir::has_effect(fn->effects, hir::Effect::WriteOutput));
    CHECK(hir::has_effect(fn->effects, hir::Effect::UseCapability));
}

TEST_CASE("typed HIR keeps node reference inputs opaque", "[ir][typed][ref]") {
    const auto rejects = [](std::string source, std::string_view message) {
        Lowered lowered{std::move(source)};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK(lowered.diagnostics.render(lowered.file).find(message) != std::string::npos);
    };

    rejects(R"(
module checks.reference_arithmetic
fn invalid(value: ref<f64>) -> f64 {
    when modified(value) {
        return value + 1.0
    }
}
)",
            "node evaluation cannot read through ref<T>");

    rejects(R"(
module checks.reference_index
fn invalid(value: ref<list<f64, 3>>) -> f64 {
    when modified(value) {
        return value[0]
    }
}
)",
            "node evaluation cannot index through ref<T>");

    rejects(R"(
module checks.reference_field
struct Quote {
    price: f64
}
fn invalid(value: ref<Quote>) -> f64 {
    when modified(value) {
        return value.price
    }
}
)",
            "node evaluation cannot access fields through ref<T>");

    rejects(R"(
module checks.reference_condition
fn invalid(value: ref<bool>) -> bool {
    when modified(value) {
        if value {
            return true
        }
        return false
    }
}
)",
            "node evaluation cannot test a value through ref<T>");
}

TEST_CASE("typed HIR keeps signal payloads inaccessible to operators", "[ir][typed][signal]") {
    const auto rejects = [](std::string source) {
        Lowered lowered{std::move(source)};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK(lowered.diagnostics.render(lowered.file).find("'signal' has no payload and cannot be used with operators") !=
              std::string::npos);
    };

    rejects(R"(
module checks.signal_equality
fn invalid(pulse: signal) -> bool {
    when modified(pulse) {
        return pulse == true
    }
}
)");

    rejects(R"(
module checks.signal_unary
fn invalid(pulse: signal) -> bool {
    when modified(pulse) {
        return !pulse
    }
}
)");
}

TEST_CASE("typed HIR validates an explicit lambda against collection context", "[ir][typed][lambdas]") {
    Lowered lowered{R"(
module checks.lambda_context
use hgraph.std::{map}

fn wrong(values: map<str, f64>) -> map<str, f64> =>
    map(values, fn(value: i64) -> f64 => value)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.has_errors());
    CHECK(lowered.diagnostics.render(lowered.file).find("lambda parameter type conflicts with its call context") !=
          std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("typed HIR keeps graph iteration in the wiring phase", "[ir][typed][iteration]") {
    Lowered lowered{R"(
module checks.graph_iteration
use hgraph.std::{null_sink}

fn observe(samples: list<f64, 3>) {
    for sample in elements(samples) {
        null_sink(sample)
    }
}
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    const auto statement = std::ranges::find_if(
        lowered.hir.stmts, [](const hir::Stmt &candidate) { return std::holds_alternative<hir::ForStmt>(candidate.node); });
    REQUIRE(statement != lowered.hir.stmts.end());
    const auto &loop = std::get<hir::ForStmt>(statement->node);
    REQUIRE(loop.bindings.size() == 1U);
    CHECK(lowered.hir.expr(loop.iterable).phase == hir::Phase::Wiring);

    bool found_loop_value = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        const auto *reference = std::get_if<hir::SymbolRef>(&expression.node);
        if (reference == nullptr || reference->symbol != loop.bindings.front()) { continue; }
        found_loop_value = true;
        CHECK(expression.phase == hir::Phase::Wiring);
    }
    CHECK(found_loop_value);
}

TEST_CASE("typed HIR keeps values and elements as distinct projections", "[ir][typed][iteration]") {
    SECTION("a missing collection is diagnosed without dereferencing an invalid type") {
        Lowered lowered{R"(
module checks.missing_collection

fn observe() {
    for value in elements() { value }
}
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("'elements' takes a collection") != std::string::npos);
    }

    SECTION("values is not a list alias") {
        Lowered lowered{R"(
module checks.list_values

fn observe(samples: list<f64, 3>) {
    for sample in values(samples) { sample }
}
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("'values' takes a map") != std::string::npos);
    }

    SECTION("elements is not a map alias") {
        Lowered lowered{R"(
module checks.map_elements

fn observe(book: map<str, f64>) {
    for value in elements(book) { value }
}
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("'elements' takes a list or set") != std::string::npos);
    }
}

TEST_CASE("typed HIR rejects an invalid result without claiming completion", "[ir][typed][diagnostics]") {
    Lowered lowered{"module checks.bad\nfn wrong(value: f64) -> str => value\n"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.has_errors());
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("the resolved HIR dump is deterministic and source ranged", "[ir][snapshot]") {
    Lowered lowered{"module checks.snapshot\n\nfn add_one(value: f64) -> f64 => value + 1.0\n"};
    require_clean(lowered);

    CHECK(hgl::ir::print_hir(lowered.hir) == R"(HIR resolved module checks.snapshot
symbols
  s0 module checks.snapshot owner=d0 type=_ index=0 [0..22)
  s1 function add_one owner=d1 type=_ index=0 [27..34)
  s2 signal-parameter value owner=d1 type=t0 index=0 [35..40)
types
  t0 scalar f64 owner=d1 signal [42..45)
  t1 scalar f64 owner=d1 signal [50..53)
  t2 scalar f64 value [0..0)
native-functions
expressions
  e0 type=t0 phase=wiring value=signal ref s2 [57..62)
  e1 type=t2 phase=constant value=constant literal 1 [65..68)
  e2 type=_ phase=unknown value=unknown add e0 e1 [57..68)
statements
blocks
constraints
declarations
  d0 symbol=s0 module [0..22)
  d1 symbol=s1 internal composition function parameters=[s2:t0] result=t1 requires=_ concise=e2 block=_ [24..68)
source-order [d0, d1]
)");
}

TEST_CASE("HIR gives state and each injected capability its own identity", "[ir][symbols]") {
    Lowered lowered{R"(
module checks.stateful

fn total(value: f64) -> f64 {
    state current: f64 = 0.0
    inject out, logger

    when modified(value) && valid(value) {
        current += value
        logger.info("updated")
        out = current
    }
}
)"};
    require_clean(lowered);

    const std::optional<hir::SymbolId> current = lowered.referenced_symbol("current");
    const std::optional<hir::SymbolId> logger  = lowered.referenced_symbol("logger");
    const std::optional<hir::SymbolId> out     = lowered.referenced_symbol("out");
    REQUIRE(current);
    REQUIRE(logger);
    REQUIRE(out);
    CHECK(*current != *logger);
    CHECK(*current != *out);
    CHECK(*logger != *out);
    CHECK(lowered.hir.symbol(*current).kind == hir::SymbolKind::State);
    CHECK(lowered.hir.symbol(*logger).kind == hir::SymbolKind::InjectedCapability);
    CHECK(lowered.hir.symbol(*out).kind == hir::SymbolKind::InjectedCapability);
}

TEST_CASE("HIR resolves anonymous parameters independently of enclosing loop bindings", "[ir][symbols]") {
    Lowered lowered{R"(
module checks.lambda_scope

fn recent(book: map<str, f64>, const cutoff: datetime) {
    inject logger
    when modified(book) {
        for key, value in items(book, fn(key, value) => last_modified(value) > cutoff) {
            logger.info(key)
        }
    }
}
)"};
    require_clean(lowered);

    std::size_t lambda_parameters = 0;
    for (const hir::Symbol &symbol : lowered.hir.symbols) {
        if (symbol.kind == hir::SymbolKind::LambdaParameter) { ++lambda_parameters; }
    }
    CHECK(lambda_parameters == 2);

    for (ast::ExprId expression = 0; expression < lowered.ast.exprs.size(); ++expression) {
        const auto *name = std::get_if<ast::NameRef>(&lowered.ast.expr(expression).node);
        if (name == nullptr || name->name.text != "value") { continue; }
        const auto *reference = std::get_if<hir::SymbolRef>(&lowered.hir.expr(hir::ExprId{expression}).node);
        REQUIRE(reference != nullptr);
        CHECK(lowered.hir.symbol(reference->symbol).kind == hir::SymbolKind::LambdaParameter);
    }
}

TEST_CASE("bare generic type and value arguments retain resolved identities", "[ir][generics]") {
    Lowered lowered{R"(
module checks.generics

struct Box<T, const N: i64> {
    values: list<T, N>
}

fn identity<T, const N: i64>(value: Box<T, N>) -> Box<T, N> => value
)"};
    require_clean(lowered);

    std::size_t synthesized_type_arguments  = 0;
    std::size_t synthesized_value_arguments = 0;
    for (const hir::Type &type : lowered.hir.types) {
        for (const hir::TypeArgument &argument : type.arguments) {
            if (argument.kind == hir::TypeArgumentKind::Type) {
                REQUIRE(argument.type.valid());
                const hir::Type &referenced = lowered.hir.type(argument.type);
                if (referenced.range == argument.range && referenced.kind == hir::TypeKind::Symbol) {
                    ++synthesized_type_arguments;
                    CHECK(referenced.symbol.valid());
                }
            } else {
                REQUIRE(argument.value.valid());
                const hir::Expr &referenced = lowered.hir.expr(argument.value);
                if (referenced.range == argument.range) {
                    ++synthesized_value_arguments;
                    const auto *symbol = std::get_if<hir::SymbolRef>(&referenced.node);
                    REQUIRE(symbol != nullptr);
                    CHECK(symbol->symbol.valid());
                    CHECK(referenced.type.valid());
                }
            }
        }
    }
    CHECK(synthesized_type_arguments >= 2);
    CHECK(synthesized_value_arguments >= 2);
}

TEST_CASE("HIR lowering reports an unresolved identity instead of fabricating one", "[ir][diagnostics]") {
    hgl::syntax::SourceFile        file{"test.hgl", "module checks\nfn f() => missing\n"};
    hgl::syntax::DiagnosticSink    diagnostics;
    ast::Module                    module   = hgl::syntax::parse(file, diagnostics);
    hgl::semantics::ResolvedModule resolved = hgl::semantics::resolve(file, module, has_operator, diagnostics);
    REQUIRE(diagnostics.has_errors());

    // The driver never lowers a failed resolver result. If a pass client does,
    // the HIR boundary still fails closed rather than inventing a SymbolId.
    diagnostics              = {};
    const hir::Module result = hgl::ir::lower_to_hir(module, resolved, diagnostics);
    CHECK(diagnostics.has_errors());
    REQUIRE(result.exprs.size() == module.exprs.size());
    const auto *reference = std::get_if<hir::SymbolRef>(&result.expr(hir::ExprId{0}).node);
    REQUIRE(reference != nullptr);
    CHECK_FALSE(reference->symbol.valid());
}

// ---------------------------------------------------------------- #767 item 2:
// the language reference's shape, injectable and placement rules are typed-HIR
// diagnostics, not backend afterthoughts.

namespace
{
    std::string completion_diagnostics(std::string source) {
        Lowered lowered{std::move(source)};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        return lowered.diagnostics.render(lowered.file);
    }

    bool completes(std::string source) {
        Lowered lowered{std::move(source)};
        require_clean(lowered);
        const bool ok = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        return ok;
    }
}  // namespace

TEST_CASE("typed HIR enforces rolling and list size rules", "[ir][typed][shape]") {
    CHECK(completes("module checks.sizes_ok\n"
                    "export fn a(w: rolling<f64, 20, 5>, v: rolling<f64, 5m, 0s>, xs: list<f64, 3>) -> f64 => 1.0\n"
                    "export fn b<T, const n: i64>(xs: list<T, n>, w: rolling<T, n>) -> f64 => 1.0\n"
                    "export fn empty(xs: list<f64, 0>) -> f64 => 1.0\n"));
    CHECK(completion_diagnostics("module checks.rolling_mixed\n"
                                 "export fn f(w: rolling<f64, 5m, 3>) -> f64 => 1.0\n")
              .find("rolling sizes must both be i64 or both be duration") != std::string::npos);
    CHECK(completion_diagnostics("module checks.rolling_min\n"
                                 "export fn f(w: rolling<f64, 20, 25>) -> f64 => 1.0\n")
              .find("a rolling minimum size must be positive and no larger than the maximum") != std::string::npos);
    CHECK(completion_diagnostics("module checks.rolling_zero\n"
                                 "export fn f(w: rolling<f64, 0>) -> f64 => 1.0\n")
              .find("a rolling tick size must be positive") != std::string::npos);
    CHECK(completion_diagnostics("module checks.rolling_zero_min\n"
                                 "export fn f(w: rolling<f64, 20, 0>) -> f64 => 1.0\n")
              .find("a rolling minimum size must be positive") != std::string::npos);
    CHECK(completion_diagnostics("module checks.rolling_long_min\n"
                                 "export fn f(w: rolling<f64, 5m, 6m>) -> f64 => 1.0\n")
              .find("a rolling minimum duration must be non-negative and no longer than the maximum") != std::string::npos);
    // A symbolic size has no folded value but does have a declared kind.
    CHECK(completion_diagnostics("module checks.list_duration_size\n"
                                 "export fn f<const n: duration>(xs: list<f64, n>) -> f64 => 1.0\n")
              .find("a list size must be an i64 constant or 'unbounded'") != std::string::npos);
}

TEST_CASE("typed HIR admits only approved injectables", "[ir][typed][injectable]") {
    CHECK(completes("module checks.inject_ok\n"
                    "fn f(value: f64) -> f64 {\n"
                    "    inject out, logger\n"
                    "    when modified(value) { out = value }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.inject_unknown\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, banana\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("injectable: 'banana' is not an approved runtime capability") != std::string::npos);
    CHECK(completion_diagnostics("module checks.inject_clock\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, clock\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("injectable: the 'clock' injectable is agreed but not implemented yet") != std::string::npos);
    CHECK(completion_diagnostics("module checks.inject_outputless\n"
                                 "fn f(value: f64) {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("injectable: 'out' requires a function output") != std::string::npos);
}

TEST_CASE("typed HIR enforces runtime body placement", "[ir][typed][function-kind]") {
    CHECK(completion_diagnostics("module checks.late_state\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "    state total: f64 = 0.0\n"
                                 "}\n")
              .find("function-kind: 'state' must be declared before runtime handlers") != std::string::npos);
    CHECK(completion_diagnostics("module checks.nested_when\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) {\n"
                                 "        when valid(value) { out = value }\n"
                                 "    }\n"
                                 "}\n")
              .find("function-kind: 'when' cannot be nested in another block") != std::string::npos);
    CHECK(completion_diagnostics("module checks.two_starts\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, logger\n"
                                 "    start { logger.info(\"a\") }\n"
                                 "    start { logger.info(\"b\") }\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("function-kind: a runtime function has at most one 'start' block") != std::string::npos);
    CHECK(completion_diagnostics("module checks.out_in_stop\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "    stop { out = 0.0 }\n"
                                 "}\n")
              .find("phase: 'out' is not available during stop") != std::string::npos);
    CHECK(completion_diagnostics("module checks.return_in_start\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    start { return 1.0 }\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("phase: 'return' is not available during start") != std::string::npos);
    // Function-level forms hidden inside nested blocks and expressions.
    CHECK(completion_diagnostics("module checks.nested_start\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, logger\n"
                                 "    when modified(value) {\n"
                                 "        start { logger.info(\"late\") }\n"
                                 "        out = value\n"
                                 "    }\n"
                                 "}\n")
              .find("function-kind: 'start' must be a function-level block, not nested in another block") != std::string::npos);
    CHECK(completion_diagnostics("module checks.nested_state\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) {\n"
                                 "        state total: f64 = 0.0\n"
                                 "        out = value\n"
                                 "    }\n"
                                 "}\n")
              .find("function-kind: 'state' must be declared at function level, not inside a block") != std::string::npos);
    CHECK(completion_diagnostics("module checks.when_in_operand\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) {\n"
                                 "        out = 1.0 + { when valid(value) { out = value }\n"
                                 "                      value }\n"
                                 "    }\n"
                                 "}\n")
              .find("function-kind: 'when' cannot be nested in another block") != std::string::npos);
}
