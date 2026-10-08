#include "ir/hir_printer.h"
#include "ir/lower.h"
#include "ir/list_literal_admission.h"
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
            if (query.identity == "const" && !query.arguments.empty()) { result.result = query.arguments.front().type; }
            result.candidate_label = query.identity + "(<typed>)";
            result.deferred        = true;
            return result;
        };
        return hgl::ir::complete_hir(lowered.hir, resolver, lowered.diagnostics);
    }

    hgl::semantics::ModuleCatalog
    native_catalog(std::vector<hgl::semantics::NativeCallPhase> phases = {hgl::semantics::NativeCallPhase::Evaluation},
                   hgl::NativeExecutionRole                     role   = hgl::NativeExecutionRole::LegacyValue) {
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
            .execution_role         = role,
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

    SECTION("forwarded premise with negated logic") {
        Lowered lowered{R"(
module packs.each_forwarded_negation
fn accepts<...Ts>(values: ...Ts) -> i64
requires each T in types(Ts) {
    !(T in {f64} || T in {bool})
}
=> 1

fn forward<...Us>(values: ...Us) -> i64
requires each U in types(Us) {
    !(U in {f64} || U in {bool})
}
=> accepts(values)

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

native fn len<T>(value: set<T>) -> i64 throws {
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
    CHECK_FALSE(list_len.throws);
    CHECK(set_len.throws);
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

TEST_CASE("runtime parameter packs expose borrowed schema views to native functions", "[ir][native][parameter-pack][schema]") {
    Lowered lowered{R"(
module checks.runtime_schemas

native fn known(value: schema) -> bool {
    cpp(const hgraph::TSValueTypeMetaData *value) { return value != nullptr; }
}

fn positional<...Ts>(values: ...Ts) -> i64 {
    when {
        var count = 0
        for value_schema in elements(schemas(values)) {
            if known(value_schema) { count += 1 }
        }
        return count
    }
}

fn named<...Fields>(values: ...{Fields}) -> i64 {
    when {
        var count = 0
        for name, value_schema in items(schemas(values)) {
            if known(value_schema) && name == "price" { count += 1 }
        }
        return count
    }
}
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));
    INFO(lowered.diagnostics.render(lowered.file));

    std::vector<const hir::Type *> views;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.identity != "schemas") { continue; }
        const hir::Type &type = lowered.hir.type(expression.type);
        REQUIRE(type.kind == hir::TypeKind::SchemaView);
        views.push_back(&type);
    }
    REQUIRE(views.size() == 2U);
    CHECK_FALSE(views[0]->schema_view_named);
    CHECK(views[1]->schema_view_named);
    REQUIRE(views[0]->children.size() == 1U);
    CHECK(lowered.hir.type(views[0]->children.front()).kind == hir::TypeKind::Schema);
}

TEST_CASE("borrowed schema metadata cannot escape its runtime iteration", "[ir][native][parameter-pack][schema]") {
    Lowered local_escape{R"(
module checks.schema_local
native fn known(value: schema) -> bool {
    cpp(const hgraph::TSValueTypeMetaData *value) { return value != nullptr; }
}
fn invalid<...Ts>(values: ...Ts) -> i64 {
    when {
        for value_schema in elements(schemas(values)) {
            let saved = value_schema
            if known(saved) { return 1 }
        }
        return 0
    }
}
)"};
    require_clean(local_escape);
    CHECK_FALSE(complete(local_escape));
    CHECK(
        local_escape.diagnostics.render(local_escape.file).find("borrowed schema metadata cannot be stored in a local variable") !=
        std::string::npos);

    Lowered ordinary_parameter{"module checks.schema_parameter\nfn invalid(value: schema) -> i64 => 0\n"};
    CHECK_FALSE(complete(ordinary_parameter));
    CHECK(ordinary_parameter.diagnostics.render(ordinary_parameter.file)
              .find("'schema' is borrowed runtime metadata and is only valid as a non-const native parameter type") !=
          std::string::npos);

    Lowered filtered_view{R"(
module checks.schema_filter
native fn known(value: schema) -> bool {
    cpp(const hgraph::TSValueTypeMetaData *value) { return value != nullptr; }
}
fn invalid<...Ts>(values: ...Ts) -> i64 {
    when {
        for value_schema in elements(schemas(values), valid) {
            if known(value_schema) { return 1 }
        }
        return 0
    }
}
)"};
    require_clean(filtered_view);
    CHECK_FALSE(complete(filtered_view));
    CHECK(filtered_view.diagnostics.render(filtered_view.file).find("schema views do not support runtime value predicates") !=
          std::string::npos);
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

namespace
{
    // The binding of the single type parameter of each call to `callee`,
    // in HGL spelling: `f64`, `ref<f64>`, `list<f64>`.
    std::string spell(const hir::Module &module, hir::TypeId id) {
        const hir::Type &type = module.type(id);
        switch (type.kind) {
            case hir::TypeKind::Scalar: return type.scalar == hir::ScalarType::F64 ? "f64" : "scalar";
            case hir::TypeKind::Reference: return "ref<" + spell(module, type.children.front()) + ">";
            case hir::TypeKind::List: {
                std::string extent;
                if (type.size.valid() && module.expr(type.size).constant) {
                    extent = ", " + std::to_string(std::get<std::int64_t>(*module.expr(type.size).constant));
                }
                return "list<" + spell(module, type.children.front()) + extent + ">";
            }
            default: return "?";
        }
    }

    std::vector<std::string> bindings_of(const Lowered &lowered, std::string_view callee) {
        std::vector<std::string> result;
        for (const hir::Expr &expression : lowered.hir.exprs) {
            if (expression.operation.identity != callee) { continue; }
            REQUIRE(expression.operation.substitutions.size() == 1U);
            result.push_back(spell(lowered.hir, expression.operation.substitutions.front().type));
        }
        return result;
    }
}  // namespace

TEST_CASE("typed HIR binds a generic with every reference removed (runtime spec WIR-7, WIR-14)",
          "[ir][typed][generics][ref]") {
    // external/hgraph_spec/runtime/validation/wiring/front_end.hgl: the runtime
    // binds T to the argument's type without its references, and so must HGL.
    Lowered lowered{R"(
module checks.wiring_front_end

fn pass<T>(value: T) -> T {
    when modified(value) {
        return value
    }
}

export fn through_ref(value: ref<f64>) -> f64 =>
    pass(value)

export fn through_list(values: list<ref<f64>, 2>) -> list<f64, 2> =>
    pass(values)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    CHECK(bindings_of(lowered, "checks.wiring_front_end.pass") == std::vector<std::string>{"f64", "list<f64, 2>"});
}

TEST_CASE("typed HIR binds a generic beneath a reference pattern (runtime spec WIR-10)",
          "[ir][typed][generics][ref]") {
    // Inference removes the argument's reference, so wrap's T is f64 and its
    // ref<T> result is ref<f64>: no nested reference is formed.
    Lowered lowered{R"(
module checks.reference_pattern

fn wrap<T>(value: T) -> ref<T> => value
fn forwarded(value: ref<f64>) -> ref<f64> => wrap(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    CHECK(bindings_of(lowered, "checks.reference_pattern.wrap") == std::vector<std::string>{"f64"});
}

TEST_CASE("typed HIR selects a materialized local candidate for a reference argument (runtime spec WIR-6, WIR-7)",
          "[ir][typed][generics][operators][ref]") {
    // T binds f64 from ref<f64>; the candidate's f64 parameter then matches
    // the argument ignoring references, so the call is not left deferred.
    Lowered lowered{R"(
module checks.local_reference_candidate

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T
requires T in {i64, f64}
=> value

instantiate choose<f64>

fn choose_ref(value: ref<f64>) -> f64 => choose(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (std::holds_alternative<hir::Call>(expression.node) && expression.operation.identity.find("choose") != std::string::npos) {
            CHECK_FALSE(expression.operation.deferred);
        }
    }
}

TEST_CASE("typed HIR requirements reject a candidate that adds a reference to the requested result (runtime spec WIR-12)",
          "[ir][typed][constraints][operators][ref]") {
    // The request is a plain T; an implementation producing ref<f64> cannot
    // produce it, as the runtime's directional output matching decides.
    Lowered lowered{R"(
module checks.requested_result_reference

operator wrap<T>(value: T) -> ref<T>
impl fn wrap(value: f64) -> ref<f64> => value

fn keep<T>(value: T) -> T
requires wrap(T) -> T
=> value

fn apply(value: f64) -> f64 => keep(value)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("operator requirement has no implementation") != std::string::npos);
}

TEST_CASE("typed HIR requirements match the requested result before the arguments (runtime spec WIR-17)",
          "[ir][typed][constraints][operators][ref]") {
    // Instantiating id<ref<f64>> solves its requirement probe(T) -> T as
    // probe(ref<f64>) -> ref<f64>: the requested result binds T to ref<f64>
    // first, and the reference argument then matches it as supplied, as the
    // runtime matcher does. Arguments first bound f64 and then rejected the
    // requested ref<f64>.
    Lowered lowered{R"(
module checks.requested_result_first

operator probe<T>(value: T) -> T
impl fn probe(value: ref<f64>) -> ref<f64> => value

operator id<T>(value: T) -> T
requires probe(T) -> T

impl fn id<T>(value: T) -> T => value

instantiate id<ref<f64>>
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
}

TEST_CASE("typed HIR accepts an implementation that extends its operator contract (runtime spec WIR-22)",
          "[ir][typed][operators][contract]") {
    // An operator's signature is the minimum an implementation meets: it may
    // declare more parameters, each with a default a call through the
    // contract uses (owner ruling 2026-09-26).
    Lowered lowered{R"(
module checks.contract_superset

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64 = 2.0) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
}

TEST_CASE("typed HIR rejects an implementation that drops a contract parameter (runtime spec WIR-22)",
          "[ir][typed][operators][contract]") {
    Lowered dropped{R"(
module checks.contract_dropped

operator combine<T>(lhs: T, rhs: T) -> T

impl fn combine(lhs: f64) -> f64 => lhs

fn use_combine(value: f64) -> f64 => combine(value, value)
)"};
    require_clean(dropped);
    CHECK_FALSE(complete(dropped));
    CHECK(dropped.diagnostics.render(dropped.file).find("declares every parameter of its operator contract") !=
          std::string::npos);
}

TEST_CASE("typed HIR does not select an implementation whose extra parameter the call does not supply (runtime spec WIR-22)",
          "[ir][typed][operators][contract]") {
    // No default is needed (owner ruling 2026-09-26): the implementation is
    // valid, and simply does not match a call that omits the argument.
    Lowered lowered{R"(
module checks.contract_required_extra

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value)
)"};
    require_clean(lowered);
    (void)complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    CHECK(lowered.diagnostics.render(lowered.file).find("declares every parameter") == std::string::npos);
    bool saw_call = false;
    for (const hir::Expr &expression : lowered.hir.exprs) {
        if (expression.operation.kind != hir::OperationKind::NominalOperator) { continue; }
        saw_call = true;
        CHECK_FALSE(expression.operation.candidate.valid());
    }
    CHECK(saw_call);
}

namespace {
    /// The implementation each nominal operator call in ``lowered`` selected.
    std::vector<bool> selected_candidates(const Lowered &lowered) {
        std::vector<bool> selected;
        for (const hir::Expr &expression : lowered.hir.exprs) {
            if (expression.operation.kind != hir::OperationKind::NominalOperator ||
                expression.operation.identity.find(".pick") == std::string::npos) {
                continue;
            }
            selected.push_back(expression.operation.candidate.valid());
        }
        return selected;
    }
}  // namespace

TEST_CASE("typed HIR passes an operator call's extra arguments to the implementation (runtime spec WIR-22, WV-7)",
          "[ir][typed][operators][contract]") {
    // Every operator behaves as if its signature ended with *args, **kwargs:
    // an argument the contract does not declare goes to the implementations,
    // by name or by position after the contract's parameters.
    SECTION("by name") {
        Lowered lowered{R"(
module checks.extra_keyword

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value, scale: 5.0)
)"};
        require_clean(lowered);
        const bool completed = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(completed);
        CHECK(selected_candidates(lowered) == std::vector<bool>{true});
    }
    SECTION("by position") {
        Lowered lowered{R"(
module checks.extra_positional

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value, 5.0)
)"};
        require_clean(lowered);
        const bool completed = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(completed);
        CHECK(selected_candidates(lowered) == std::vector<bool>{true});
    }
    SECTION("an extra of the wrong type does not match") {
        Lowered lowered{R"(
module checks.extra_mistyped

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value, scale: "wide")
)"};
        require_clean(lowered);
        (void)complete(lowered);
        CHECK(selected_candidates(lowered) == std::vector<bool>{false});
    }
    SECTION("a call without the extra takes a defaulted parameter's default") {
        Lowered lowered{R"(
module checks.extra_defaulted

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64 = 2.0) -> f64 => value * scale

fn use_pick(value: f64) -> f64 => pick(value)
fn use_scaled(value: f64) -> f64 => pick(value, scale: 3.0)
)"};
        require_clean(lowered);
        const bool completed = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(completed);
        CHECK(selected_candidates(lowered) == std::vector<bool>{true, true});
    }
}

TEST_CASE("typed HIR reports an extra argument no implementation accepts (runtime spec WIR-22, WV-7)",
          "[ir][typed][operators][contract]") {
    Lowered lowered{R"(
module checks.extra_unaccepted

operator pick<T>(value: T) -> T

impl fn pick(value: f64) -> f64 => value

fn by_name(value: f64) -> f64 => pick(value, scale: 5.0)
fn by_position(value: f64) -> f64 => pick(value, 5.0)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    const std::string rendered = lowered.diagnostics.render(lowered.file);
    CHECK(rendered.find("no implementation of operator 'pick' accepts the argument 'scale'") != std::string::npos);
    CHECK(rendered.find("no implementation of operator 'pick' accepts the positional argument 2") != std::string::npos);
}

TEST_CASE("typed HIR checks an extra argument against the implementations of the caller's build (runtime spec WIR-22, WV-7)",
          "[ir][typed][operators][contract]") {
    // A production call sees only production implementations. A test-only
    // implementation cannot be written today: a test block holds only private
    // helper functions, so an `impl fn` there is rejected before type
    // checking, and never accepts a production call's extra argument.
    SECTION("an implementation in a test block is rejected") {
        const hgl::syntax::SourceFile file{"test.hgl", R"(
module checks.extra_test_only

operator pick<T>(value: T) -> T

impl fn pick(value: f64) -> f64 => value

fn use_pick(value: f64) -> f64 => pick(value, scale: 5.0)

test {
    impl fn pick(value: f64, const scale: f64) -> f64 => value * scale
}
)"};
        hgl::syntax::DiagnosticSink diagnostics;
        (void)hgl::syntax::parse(file, diagnostics);
        CHECK(diagnostics.render(file).find("test helpers must be private fn declarations") != std::string::npos);
    }
    SECTION("test code sees the production implementations") {
        Lowered lowered{R"(
module checks.extra_in_test

operator pick<T>(value: T) -> T

impl fn pick(value: f64, const scale: f64) -> f64 => value * scale

test {
    fn use_pick(value: f64) -> f64 => pick(value, scale: 5.0)
}
)"};
        require_clean(lowered);
        const bool completed = complete(lowered);
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(completed);
        CHECK(selected_candidates(lowered) == std::vector<bool>{true});
    }
}

TEST_CASE("typed HIR requirements admit an implementation that extends its contract (runtime spec WIR-22)",
          "[ir][typed][constraints][operators][contract]") {
    // A requirement supplies the contract's arguments; an implementation whose
    // extra parameter has a default satisfies it.
    Lowered lowered{R"(
module checks.requirement_superset

operator pick<T>(value: T) -> T
impl fn pick(value: f64, const scale: f64 = 2.0) -> f64 => value * scale

fn scaled<T>(value: T) -> T
requires pick(T) -> T
=> pick(value)

fn apply(value: f64) -> f64 => scaled(value)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
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
        info(logger, "updated")
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
        info(logger, "updated")
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
            info(logger, key)
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
    // ADR 0010: clock and scheduler are implemented capabilities.
    CHECK(completes("module checks.inject_clock\n"
                    "fn f(value: f64) -> f64 {\n"
                    "    inject out, clock, scheduler\n"
                    "    when modified(value) { if clock.evaluation_time > @2020-01-01T00:00Z { out = value } }\n"
                    "    when scheduled() {\n"
                    "        schedule(scheduler, 1s)\n"
                    "        passivate(value)\n"
                    "    }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.scheduled_needs_scheduler\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when scheduled() { out = value }\n"
                                 "}\n")
              .find("injectable: 'scheduled' requires 'inject scheduler'") != std::string::npos);
    CHECK(completion_diagnostics("module checks.scheduled_outside_when\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, scheduler\n"
                                 "    when modified(value) { if scheduled() { out = value } }\n"
                                 "}\n")
              .find("'scheduled' is only valid in a function-level 'when' condition") != std::string::npos);
    CHECK(completion_diagnostics("module checks.passivate_projection\n"
                                 "fn f(value: map<str, f64>) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) { passivate(key_set(value)) }\n"
                                 "}\n")
              .find("'passivate' takes a temporal parameter of this function, not a projection") != std::string::npos);
    CHECK(completion_diagnostics("module checks.scheduler_method\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, scheduler\n"
                                 "    when modified(value) { schedule(scheduler, 1) }\n"
                                 "}\n")
              .find("scheduler.schedule delay") != std::string::npos);
    CHECK(completion_diagnostics("module checks.clock_method\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out, clock\n"
                                 "    when modified(value) { out = clock.wall() }\n"
                                 "}\n")
              .find("capabilities expose only clock properties") != std::string::npos);
    CHECK(completion_diagnostics("module checks.inject_outputless\n"
                                 "fn f(value: f64) {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "}\n")
              .find("injectable: 'out' requires a function output") != std::string::npos);
}

TEST_CASE("typed HIR requires a scheduler for runtime sources", "[ir][typed][lifecycle]") {
    CHECK(completion_diagnostics("module checks.unscheduled_source\n"
                                 "fn source(const value: i64) -> i64 { when { return value } }\n")
              .find("injectable: a runtime function without temporal parameters must 'inject scheduler' or 'inject alarm'") !=
          std::string::npos);
    CHECK(completes("module checks.scheduled_source\n"
                    "fn source() -> bool {\n"
                    "    inject scheduler\n"
                    "    start { schedule(scheduler, 0s) }\n"
                    "    when scheduled() { return true }\n"
                    "}\n"));
}

TEST_CASE("typed HIR admits the stateless alarm in sources only", "[ir][typed][adr-0015]") {
    // An alarm source publishes from a plain `when`: every evaluation is its
    // wake-up. `scheduled()` belongs to `scheduler`.
    CHECK(completes("module checks.alarm_source\n"
                    "fn source(const value: i64, const delay: duration = 0s) -> i64 {\n"
                    "    inject alarm\n"
                    "    start { schedule(alarm, delay) }\n"
                    "    when { return value }\n"
                    "}\n"));
    CHECK(completes("module checks.alarm_at\n"
                    "fn source(const at: datetime) -> bool {\n"
                    "    inject alarm\n"
                    "    start { schedule_at(alarm, at) }\n"
                    "    when { return true }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.alarm_scheduled\n"
                                 "fn source(const value: i64) -> i64 {\n"
                                 "    inject alarm\n"
                                 "    start { schedule(alarm, 1s) }\n"
                                 "    when scheduled() { return value }\n"
                                 "}\n")
              .find("'scheduled' requires 'inject scheduler'; a source on 'alarm' publishes from a plain 'when'") !=
          std::string::npos);
    CHECK(completion_diagnostics("module checks.alarm_with_input\n"
                                 "fn f(value: i64) -> i64 {\n"
                                 "    inject alarm\n"
                                 "    when modified(value) { schedule(alarm, 1s)\n        return value }\n"
                                 "}\n")
              .find("'alarm' is admitted only in a source") != std::string::npos);
    CHECK(completion_diagnostics("module checks.alarm_query\n"
                                 "fn source() -> bool {\n"
                                 "    inject alarm\n"
                                 "    start { schedule(alarm, 1s) }\n"
                                 "    when { return is_scheduled(alarm) }\n"
                                 "}\n")
              .find("'alarm.is_scheduled' is not a capability method") != std::string::npos);
    CHECK(completion_diagnostics("module checks.alarm_const\n"
                                 "const fn f(value: i64) -> i64 {\n"
                                 "    inject alarm\n"
                                 "    return value\n"
                                 "}\n")
              .find("const fn cannot inject its own 'alarm'") != std::string::npos);
    // One wake-up mechanism per source: with both injected, scheduled()
    // could not answer for the alarm.
    CHECK(completion_diagnostics("module checks.alarm_and_scheduler\n"
                                 "fn source(const value: i64) -> i64 {\n"
                                 "    inject scheduler, alarm\n"
                                 "    start { schedule(alarm, 1s) }\n"
                                 "    when scheduled() { return value }\n"
                                 "}\n")
              .find("a source injects 'scheduler' or 'alarm', not both") != std::string::npos);
}

TEST_CASE("typed HIR admits while in runtime bodies only", "[ir][typed][adr-0015]") {
    CHECK(completes("module checks.while_runtime\n"
                    "fn f(value: i64) -> i64 {\n"
                    "    when modified(value) {\n"
                    "        var n: i64 = 0\n"
                    "        while n < value { n += 1 }\n"
                    "        while { return n }\n"
                    "    }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.while_composition\n"
                                 "fn f(value: i64) -> i64 {\n"
                                 "    while value {\n"
                                 "        value\n"
                                 "    }\n"
                                 "    value\n"
                                 "}\n")
              .find("'while' is a runtime statement") != std::string::npos);
    CHECK(completion_diagnostics("module checks.while_condition\n"
                                 "fn f(value: i64) -> i64 {\n"
                                 "    when modified(value) {\n"
                                 "        while value { return value }\n"
                                 "    }\n"
                                 "}\n")
              .find("while condition") != std::string::npos);
}

TEST_CASE("typed HIR classifies a yielding body as a generator source", "[ir][typed][adr-0015]") {
    CHECK(completes("module checks.generator\n"
                    "fn constant(const value: i64, const delay: duration = 0s) -> i64 {\n"
                    "    yield delay: value\n"
                    "}\n"
                    "fn heartbeat(const period: duration, const beats: i64) -> bool {\n"
                    "    var count: i64 = 0\n"
                    "    while count < beats {\n"
                    "        yield period: true\n"
                    "        count += 1\n"
                    "    }\n"
                    "}\n"
                    "fn at(const when_: datetime) -> i64 {\n"
                    "    yield when_: 1\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.generator_input\n"
                                 "fn g(value: i64) -> i64 {\n"
                                 "    yield 0s: value\n"
                                 "}\n")
              .find("a generator source has no temporal parameters") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_result\n"
                                 "fn g(const value: i64) {\n"
                                 "    yield 0s: value\n"
                                 "}\n")
              .find("a generator source declares a result type") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_state\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    state count: i64 = 0\n"
                                 "    yield 0s: value\n"
                                 "}\n")
              .find("a generator source declares no state") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_out\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    inject out\n"
                                 "    yield 0s: value\n"
                                 "}\n")
              .find("a generator source cannot inject 'out'") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_when\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    yield 0s: value\n"
                                 "    when { return value }\n"
                                 "}\n")
              .find("a generator source has no 'when' handler") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_time\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    yield \"soon\": value\n"
                                 "}\n")
              .find("a yield time is a duration (from now) or a datetime") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_value\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    yield 0s: \"text\"\n"
                                 "}\n")
              .find("yield value") != std::string::npos);
}

TEST_CASE("typed HIR rejects input activity in lifecycle hooks", "[ir][typed][lifecycle]") {
    for (const std::string hook : {"start", "stop"}) {
        for (const std::string operation : {"passivate", "activate"}) {
            const std::string source = "module checks.activity_hook\n"
                                       "fn f(value: i64) -> i64 {\n    " + hook + " { " + operation +
                                       "(value) }\n    when { return value }\n}\n";
            INFO(source);
            CHECK(completion_diagnostics(source).find("phase: '" + operation +
                  "' needs temporal inputs, which are unavailable during " + hook) != std::string::npos);
        }
    }
}

TEST_CASE("typed HIR constrains functional output mutations", "[ir][typed][collection][mutation]") {
    CHECK(completes("module checks.output_mutations\n"
                    "fn set_ops(value: i64) -> set<i64> {\n"
                    "    inject out\n"
                    "    when {\n"
                    "        insert(out, value)\n"
                    "        upsert(out, value)\n"
                    "        remove(out, value)\n"
                    "        discard(out, value)\n"
                    "        clear(out)\n"
                    "    }\n"
                    "}\n"
                    "fn map_ops(key: str, value: i64) -> map<str, i64> {\n"
                    "    inject out\n"
                    "    when {\n"
                    "        insert(out, key, value)\n"
                    "        update(out, key, value)\n"
                    "        upsert(out, key, value)\n"
                    "        remove(out, key)\n"
                    "        discard(out, key)\n"
                    "        invalidate(out, key)\n"
                    "        clear(out)\n"
                    "    }\n"
                    "}\n"
                    "fn list_ops(value: i64) -> list<i64, unbounded> {\n"
                    "    inject out\n"
                    "    when {\n"
                    "        push(out, value)\n"
                    "        invalidate(out, 0)\n"
                    "        pop(out)\n"
                    "        clear(out)\n"
                    "    }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.mutation_target\n"
                                 "fn f(value: set<i64>) -> set<i64> {\n"
                                 "    inject out\n"
                                 "    when { insert(value, 1) }\n"
                                 "}\n")
              .find("'insert' requires 'out' as its first argument") != std::string::npos);
    CHECK(completion_diagnostics("module checks.fixed_push\n"
                                 "fn f(value: i64) -> list<i64, 2> {\n"
                                 "    inject out\n"
                                 "    when { push(out, value) }\n"
                                 "}\n")
              .find("'push' requires an unbounded list output") != std::string::npos);
    CHECK(completion_diagnostics("module checks.set_update\n"
                                 "fn f(value: i64) -> set<i64> {\n"
                                 "    inject out\n"
                                 "    when { update(out, value, value) }\n"
                                 "}\n")
              .find("'update' requires a map output") != std::string::npos);
    CHECK(completion_diagnostics("module checks.structural_map_value\n"
                                 "fn f(values: set<i64>) -> map<str, set<i64>> {\n"
                                 "    inject out\n"
                                 "    when { upsert(out, \"key\", values) }\n"
                                 "}\n")
              .find("'upsert' cannot stage a structural map value") != std::string::npos);
    CHECK(completion_diagnostics("module checks.structural_list_value\n"
                                 "fn f(values: set<i64>) -> list<set<i64>, unbounded> {\n"
                                 "    inject out\n"
                                 "    when { push(out, values) }\n"
                                 "}\n")
              .find("'push' cannot stage a structural list value") != std::string::npos);
    CHECK(completes("module checks.atomic_collection_value\n"
                    "fn f(values: atomic<set<i64>>) -> map<str, atomic<set<i64>>> {\n"
                    "    inject out\n"
                    "    when { upsert(out, \"key\", values) }\n"
                    "}\n"));
}

TEST_CASE("typed HIR enforces runtime body placement", "[ir][typed][function-kind]") {
    CHECK(completion_diagnostics("module checks.late_state\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "    state total: f64 = 0.0\n"
                                 "}\n")
              .find("function-kind: 'state' must be declared before runtime handlers") != std::string::npos);
    CHECK(completion_diagnostics("module checks.late_cache\n"
                                 "fn f(value: f64) -> f64 {\n"
                                 "    inject out\n"
                                 "    when modified(value) { out = value }\n"
                                 "    cache last: f64 = 0.0\n"
                                 "}\n")
              .find("function-kind: 'cache' must be declared before runtime handlers") != std::string::npos);
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
                                 "    start { info(logger, \"a\") }\n"
                                 "    start { info(logger, \"b\") }\n"
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
                                 "        start { info(logger, \"late\") }\n"
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

// ADR 0012: an admitted recursive edge enters typed HIR as a marked field.
// The target stays the nominal struct inside its `atomic<...>`; no pass
// expands it. Every struct inheriting the edge carries the mark as well.
TEST_CASE("an admitted recursive struct edge is marked in typed HIR", "[ir][recursive]") {
    const Lowered lowered{"module t\nabstract struct Expr { next: atomic<Expr> = null }\nstruct Lit: Expr { value: i64 }\n"
                          "struct Node {\n value: i64\n next: atomic<Node> = null\n}\n"};
    require_clean(lowered);
    const auto fields = [&](std::string_view name) -> const std::vector<hir::StructField> & {
        for (const hir::Declaration &declaration : lowered.hir.declarations) {
            const auto *structure = std::get_if<hir::StructDecl>(&declaration.node);
            if (structure != nullptr && lowered.hir.symbol(declaration.symbol).name == name) { return structure->fields; }
        }
        FAIL("no struct " << name);
        throw 0;
    };
    const auto marked = [&](std::string_view structure, std::string_view field) {
        const auto &items = fields(structure);
        const auto  found = std::ranges::find(items, field, &hir::StructField::name);
        REQUIRE(found != items.end());
        return found->recursive;
    };
    CHECK(marked("Expr", "next"));
    CHECK(marked("Lit", "next"));
    CHECK_FALSE(marked("Lit", "value"));
    CHECK(marked("Node", "next"));
    CHECK_FALSE(marked("Node", "value"));
    CHECK(hgl::ir::print_hir(lowered.hir).find(" recursive") != std::string::npos);
}

TEST_CASE("native value roles survive imports and lift while temporal roles cannot be value calls", "[ir][native][interface]") {
    using hgl::NativeExecutionRole;
    using hgl::semantics::NativeCallPhase;
    for (const auto role : {NativeExecutionRole::Value, NativeExecutionRole::Temporal}) {
        const auto catalog = native_catalog({NativeCallPhase::Evaluation}, role);
        for (const auto body : {"fn f(value: f64) -> f64 => blend(value, 3)", "const fn f(value: f64) -> f64 => blend(value, 3)"}) {
            Lowered lowered{std::string{"module t\nuse acme.stats::{blend}\n"} + body + "\n", catalog};
            require_clean(lowered);
            REQUIRE(lowered.hir.native_functions.front().execution_role == role);
            if (role == NativeExecutionRole::Value) {
                CHECK(complete(lowered));
                INFO(lowered.diagnostics.render(lowered.file));
                CHECK_FALSE(lowered.diagnostics.has_errors());
            } else {
                CHECK_FALSE(complete(lowered));
                CHECK((lowered.diagnostics.render(lowered.file).find("graph construction") != std::string::npos ||
                       lowered.diagnostics.render(lowered.file).find("not available during wiring") != std::string::npos));
            }
        }
    }
}

TEST_CASE("inline native fn cannot silently become a const fn dependency", "[ir][native][interface]") {
    Lowered lowered{R"(module t
native fn legacy(value: i64) -> i64 { cpp(hgraph::Int value) { return value; } }
const fn value(value: i64) -> i64 => legacy(value)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("declare native const fn") != std::string::npos);
}

TEST_CASE("value helper requirements silently upgrade callers transitively", "[ir][typed][capabilities]") {
    Lowered unit{R"hgl(module capabilities.transitive
fn caller(value: i64) -> i64 {
    when { return middle(value) }
}
const fn middle(value: i64) -> i64 => leaf(value)
const fn leaf(value: i64) -> i64 {
    inject logger
    info(logger, "value")
    return value
}
)hgl"};
    require_clean(unit);
    REQUIRE(complete(unit));
    INFO(unit.diagnostics.render(unit.file));
    for (const auto &declaration : unit.hir.declarations) {
        const auto *fn = std::get_if<hir::FunctionDecl>(&declaration.node);
        if (!fn) { continue; }
        REQUIRE(fn->capabilities.size() == 1);
        CHECK(unit.hir.symbol(fn->capabilities.front()).name == "logger");
    }
}

TEST_CASE("native capability requirements survive explicit deduplication and imports", "[ir][typed][capabilities]") {
    Lowered local{R"hgl(module capabilities.native
native const fn leaf(value: i64) -> i64
native const fn leaf(value: i64) -> i64 {
    inject logger
}
fn caller(value: i64) -> i64 {
    inject logger
    when { return leaf(value) }
}
)hgl"};
    require_clean(local);
    REQUIRE(complete(local));
    CHECK(local.hir.native_functions.front().capabilities == std::vector<std::string>{"logger"});
    const auto &fn = std::get<hir::FunctionDecl>(local.hir.declarations.back().node);
    CHECK(fn.capabilities.size() == 1);

    auto catalog = native_catalog({hgl::semantics::NativeCallPhase::Evaluation}, hgl::NativeExecutionRole::Value);
    // Build a separate imported contract with an explicit service requirement.
    hgl::semantics::ImportableModule provider;
    provider.identity        = "helpers";
    auto imported            = *catalog.find_function("acme.stats", "blend");
    imported.module_identity = "helpers";
    imported.identity        = "helpers::blend";
    imported.capabilities    = {"logger"};
    provider.functions.push_back(std::move(imported));
    REQUIRE_FALSE(catalog.add(std::move(provider)));
    Lowered consumer{"module capabilities.consumer\nuse helpers::{blend}\n"
                     "fn caller(value: f64) -> f64 {\n    when { return blend(value, 3) }\n}\n",
                     catalog};
    require_clean(consumer);
    REQUIRE(complete(consumer));
    const auto &caller = std::get<hir::FunctionDecl>(consumer.hir.declarations.back().node);
    REQUIRE(caller.capabilities.size() == 1);
    CHECK(consumer.hir.symbol(caller.capabilities.front()).name == "logger");
}

TEST_CASE("inferred capabilities do not create runtime context at wiring time", "[ir][typed][capabilities]") {
    CHECK(completion_diagnostics(R"hgl(module capabilities.phase
const fn leaf(value: i64) -> i64 {
    inject logger
    info(logger, "value")
    return value
}
const fn middle(value: i64) -> i64 => leaf(value)
fn invalid(value: i64) -> i64 => middle(2)
)hgl")
              .find("not available in this execution phase") != std::string::npos);
    for (const auto name : {"out", "scheduler"}) {
        CHECK(completion_diagnostics(std::string{"module capabilities.owner\nconst fn leaf(value: i64) -> i64 {\n inject "} + name +
                                     "\n return value\n}\n")
                  .find("const fn cannot inject its own") != std::string::npos);
    }
}

TEST_CASE("native implementation contracts match independently of declaration order", "[ir][native][parts]") {
    using Kind = hgl::NativeImplementationKind;
    for (const bool reverse : {false, true}) {
        for (const auto &[body, expected] : std::vector<std::pair<std::string, Kind>>{
                 {"{}", Kind::Graph}, {"{ when; }", Kind::Node}, {"{ inject out, logger\n start; when; stop; }", Kind::Node}}) {
            const std::string signature = "native fn filter(value: i64, const limit: i64) -> i64";
            Lowered           unit{"module contracts\n" +
                                   (reverse ? signature + body + "\n" + signature : signature + "\n" + signature + body) + "\n"};
            require_clean(unit);
            const bool completed = complete(unit);
            INFO(unit.diagnostics.render(unit.file));
            REQUIRE(completed);
            REQUIRE(unit.hir.native_functions.size() == 1);
            const auto &fn = unit.hir.native_functions.front();
            CHECK(fn.execution_role == hgl::NativeExecutionRole::Temporal);
            CHECK(fn.implementation_kind == expected);
            CHECK(fn.candidate_identity == "contracts::filter#0");
            CHECK(fn.phases == std::vector{hir::NativePhase::Wiring});
        }
    }
}

TEST_CASE("native implementation matching rejects changed and duplicate contracts", "[ir][native][parts]") {
    const std::string declaration = "native const fn identity(value: i64) -> i64\n";
    for (const auto implementation :
         {"native const fn identity(value: bool) -> i64 {}", "native const fn identity(value: i64) -> bool {}",
          "native const fn identity(const value: i64) -> i64 {}", "native const fn identity(other: i64) -> i64 {}",
          "native const fn identity(value: i64) -> i64 throws {}", "native fn identity(value: i64) -> i64 { when; }",
          "native const fn identity(value: i64) -> i64 {}\nnative const fn identity(value: i64) -> i64 {}",
          "native const fn identity(value: i64) -> i64"}) {
        Lowered unit{"module contracts\n" + declaration + implementation + "\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        CHECK(unit.diagnostics.has_errors());
    }
    Lowered missing{"module contracts\nnative const fn identity(value: i64) -> i64 {}\n"};
    require_clean(missing);
    CHECK_FALSE(complete(missing));
}

TEST_CASE("native lifecycle and injection requirements respect ownership", "[ir][native][parts]") {
    for (const auto source :
         {"native fn f(value: i64) -> i64 { start; }", "native fn f(value: i64) -> i64 { when; when; }",
          "native const fn f(value: i64) -> i64 { when; }", "native const fn f(value: i64) -> i64 { inject out }",
          "native fn f(value: i64) { inject out\n when; }", "native fn f(value: i64) -> i64 { inject scheduler }"}) {
        Lowered unit{std::string{"module contracts\n"} + source + "\n"};
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("generic native implementation signatures normalize type parameters", "[ir][native][parts]") {
    Lowered unit{R"hgl(module contracts
native const fn identity<T>(value: T) -> T
native const fn identity<U>(value: U) -> U {}
)hgl"};
    require_clean(unit);
    const bool completed = complete(unit);
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(completed);
    CHECK(unit.hir.native_functions.size() == 1);
}

TEST_CASE("native temporal calls wire without borrowing child node services", "[ir][native][parts]") {
    for (const auto body : {"{}", "{ inject out, logger, scheduler\n when; }"}) {
        const std::string declarations =
            std::string{"module contracts\nnative fn f(value: i64) -> i64\n"} + "native fn f(value: i64) -> i64 " + body + "\n";
        Lowered wiring{declarations + "fn caller(value: i64) -> i64 => f(value)\n"};
        require_clean(wiring);
        const bool completed = complete(wiring);
        INFO(wiring.diagnostics.render(wiring.file));
        REQUIRE(completed);
        const auto &caller = std::get<hir::FunctionDecl>(wiring.hir.declarations.back().node);
        CHECK(caller.capabilities.empty());
        CHECK(wiring.hir.expr(caller.concise_body).phase == hir::Phase::Wiring);
        Lowered evaluation{declarations + "fn caller(value: i64) -> i64 { when { return f(value) } }\n"};
        require_clean(evaluation);
        CHECK_FALSE(complete(evaluation));
    }
}

TEST_CASE("native implementation matching preserves generic collection cardinalities", "[ir][native][parts]") {
    Lowered unit{R"hgl(module contracts
native fn identity<T, const N: i64>(value: list<T, N>) -> list<T, N>
native fn identity<U, const M: i64>(value: list<U, M>) -> list<U, M> { when; }
)hgl"};
    require_clean(unit);
    const bool completed = complete(unit);
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(completed);
    CHECK(unit.hir.native_functions.size() == 1);
}

TEST_CASE("native scalar requirements admit exact value signatures and infer results", "[ir][native][constraints]") {
    Lowered lowered{R"(
module checks.native_requirements
native const fn sum(lhs: i64, rhs: i64) -> i64
native const fn sum(lhs: i64, rhs: f64) -> f64
operator add_<L, R, O>(lhs: L, rhs: R) -> O
impl fn add_<L, R, O>(lhs: L, rhs: R) -> O
requires sum(L, R) -> O {
    when { return sum(lhs, rhs) }
}
instantiate add_<i64, i64, i64>, add_<i64, f64, f64>
const fn integer_sum(lhs: i64, rhs: i64) -> i64
requires sum(i64, i64) -> i64 => sum(lhs, rhs)
)"};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    CHECK(std::ranges::any_of(lowered.hir.exprs,
                              [](const auto &expression) { return expression.operation.native_candidates.size() == 2; }));
}

TEST_CASE("native scalar requirements reject absent wrong-result and temporal candidates", "[ir][native][constraints]") {
    for (const auto &signature : {"native const fn sum(lhs: f64, rhs: f64) -> f64",
                                  "native const fn sum(lhs: i64, rhs: i64) -> f64", "native fn sum(lhs: i64, rhs: i64) -> i64"}) {
        Lowered lowered{std::string{"module checks.bad_native_requirement\n"} + signature + R"(
operator add_<L, R, O>(lhs: L, rhs: R) -> O
impl fn add_<L, R, O>(lhs: L, rhs: R) -> O
requires sum(L, R) -> O { when { return sum(lhs, rhs) } }
instantiate add_<i64, i64, i64>
)"};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.render(lowered.file).find("instantiate matches no") != std::string::npos);
    }
}

TEST_CASE("imported native scalar requirements use nominal identity and propagate capabilities", "[ir][native][constraints]") {
    using namespace hgl::semantics;
    ImportableModule provider;
    provider.identity = "checks.values";
    for (auto type : {ImportedScalarType::I64, ImportedScalarType::F64}) {
        provider.functions.push_back(ImportedFunction{
            .module_identity    = provider.identity,
            .name               = "combine",
            .identity           = "checks.values::combine",
            .candidate_identity = type == ImportedScalarType::I64 ? "integer" : "float",
            .cpp_symbol         = type == ImportedScalarType::I64 ? "values::integer" : "values::floating",
            .parameters         = {{"lhs", type, false}, {"rhs", type, false}},
            .result             = type,
            .phases             = {NativeCallPhase::Evaluation},
            .capabilities       = type == ImportedScalarType::I64 ? std::vector<std::string>{} : std::vector<std::string>{"logger"},
            .execution_role     = hgl::NativeExecutionRole::Value,
        });
    }
    ModuleCatalog catalog;
    REQUIRE_FALSE(catalog.add(std::move(provider)));
    Lowered lowered{R"(
module checks.native_import
use checks.values as values
operator pair<T>(lhs: T, rhs: T) -> T
impl fn pair<T>(lhs: T, rhs: T) -> T
requires values::combine(T, T) -> T {
    when { return values::combine(lhs, rhs) }
}
instantiate pair<i64>, pair<f64>
)",
                    catalog};
    require_clean(lowered);
    const bool completed = complete(lowered);
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(completed);
    const auto implementation = std::ranges::find_if(lowered.hir.declarations, [](const auto &declaration) {
        const auto *fn = std::get_if<hir::FunctionDecl>(&declaration.node);
        return fn && fn->visibility == hir::Visibility::Implementation;
    });
    REQUIRE(implementation != lowered.hir.declarations.end());
    CHECK(std::get<hir::FunctionDecl>(implementation->node).capabilities.size() == 1);
}

TEST_CASE("native scalar requirements reject ambiguous exact signatures", "[ir][native][constraints]") {
    Lowered lowered{R"(
module checks.native_ambiguity
native const fn combine(lhs: i64, rhs: i64) -> i64
native const fn combine(left: i64, right: i64) -> i64
fn apply(lhs: i64, rhs: i64) -> i64
requires combine(i64, i64) -> i64 { when { return lhs + rhs } }
fn use_it(lhs: i64, rhs: i64) -> i64 => apply(lhs, rhs)
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("native scalar requirement is ambiguous") != std::string::npos);
}

TEST_CASE("const is admitted as an operator name and const(f) stays the selector", "[ir][typed][mig-009]") {
    // The library spells hgraph's `const` by its own name; a call with one
    // argument naming a function is still the ADR 0008 value-role selector.
    CHECK(completes("module checks.const_name\n"
                    "operator const<T>(const value: T, const delay: duration = 0s) -> T\n"
                    "impl fn const<T>(const value: T, const delay: duration = 0s) -> T {\n"
                    "    inject scheduler\n"
                    "    start { schedule(scheduler, delay) }\n"
                    "    when scheduled() { return value }\n"
                    "}\n"
                    "instantiate const<i64>\n"
                    "operator sink(ts: signal)\n"
                    "impl fn sink(ts: signal) { when { } }\n"
                    "fn c(tick: i64) -> i64 {\n"
                    "    sink(tick)\n"
                    "    const(42)\n"
                    "}\n"
                    "const fn scale(value: f64, factor: f64) -> f64 => value * factor\n"
                    "fn scale(value: f64, factor: f64) -> f64 { when { return value * factor } }\n"
                    "fn selected(value: f64) -> f64 { const(scale)(value, 3.0) }\n"));
    // Without an operator in scope, `const(value)` is still no selector.
    CHECK(completion_diagnostics("module checks.no_const_operator\n"
                                 "operator sink(ts: signal)\n"
                                 "impl fn sink(ts: signal) { when { } }\n"
                                 "fn c(tick: i64) -> i64 {\n"
                                 "    sink(tick)\n"
                                 "    const(42)\n"
                                 "}\n")
              .find("const(function) requires a const fn declaration") != std::string::npos);
    CHECK(completion_diagnostics("module checks.no_const_operator_delay\n"
                                 "operator sink(ts: signal)\n"
                                 "impl fn sink(ts: signal) { when { } }\n"
                                 "fn c(tick: i64) -> i64 {\n"
                                 "    sink(tick)\n"
                                 "    const(42, delay: 1s)\n"
                                 "}\n")
              .find("is a library operator and is not in scope") != std::string::npos);
}

TEST_CASE("typed HIR bounds the statements a generator source may hold", "[ir][typed][adr-0015]") {
    // A bare return finishes the source; a yield sits at statement level in
    // the body, a while block or an if statement.
    CHECK(completes("module checks.generator_forms\n"
                    "fn bounded(const n: i64) -> i64 {\n"
                    "    var i: i64 = 0\n"
                    "    while {\n"
                    "        if i >= n { return }\n"
                    "        yield 1us: i\n"
                    "        i += 1\n"
                    "    }\n"
                    "}\n"
                    "fn branch(const even: bool) -> i64 {\n"
                    "    if even {\n"
                    "        yield 0s: 2\n"
                    "    } else {\n"
                    "        yield 0s: 1\n"
                    "    }\n"
                    "}\n"));
    CHECK(completion_diagnostics("module checks.generator_return\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    yield 0s: value\n"
                                 "    return value\n"
                                 "}\n")
              .find("a generator source returns no value") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_early_return\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    if value < 0 { return value }\n"
                                 "    yield 0s: value\n"
                                 "}\n")
              .find("a generator source returns no value") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_for\n"
                                 "fn g(const values: list<i64>) -> i64 {\n"
                                 "    for value in elements(values) {\n"
                                 "        yield 1us: value\n"
                                 "    }\n"
                                 "}\n")
              .find("'for' is not available in a generator source") != std::string::npos);
    CHECK(completion_diagnostics("module checks.generator_value_yield\n"
                                 "fn g(const value: i64) -> i64 {\n"
                                 "    let picked: i64 = if value > 0 { yield 0s: value\n"
                                 "        value } else { 0 }\n"
                                 "    yield 1us: picked\n"
                                 "}\n")
              .find("cannot sit inside a value") != std::string::npos);
}

TEST_CASE("ordinary value extensions preserve delta identity and lexical access", "[ir][typed][ordinary]") {
    const std::vector<std::string> accepted{
        "const fn value(x: delta<i64>) -> i64 => x\n",
        "const fn value() -> delta<map<i64, i64>> => delta<map<i64, i64>>(upsert: [1: 2])\n",
        "const fn value() -> delta<list<i64, 2>> => delta<list<i64, 2>>(items: [1: 3])\n",
        "const fn value() -> delta<tuple<i64, str>> => delta<tuple<i64, str>>(items: [1: \"yes\"])\n",
        "const fn value() -> delta<set<i64>> => delta<set<i64>>(added: [1], removed: [2])\n",
        "struct Box { amount: i64 }\nconst fn value() -> i64 { var box = Box(amount: 1)\nbox.amount = 2\nreturn box.amount }\n",
        "const fn value() -> i64 { var xs: list<i64> = []\npush(xs, 1)\nreturn len(xs) }\n",
        "fn value(x: i64) -> i64 { when { return delta_value(x) } }\n",
        "fn value(x: i64) -> datetime { inject clock\nwhen { return clock.evaluation_time } }\n",
        "struct Box { amount: i64 }\nfn value(x: i64) { inject global_state\nwhen { var box: Box = get(global_state, \"box\")\nbox.amount += 1 } }\n",
        "fn value(x: i64) { inject global_state\nwhen { var n: i64 = get(global_state, \"n\")\nn += 1\nset(global_state, \"n\", n) } }\n",
        "struct TimedValue<T> { time: datetime\nvalue: delta<T> }\nconst fn value() -> list<TimedValue<i64>> { var xs: list<TimedValue<i64>> = []\npush(xs, TimedValue(time: @2020-01-01T00:00Z, value: 1))\nreturn xs }\n"
    };
    for (const auto &source : accepted) {
        Lowered unit{"module checks.ordinary\n" + source};
        INFO(source);
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        const bool result = complete(unit);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result);
    }
    const std::vector<std::string> rejected{
        "const fn value() -> i64 { let xs = []\nreturn 1 }\n",
        "const fn value(x: delta<list<ref<i64>>>) -> i64 => 1\n",
        "const fn value(x: delta<map<list<i64>, i64>>) -> i64 => 1\n",
        "const fn value() -> delta<map<i64, i64>> => delta<map<i64, i64>>(upsert: [1: 2], remove: [1])\n",
        "const fn value() -> delta<list<i64, 2>> => delta<list<i64, 2>>(items: [2: 3])\n",
        "const fn value() -> i64 { let xs: list<i64> = []\npush(xs, 1)\nreturn 1 }\n",
        "const fn value() -> i64 { var xs: list<i64, 0> = []\npush(xs, 1)\nreturn 1 }\n",
        "fn value(x: i64) -> i64 { when { return delta(x) } }\n",
        "fn value(x: i64) -> datetime { inject clock\nwhen { return clock.now() } }\n",
        "fn value(x: i64) { inject global_state\nwhen { let n = get(global_state, \"n\") } }\n",
        "fn value(x: i64) { inject global_state\nwhen { set(global_state, \"n\", 1)\nset(global_state, \"n\", true) } }\n",
        "struct Box { amount: i64 }\nfn value(x: i64) { inject global_state\nwhen { let box: Box = get(global_state, \"box\")\nbox.amount = 1 } }\n",
        "struct Box { amount: i64 }\nfn value(x: i64) { inject global_state\nwhen { var box: Box = get(global_state, \"box\")\nlet alias = box } }\n",
        "struct Box { amount: i64 }\nfn value(x: i64) { inject global_state\nwhen { let box: Box = get(global_state, \"box\")\nset(global_state, \"box\", Box(amount: 1)) } }\n"
    };
    for (const auto &source : rejected) {
        INFO(source);
        Lowered unit{"module checks.ordinary\n" + source};
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("eval indexing refines contextual nullable immutable locals", "[ir][typed][nullable]") {
    const std::vector<std::string> accepted{
        "assert len(result) == 3\nlet item = result[0]\nassert item != null && item == 0\n",
        "assert [0, _, 2] == result\nassert [] != result\n",
        "let item = result[0]\nassert item == null || item == 0\n",
        "let item = result[0]\nif !(null == item) { let ordinary: i64 = item\nassert ordinary == 0 }\n",
        "let item = result[0]\nlet alias = item\nif item != null { if alias != null { assert alias == item } }\n",
        "let item = result[0]\nif item != null { let copy = item\nassert copy == 0 }\n",
        "let item = result[0]\nif item == null || false {} else { assert item == 0 }\n"
    };
    const std::vector<std::string> rejected{
        "let item = result[0]\nassert item == 0\n",
        "let item = result[0]\nvar copy = item\n",
        "let item: i64 = result[0]\n",
        "let item = result[0]\nlet alias = item\nif item != null { assert alias == 0 }\n",
        "if result[0] != null { assert result[0] == 0 }\n",
        "let item = result[0]\nlet present = item != null\nif present { assert item == 0 }\n",
        "let item = result[0]\nif item == null { assert item == 0 }\n",
        "let item = result[0]\nassert item != null || item == 0\n",
        "let item = result[0]\nif item != null {}\nassert item == 0\n",
        "let item = result[0]\nif item != null { let item = result[1]\nassert item == 0 }\n"
    };
    const auto source = [](const std::string &body) {
        return "module checks.nullable\nfn identity(value: i64) -> i64 => value\n"
               "test check { let result = eval(identity, value: [0, _, 2])\n" + body + "}\n";
    };
    for (const auto &body : accepted) {
        Lowered unit{source(body)};
        INFO(body);
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        const bool result = complete(unit);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result);
    }
    for (const auto &body : rejected) {
        Lowered unit{source(body)};
        INFO(body);
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        CHECK_FALSE(complete(unit));
        CHECK(unit.diagnostics.render(unit.file).find("nullable") != std::string::npos);
    }
}

TEST_CASE("generator final expressions complete without becoming publications", "[ir][typed][generator]") {
    Lowered unit{"module checks.generator_tail\nfn values() -> i64 { inject logger\nyield 0us: 1\ninfo(logger, \"done\")\n}\n"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    const bool result = complete(unit);
    INFO(unit.diagnostics.render(unit.file));
    CHECK(result);
}

TEST_CASE("computed const global keys defer equality to preflight", "[ir][typed][global-state]") {
    Lowered unit{R"(
module checks.computed_keys
fn update(value: i64, const left: str, const right: str) {
    inject global_state
    when {
        var first: list<i64> = get(global_state, left + ".samples")
        var second: list<i64> = get(global_state, right + ".samples")
        push(first, value)
        push(second, value)
    }
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    const bool result = complete(unit);
    INFO(unit.diagnostics.render(unit.file));
    CHECK(result);
}

TEST_CASE("eval checks publication profile boundaries before graph construction", "[ir][typed][harness]") {
    for (const std::string shape : {"atomic<set<list<i64>>>", "ref<i64>", "list<ref<i64>>", "map<list<i64>, i64>", "Node"}) {
        Lowered unit{"module checks.eval_shape\nstruct Node { value: i64\nnext: atomic<Node> = null }\n"
            "fn identity(value: " + shape + ") -> " + shape + " => value\n"
            "test rejected { eval(identity, value: []) }\n"};
        INFO(shape);
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        CHECK_FALSE(complete(unit));
        CHECK(unit.diagnostics.render(unit.file).find("publication profile") != std::string::npos);
    }
    Lowered accepted{"module checks.eval_tuple\nfn identity(value: tuple<f64, f64>) -> tuple<f64, f64> => value\n"
        "test admitted { eval(identity, value: [(1.0, 2.0), _]) }\n"};
    REQUIRE_FALSE(accepted.diagnostics.has_errors());
    const bool result = complete(accepted);
    INFO(accepted.diagnostics.render(accepted.file));
    CHECK(result);
}

TEST_CASE("atomic scalar spellings have one canonical identity", "[ir][typed][atomic]") {
    for (const std::string scalar : {"bool", "i64", "f64", "str", "date", "time", "datetime", "duration",
                                     "civil_datetime", "zoned_datetime", "zoned_time", "timezone"}) {
        Lowered unit{"module checks.atomic_identity\nfn identity(value: atomic<" + scalar + ">) -> " + scalar + " => value\n"};
        INFO(scalar);
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        const bool result = complete(unit);
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE(result);
        for (const auto &declaration : unit.hir.declarations) {
            if (const auto *fn = std::get_if<hir::FunctionDecl>(&declaration.node)) {
                CHECK(fn->signature.parameters.front().type == fn->signature.result);
            }
        }
    }
}

TEST_CASE("finite atomic publications and generic shape arguments are checked", "[ir][typed][atomic]") {
    const std::vector<std::string> accepted{
        "const fn seed() -> atomic<i64> { return 1 }\n",
        "struct Optional { value: i64 = null }\nfn value(x: atomic<Optional>) -> atomic<Optional> => x\ntest accepted { eval(value, []) }\n",
        "struct Box<T> { value: T }\nfn value(x: Box<atomic<i64>>) -> Box<i64> => x\n",
        "struct Box<T> { value: atomic<list<T>> }\nconst fn sample() -> Box<i64> { let value = Box(value: [1, 2])\nreturn value }\n",
        "struct Box<T> { value: atomic<list<T>> }\nconst fn sample() -> Box<i64> { Box<i64>(value: []) }\n",
        "struct Publication<T> { value: delta<T> }\nstruct Batch<T> { values: list<Publication<T>> }\nconst fn value(x: Batch<atomic<list<i64>>>) -> i64 => 1\n",
        "struct Inner { x: i64 }\nstruct Outer { child: atomic<Inner>\nseq: i64 }\nfn identity(v: atomic<Outer>) -> atomic<Outer> => v\ntest nested { eval(identity, [Outer(child: Inner(x: 1), seq: 2)]) }\n",
        "struct Empty {}\nfn identity(x: atomic<Empty>) -> atomic<Empty> => x\ntest empty { eval(identity, [Empty(), _, Empty()]) }\n",
        "struct Pair { values: list<i64>\nlabel: str = \"default\" }\nfn identity(x: atomic<Pair>) -> atomic<Pair> => x\ntest values { eval(identity, [Pair(values: []), _, Pair(values: [1])]) }\n",
        "const fn take<T>(x: T, value: delta<T>) -> T => x\n",
        "struct TimedValue<T> { value: delta<T> }\nconst fn value() -> TimedValue<atomic<list<i64>>> { TimedValue(value: []) }\n"
    };
    for (const auto &source : accepted) {
        Lowered unit{"module checks.atomic_admitted\n" + source};
        INFO(source);
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        const bool result = complete(unit);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result);
    }
    const std::vector<std::string> rejected{
        "fn value(const x: atomic<i64>) -> i64 => 1\n",
        "fn value(const x: tuple<atomic<i64>, i64>) -> i64 => 1\n",
        "const fn seed(value: atomic<i64>) -> i64 => 1\n",
        "fn value(x: atomic<list<atomic<i64>>>) -> i64 => 1\n",
        "struct Box<T> { value: T }\nfn value(x: Box<atomic<list<atomic<i64>>>>) -> i64 => 1\n",
        "struct Box<T> { value: T }\nfn value(x: Box<atomic<list<i64>>>) -> i64 => 1\n",
        "struct Mixed<T> { value: T\npublication: delta<T> }\nfn value(x: Mixed<atomic<list<i64>>>) -> i64 => 1\n",
        "struct Unused<T> {}\nfn value(x: Unused<atomic<list<i64>>>) -> i64 => 1\n",
        "const fn value(x: delta<atomic<set<list<i64>>>>) -> i64 => 1\n",
        "fn pass<T>(x: T) -> T { when { return delta_value(x) } }\nfn bad(x: atomic<set<list<i64>>>) -> atomic<set<list<i64>>> => pass(x)\n",
        "struct Publication<T> { value: delta<T> }\nconst fn value() { let x = Publication(value: [1, 2]) }\n"
    };
    for (const auto &source : rejected) {
        Lowered unit{"module checks.atomic_rejected\n" + source};
        INFO(source);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("recursive generic occurrence validation does not retain provisional successes", "[ir][typed][atomic]") {
    for (const bool reverse : {false, true}) {
        for (const bool ordinary_occurrence : {false, true}) {
            const std::string a = "struct A<T> { b: atomic<B<T>> = null\npublication: delta<T>\n" +
                std::string{ordinary_occurrence ? "ordinary: T\n" : ""} + "}\n";
            const std::string b = "struct B<U> { a: atomic<A<U>> = null }\n";
            Lowered unit{"module checks.recursive_occurrences\n" + (reverse ? b + a : a + b) +
                "fn accept(const b: B<atomic<list<i64>>>) -> i64 => 0\n"};
            const bool valid = !unit.diagnostics.has_errors() && complete(unit);
            INFO(unit.diagnostics.render(unit.file));
            CHECK(valid == !ordinary_occurrence);
        }
    }
}

TEST_CASE("the temporal publication profile admits four leaves recursively", "[ir][typed][temporal]") {
    for (const std::string leaf : {"civil_datetime", "timezone", "zoned_datetime", "zoned_time"}) {
        for (const std::string &shape : {leaf, "atomic<" + leaf + ">", "list<" + leaf + ", 2>",
                                        "atomic<list<" + leaf + ">>", "map<i64, " + leaf + ">"}) {
            Lowered unit{"module checks.temporal_profile\nfn identity(value: " + shape + ") -> " + shape +
                         " => value\ntest admitted { eval(identity, []) }\n"};
            REQUIRE_FALSE(unit.diagnostics.has_errors());
            const bool result = complete(unit);
            INFO(unit.diagnostics.render(unit.file));
            CHECK(result);
        }
    }

}

TEST_CASE("generic delta equality checks its concrete publication origin", "[ir][typed][temporal]") {
    for (const std::string origin : {"i64", "timezone", "list<i64, 2>"}) {
        Lowered unit{"module checks.delta_equality\n"
            "struct Publication<T> { value: delta<T> }\n"
            "const fn equal<T>(a: Publication<T>, b: Publication<T>) -> bool => a.value == b.value\n"
            "const fn check(a: Publication<" + origin + ">, b: Publication<" + origin + ">) -> bool => equal(a, b)\n"};
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        const bool result = complete(unit);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result == (origin != "list<i64, 2>"));
    }
}

TEST_CASE("typed locals fix ordinary and temporal categories", "[ir][typed][locals][category]") {
    const std::vector<std::string> rejected{
        "fn f(x:i64)->i64 {var r:i64=1\n r=x\n return x}",
        "fn f(x:i64)->i64 {var r:i64=x\n r=1\n return x}",
        "fn f(x:i64)->i64 {var r=1\n if true {r=x}\n return x}",
        "fn f(c:bool,x:i64)->i64 {var r=1\n if c {r=2}\n return x}",
        "fn f(c:bool,x:i64)->i64 {var r=x\n if c {r=2}\n return x}",
        "fn f(c:bool,x:i64)->i64 {var r:i64\n if c {r=1}else{r=x}\n r=2\n return r}",
        "fn f(c:bool,x:i64)->i64 {var r:i64\n if c {r=1}else{r=x}\n r+=1\n r=2\n return r}",
        "fn f(const c:bool,x:i64)->i64 {var r:i64\n if c {r=1}else{r=x}\n return r}",
        "fn f(x:i64)->i64 {var r:i64\n r=1\n r=x\n return r}",
        "fn f(x:i64)->i64 {when {let r:atomic<tuple<i64,i64>> = (1,2)\n return x}}",
        "struct Pair { x:i64 }\n fn f(x:Pair)->Pair {var r=x\n r.x=1\n return r}",
    };
    for (const auto &source : rejected) {
        INFO(source);
        Lowered lowered{"module categories\n" + source};
        require_clean(lowered);
        CHECK_FALSE(complete(lowered));
        CHECK(lowered.diagnostics.has_errors());
    }
    const std::vector<std::string> accepted{
        "fn f(x:i64)->i64 {var r:i64=x\n r+=1\n return r}",
        "fn f(x:i64)->i64 {var r:i64\n r=1\n r=2\n return x+r}",
        "fn f(c:bool,x:i64)->i64 {var r:i64\n if c {r=1\n r=2\n r+=1}else{r=x}\n return r}",
        "fn f(c:bool,x:i64)->i64 {if c {var r=1\n r=2\n x+r}else{x}}",
        "fn f(const c:bool,x:i64)->i64 {var r=1\n if c {r=2}\n return x+r}",
        "fn f(x:atomic<tuple<i64,i64>>)->i64 {when {let r=x\n return r[0]}}",
        "fn f(x:i64)->i64 {when {var r=1\n r=x\n return r}}",
    };
    for (const auto &source : accepted) {
        INFO(source);
        Lowered lowered{"module categories\n" + source};
        require_clean(lowered);
        CHECK(complete(lowered));
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK_FALSE(lowered.diagnostics.has_errors());
    }
}

TEST_CASE("declared enums keep nominal identity and checked signed numbering", "[ir][typed][enum]") {
    Lowered unit{R"(module checks.enums
    enum Mode { low = -9223372036854775808, first = -7, next, high = 9223372036854775807, reset = 9 }
    const fn member() -> Mode => Mode::next
    fn identity(value: delta<atomic<Mode>>) -> Mode => value
    test admitted { eval(identity, [_, Mode::low, Mode::next, Mode::high]) }
    )"};
    require_clean(unit);
    const bool valid = complete(unit);
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(valid);
    const hir::EnumDecl *enumeration = nullptr;
    for (const auto &declaration : unit.hir.declarations) {
        if (const auto *item = std::get_if<hir::EnumDecl>(&declaration.node)) { enumeration = item; }
    }
    REQUIRE(enumeration);
    REQUIRE(enumeration->members.size() == 5);
    CHECK(enumeration->members[0].number == INT64_MIN);
    CHECK(enumeration->members[2].number == -6);
    CHECK(enumeration->members[3].number == INT64_MAX);
    CHECK(enumeration->members[4].number == 9);
    bool found = false;
    for (const auto &expression : unit.hir.exprs) {
        if (!expression.constant) { continue; }
        if (const auto *value = std::get_if<hir::EnumValue>(&*expression.constant)) {
            CHECK(value->identity == "checks.enums.Mode");
            found = true;
        }
    }
    CHECK(found);
}

TEST_CASE("enum declarations reject invalid numbering and nominal substitutions", "[ir][typed][enum]") {
    for (const std::string source : {
        "enum E {}",
        "export enum E { a }",
        "use hgraph.std as E\nenum E { a }",
        "enum E { a }\nenum F { a }\nconst fn f() -> bool => E::a == F::a",
        "enum E { a }\nconst fn f() -> bool => E::a == 0",
        "enum E { a, a }",
        "enum E { a = 0, b = 0 }",
        "enum E { a = 1, b, c = 2 }",
        "enum E { a = 9223372036854775808 }",
        "enum E { a = -9223372036854775809 }",
        "enum E { a = 9223372036854775807, b }",
        "enum E { a = 1.0 }",
        "enum E { a }\nconst fn f() -> E => E::missing",
        "enum E { a }\nconst fn f() -> E => a",
        "enum E { a }\nconst fn f() -> E => 0",
        "enum E { a }\nenum F { a }\nconst fn f() -> E => F::a",
        "enum E { a }\nconst fn f() -> i64 => E::a",
        "enum E { a }\nconst fn f() -> E<i64> => E::a"
    }) {
        Lowered unit{"module checks.invalid_enum\n" + source + "\n"};
        INFO(source);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("scalar collection keys require exact types and equality", "[ir][typed][scalar-keys]") {
    for (const std::string source : {
        "const fn f() -> delta<set<f64>> => delta<set<f64>>(added: [0.0, -0.0])",
        "const fn f() -> delta<set<f64>> => delta<set<f64>>(added: [0.0], removed: [-0.0])",
        "const fn f() -> delta<map<f64, i64>> => delta<map<f64, i64>>(upsert: [0.0: 1, -0.0: 2])",
        "const fn f() -> delta<map<f64, i64>> => delta<map<f64, i64>>(upsert: [1: 1])",
        "const fn f() -> delta<set<f64>> => delta<set<f64>>(added: [1])",
        "const fn f() -> delta<set<str>> => delta<set<str>>(added: [\"same\", \"same\"])",
        "const fn f() -> delta<set<datetime>> => delta<set<datetime>>(added: [@2026-01-15T12:30Z, @2026-01-15T13:30+01:00])",
        "enum E { a }\nenum F { a }\nconst fn f() -> delta<map<E, i64>> => delta<map<E, i64>>(upsert: [F::a: 1])",
        "enum E { a }\nconst fn f() -> delta<set<E>> => delta<set<E>>(added: [0])",
        "fn f(key: str) { when { let d = delta<map<str, i64>>(upsert: [key: 1]) } }"
    }) {
        Lowered unit{"module checks.scalar_keys\n" + source + "\n"};
        INFO(source);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
    Lowered accepted{R"(module checks.scalar_keys
    const fn f() -> delta<set<f64>> => delta<set<f64>>(added: [1.000000000000001, 1.000000000000002])
    const fn zones() -> delta<set<timezone>> => delta<set<timezone>>(added: [@[US/Eastern], @[America/New_York]])
    )"};
    require_clean(accepted);
    const bool valid = complete(accepted);
    INFO(accepted.diagnostics.render(accepted.file));
    CHECK(valid);
}

TEST_CASE("ordinary set and map constructors require exact typed items", "[ir][typed][atomic-collections]") {
    for (const std::string source : {
        "const fn f() -> set<str> => set<str>()",
        "const fn f() -> set<str> => set<str>(other: [])",
        "const fn f() -> set<str> => set<str>(items: [], items: [])",
        "const fn f() -> set<f64> => set<f64>(items: [0.0, -0.0])",
        "const fn f() -> map<f64, i64> => map<f64, i64>(items: [0.0: 1, -0.0: 2])",
        "const fn f() -> map<f64, i64> => map<f64, i64>(items: [1: 2])",
        "const fn f() -> map<str, i64> => map<str, i64>(items: [1])",
        "const fn f() -> set<str> => set<str>(items: [\"key\": 1])",
        "const fn f() -> set<list<i64>> => set<list<i64>>(items: [])"
    }) {
        Lowered unit{"module checks.atomic_collections\n" + source + "\n"};
        INFO(source);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
    Lowered accepted{R"(module checks.atomic_collections
const fn dynamic(key: str, value: i64) -> map<str, i64> => map<str, i64>(items: [key: value])
struct Snapshot { tags: set<str> = set<str>(items: []) }
fn value(x: atomic<Snapshot>) -> atomic<Snapshot> => x
test empty { assert eval(value, [Snapshot()]) == [Snapshot(tags: set<str>(items: []))] }
)"};
    require_clean(accepted);
    const bool valid = complete(accepted);
    INFO(accepted.diagnostics.render(accepted.file));
    CHECK(valid);
}

TEST_CASE("sparse key aliases exclude mutable and temporal bindings", "[ir][typed][prepared-keys]") {
    for (const std::string body : {
        "var original: timezone = @[UTC]\nreturn delta<set<timezone>>(added: [original])",
        "var original: timezone = @[UTC]\nlet alias = original\nreturn delta<set<timezone>>(added: [alias])",
        "var original: i64 = 1\nreturn delta<set<i64>>(added: [original])"
    }) {
        const std::string shape = body.find("i64") != std::string::npos ? "i64" : "timezone";
        Lowered unit{"module checks.prepared_keys\nconst fn recipe() -> delta<set<" + shape + ">> {\n" + body + "\n}\n"};
        INFO(body);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.diagnostics.render(unit.file).find("must be constants") != std::string::npos);
    }
}

TEST_CASE("growing list deltas reject malformed index recipes", "[ir][typed][growing-list]") {
    for (const std::string recipe : {
        "delta<list<i64>>(items: [-1: 1])", "delta<list<i64>>(remove: [-1])",
        "delta<list<i64>>(remove: [0, 0])",
        "delta<list<i64>>(upsert: [0: 1])", "delta<list<i64, 2>>(remove: [1])"
    }) {
        const auto result_type = recipe.substr(0, recipe.find('('));
        Lowered unit{"module checks.growing_list\nconst fn value() -> " + result_type + " => " + recipe + "\n"};
        INFO(recipe);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("rolling publications retain exact shapes and reject delta constructors", "[ir][typed][rolling]") {
    for (const std::string source : {
        "const fn bad() -> delta<rolling<i64, 2>> => delta<rolling<i64, 2>>(items: [0: 1])",
        "fn bad(value: rolling<i64, 2, 1>) -> rolling<i64, 2, 2> => value",
        "fn bad(value: rolling<i64, 2us, 1us>) -> rolling<i64, 2, 1> => value",
        "fn bad(value: atomic<rolling<i64, 2>>) -> atomic<rolling<i64, 2>> { when { return delta_value(value) } }"
    }) {
        Lowered unit{"module checks.rolling\n" + source + "\n"};
        INFO(source);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("complete optional snapshots retain required-field and exact-type checks", "[ir][typed][optional]") {
    for (const std::string constructor : {"Record()", "Record(required: null)",
        "Record(required: 1, optional: true)", "Record(required: 1, optional: 1.0)"}) {
        Lowered unit{"module checks.optional\nstruct Record { required: i64\n optional: i64 = null }\n"
            "const fn value() -> Record => " + constructor + "\n"};
        INFO(constructor);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("atomic family admission preserves membership and structural boundaries", "[ir][typed][family]") {
    for (const std::string body : {
        "let bad: Event<str> = First<i64>(value: 1)",
        "let bad: Event<str> = Other(value: \"x\")",
        "let bad = Event<str>(value: \"x\")",
        "eval(structural, [])"}) {
        Lowered unit{"module checks.family\nabstract struct Event<T> { value: T }\n"
            "struct First<T>: Event<T> {}\nstruct Other { value: str }\n"
            "fn structural(x: Event<str>) -> Event<str> => x\ntest rejected { " + body + " }\n"};
        INFO(body);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
    Lowered admitted{R"(
module checks.family_admitted
abstract struct Event<T> { value: T }
abstract struct Middle<T>: Event<list<T>> {}
struct Leaf<T>: Middle<T> {}
fn forward(value: atomic<Event<list<str>>>) -> atomic<Event<list<str>>> => value
test accepted {
    let member: Event<list<str>> = Leaf<str>(value: ["a"])
    eval(forward, [member, _, member])
}
)"};
    REQUIRE_FALSE(admitted.diagnostics.has_errors());
    const bool result = complete(admitted);
    INFO(admitted.diagnostics.render(admitted.file));
    CHECK(result);
}

TEST_CASE("composite keys retain finite complete exact type restrictions", "[ir][typed][composite-keys]") {
    for (const std::string shape : {"list<i64>", "RecursiveKey", "FamilyKey", "ref<i64>"}) {
        Lowered unit{"module checks.keys\nstruct RecursiveKey { next: atomic<RecursiveKey> = null }\n"
            "abstract struct FamilyKey { value: i64 }\nfn forward(x: set<" + shape + ">) -> set<" + shape + "> => x\n"
            "test rejected { eval(forward, []) }\n"};
        INFO(shape);
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        CHECK(unit.diagnostics.has_errors());
    }
    Lowered mismatch{R"(
module checks.nominal_key
struct Key { value: i64 }
struct Other { value: i64 }
const fn bad() -> delta<set<Key>> => delta<set<Key>>(added: [Other(value: 1)])
)"};
    CHECK_FALSE(complete(mismatch));
    CHECK(mismatch.diagnostics.has_errors());
    for (const std::string binding : {"var key = Key(value: 1)", "let key = input"}) {
        Lowered runtime_key{"module checks.dynamic_key\nstruct Key { value: i64 }\n"
            "const fn bad(input: Key) -> delta<set<Key>> { " + binding +
            "\nreturn delta<set<Key>>(added: [key]) }\n"};
        if (!runtime_key.diagnostics.has_errors()) { CHECK_FALSE(complete(runtime_key)); }
        CHECK(runtime_key.diagnostics.has_errors());
    }
}

TEST_CASE("named tests reject early return before nullable refinement", "[ir][typed][negative]") {
    for (const std::string body : {"if item == null { return }", "if item != null {} else { return }"}) {
        Lowered unit{"module checks.test_return\nfn identity(value:i64)->i64=>value\n"
                     "test invalid { let result=eval(identity,value:[0])\nlet item=result[0]\n" + body + "\n}\n"};
        REQUIRE(unit.diagnostics.has_errors());
        CHECK(unit.diagnostics.diagnostics().front().category == hgl::syntax::Category::Phase);
        CHECK(unit.diagnostics.diagnostics().front().code == "test.statement_phase");
    }
}

TEST_CASE("delta diagnostics preserve reduced annotations and precise source locations", "[ir][typed][delta-errors]") {
    struct Case { std::string body; std::string code; std::string location; hgl::syntax::Category category; };
    using hgl::syntax::Category;
    for (const auto &item : std::vector<Case>{
        {"const fn bad(v: delta<signal>) -> i64 => 0", "delta.unsupported_shape", "signal", Category::Shape},
        {"const fn bad(v: delta<list<signal, 2>>) -> i64 => 0", "delta.unsupported_shape", "list<signal, 2>", Category::Shape},
        {"const fn bad() -> i64 { let value = delta<ref<i64>>()\nreturn 0 }", "delta.unsupported_shape", "ref<i64>", Category::Shape},
        {"const fn bad() -> delta<i64> => false", "delta.type_mismatch", "false", Category::Type},
        {"const fn bad() -> delta<set<i64>> => delta<set<i64>>(other: [1])", "delta.argument_name", "other", Category::Name},
        {"const fn bad() -> delta<set<i64>> => delta<set<i64>>(added: [1, 1])", "delta.duplicate_entry", "1", Category::Type},
        {"const fn bad() -> delta<set<i64>> => delta<set<i64>>(removed: [1], added: [1])", "delta.overlap", "1", Category::Type}
    }) {
        Lowered unit{"module checks.delta_errors\n" + item.body + "\n"};
        if (!unit.diagnostics.has_errors()) { CHECK_FALSE(complete(unit)); }
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE(unit.diagnostics.size() == 1);
        const auto &diagnostic = unit.diagnostics.diagnostics().front();
        CHECK(diagnostic.category == item.category);
        CHECK(diagnostic.code == item.code);
        CHECK(unit.file.slice(diagnostic.range) == item.location);
    }
}

TEST_CASE("reduced delta annotations remain diagnostic contexts for lambdas and replacement", "[ir][typed][delta-errors]") {
    for (const std::string shape : {"i64", "atomic<i64>", "rolling<i64, 2>"}) {
        for (const std::string &body : {
            "let callback = fn(value: i64) -> delta<" + shape + "> => false",
            "var value: delta<" + shape + "> = 1\nvalue = false"
        }) {
            Lowered unit{"module checks.delta_contexts\ntest rejected {\n" + body + "\n}\n"};
            require_clean(unit);
            CHECK_FALSE(complete(unit));
            INFO(unit.diagnostics.render(unit.file));
            REQUIRE(unit.diagnostics.size() == 1);
            const auto &diagnostic = unit.diagnostics.diagnostics().front();
            CHECK(diagnostic.code == "delta.type_mismatch");
            CHECK(unit.file.slice(diagnostic.range) == "false");
        }
    }
}

TEST_CASE("delta diagnostic provenance follows fields and contextual list children", "[ir][typed][delta-errors]") {
    for (const std::string source : {
        "struct Box { value: delta<i64> }\ntest rejected { var box = Box(value: 1)\nbox.value = false }",
        "struct Publication<T> { value: delta<T> }\ntest rejected { var box = Publication<i64>(value: 1)\nbox.value = false }",
        "test rejected { let value: delta<atomic<list<i64>>> = [false] }",
        "test rejected { let value: delta<atomic<list<list<i64>>>> = [[false]] }",
        "test rejected { let value: delta<rolling<list<i64>, 2>> = [false] }",
        "struct Box { value: delta<atomic<list<i64>>> }\ntest rejected { let box = Box(value: [false]) }",
        "struct Publication<T> { value: delta<T> }\ntest rejected { let box = Publication<atomic<list<i64>>>(value: [false]) }",
        "test rejected { let value: delta<atomic<tuple<list<i64>, i64>>> = ([false], 1) }"
    }) {
        Lowered unit{"module checks.delta_provenance\n" + source + "\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE(unit.diagnostics.size() == 1);
        const auto &diagnostic = unit.diagnostics.diagnostics().front();
        CHECK(diagnostic.code == "delta.type_mismatch");
        CHECK(unit.file.slice(diagnostic.range) == "false");
    }
}

TEST_CASE("generic delta formation diagnoses the constraining call", "[ir][typed][delta-errors]") {
    Lowered unit{R"(module checks.generic_delta
const fn require_delta<T>(value: T) -> i64 {
    let storage: list<delta<T>> = []
    return len(storage)
}
test rejected {
    let source = delta<map<i64, i64>>()
    require_delta(source)
}
)"};
    require_clean(unit);
    CHECK_FALSE(complete(unit));
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(unit.diagnostics.size() == 1);
    const auto &diagnostic = unit.diagnostics.diagnostics().front();
    CHECK(diagnostic.code == "delta.unsupported_shape");
    CHECK(unit.file.slice(diagnostic.range) == "require_delta(source)");
    CHECK(diagnostic.message.find("delta<") != std::string::npos);
}

TEST_CASE("growing delta formation defers removed modified overlap to publication", "[ir][typed][delta-errors]") {
    Lowered unit{R"(module checks.delta_data
const fn stored() -> delta<list<i64>> => delta<list<i64>>(items: [1: 9], remove: [1])
)"};
    require_clean(unit);
    CHECK(complete(unit));
    INFO(unit.diagnostics.render(unit.file));
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("ordinary nonempty list literals require constant elements", "[ir][typed][list-literal]") {
    for (const std::string body : {
        "let values = [value]",
        "let values: list<i64, 1> = [value]",
        "let values: delta<atomic<list<i64>>> = [value]",
        "let values = [(value, false)]",
        "let values = [[value]]"
    }) {
        Lowered unit{"module checks.list_literal\nfn bad(value: i64) -> i64 { when {\n" + body + "\nreturn value\n} }\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(std::ranges::any_of(unit.diagnostics.diagnostics(), [&](const auto &diagnostic) {
            return diagnostic.category == hgl::syntax::Category::Phase && diagnostic.code.empty() &&
                   diagnostic.message == "a nonempty ordinary list literal requires constant elements";
        }));
    }
}

TEST_CASE("list literal admission preserves constant empty harness and delta construction", "[ir][typed][list-literal]") {
    for (const std::string source : {
        "const fn values() -> list<i64> => [1, 2]",
        "const fn values() -> list<i64, 2> => [1, 2]",
        "const fn values() -> list<timezone> => [@[Europe/London], @[Etc/UTC]]",
        "const fn values() -> list<zoned_datetime> => [@2026-01-15T12:30+00:00[Etc/UTC]]",
        "enum Mode { first, second }\nconst fn values() -> list<Mode, 2> => [Mode::first, Mode::second]",
        "struct Point { x: i64 }\nfn values(value: i64) -> i64 { when { let points = [Point(x: 1)]\nreturn len(points) } }",
        "fn values(value: i64) -> i64 { when { let fixed: list<tuple<i64, bool>, 2> = [(7, false), (8, true)]\nreturn fixed[0][0] } }",
        "fn values(value: i64) -> i64 { when { var items: list<i64> = []\npush(items, value)\nreturn len(items) } }",
        "fn values(value: list<i64, 2>) -> list<i64, 2> { when all_valid(value) { let copy = value\nreturn copy } }",
        "fn values(value: i64) -> list<i64, 2> { when { return delta<list<i64, 2>>(items: [0: value]) } }",
        "const fn countdown(x: i64) -> i64 { if x <= 0 { return 1 }\nreturn countdown(x - 1) }\nconst fn values() -> list<i64> => [countdown(2)]",
        "fn identity(value: i64) -> i64 => value\ntest harness { assert eval(identity, [1, _, 2]) == [1, _, 2] }"
    }) {
        Lowered unit{"module checks.list_controls\n" + source + "\n"};
        require_clean(unit);
        CHECK(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK_FALSE(unit.diagnostics.has_errors());
    }
}

TEST_CASE("value helpers defer list literal admission to relevant argument dependencies", "[ir][typed][list-literal]") {
    const std::string declarations =
        "const fn make(x: i64, unused: i64) -> list<i64> => [x]\n"
        "const fn first(x: i64, unused: i64) -> i64 => x\n"
        "const fn nested(x: i64, unused: i64) -> list<i64> => make(first(x, unused), unused)\n";
    for (const std::string call : {"make(value, 1)", "nested(value, 1)"}) {
        Lowered unit{"module checks.dynamic_helper\n" + declarations +
            "fn bad(value: i64) -> i64 { when { let values = " + call + "\nreturn len(values) } }\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(std::ranges::any_of(unit.diagnostics.diagnostics(), [](const auto &diagnostic) {
            return diagnostic.category == hgl::syntax::Category::Phase && diagnostic.code.empty();
        }));
    }
    for (const std::string call : {"make(1, 2)", "make(1, value)", "nested(1, value)"}) {
        Lowered unit{"module checks.constant_helper\n" + declarations +
            "fn good(value: i64) -> i64 { when { let values = " + call + "\nreturn len(values) } }\n"};
        require_clean(unit);
        CHECK(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK_FALSE(unit.diagnostics.has_errors());
    }
    for (const std::string source : {
        "const fn make() -> list<datetime> { inject clock\nreturn [clock.now] }",
        "const fn stamp() -> datetime { inject clock\nreturn clock.now }\nconst fn make() -> list<datetime> => [stamp()]",
        "const fn make(x: i64) -> list<i64> { var saved = 1\nsaved = x\nreturn [saved] }\nfn bad(value: i64) -> i64 { when { let values = make(value)\nreturn len(values) } }",
        "const fn make(x: i64) -> list<i64> { var saved = 1\nif x > 0 { saved = 2 }\nreturn [saved] }\nfn bad(value: i64) -> i64 { when { let values = make(value)\nreturn len(values) } }",
        "const fn pick(x: i64) -> i64 { if x > 0 { return 1 }\nreturn 2 }\nfn bad(value: i64) -> i64 { when { let values = [pick(value)]\nreturn len(values) } }",
        "const fn make(x: i64) -> list<list<i64>> { var saved: list<i64> = []\nif x > 0 { push(saved, 1) }\nreturn [saved] }\nfn bad(value: i64) -> i64 { when { let values = make(value)\nreturn len(values) } }",
        "struct Pair { first: i64\nsecond: i64 }\nconst fn make(x: i64) -> list<Pair> { var saved = Pair(first: x, second: 1)\nsaved.second = 2\nreturn [saved] }\nfn bad(value: i64) -> i64 { when { let values = make(value)\nreturn len(values) } }",
        "const fn make(x: i64) -> list<i64> { var count = 0\nvar again = true\nwhile again { count += 1\nagain = x > count }\nreturn [count] }\nfn bad(value: i64) -> i64 { when { let values = make(value)\nreturn len(values) } }"
    }) {
        Lowered unit{"module checks.dynamic_recipe\n" + source + "\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(std::ranges::any_of(unit.diagnostics.diagnostics(), [](const auto &diagnostic) {
            return diagnostic.category == hgl::syntax::Category::Phase && diagnostic.code.empty();
        }));
    }
}

TEST_CASE("body absent value declarations do not infer irrelevant list dependencies", "[ir][typed][list-literal]") {
    // Ordinary source functions require a body. Exercise the defensive HIR path
    // by removing a checked source body, as a declaration-only provider would.
    for (const std::string argument : {"1", "value"}) {
        Lowered unit{"module checks.body_absent\nconst fn identity(x: i64) -> i64 => x\n"
            "fn observe(value: i64) -> i64 { when { let result = identity(" + argument + ")\nreturn result } }\n"};
        require_clean(unit);
        REQUIRE(complete(unit));
        for (auto &declaration : unit.hir.declarations) {
            if (auto *fn = std::get_if<hir::FunctionDecl>(&declaration.node); fn && fn->is_const) { fn->concise_body = {}; }
        }
        // Wrap the existing call in an ordinary literal without changing its
        // resolved target/argument facts, then check admission in isolation.
        auto found = std::ranges::find_if(unit.hir.exprs, [](const auto &expr) {
            return std::holds_alternative<hir::Call>(expr.node) && expr.operation.kind == hir::OperationKind::ExactFunction;
        });
        REQUIRE(found != unit.hir.exprs.end());
        const hir::ExprId call{static_cast<std::uint32_t>(found - unit.hir.exprs.begin())};
        unit.hir.exprs.push_back(*found);
        auto &literal = unit.hir.exprs[call.value];
        hir::Sequence sequence;
        sequence.elements.push_back({.value = hir::ExprId{static_cast<std::uint32_t>(unit.hir.exprs.size() - 1)}});
        literal.node = std::move(sequence);
        const std::array<const hir::Expr *, 1> literals{&literal};
        hgl::syntax::DiagnosticSink diagnostics;
        hgl::ir::check_list_literal_admission(unit.hir, literals,
            [&](hir::ExprId id) { return unit.hir.expr(id).phase == hir::Phase::Constant; }, diagnostics);
        INFO(diagnostics.render(unit.file));
        CHECK(diagnostics.has_errors() == (argument == "value"));
    }
}

TEST_CASE("native value list elements require cold arguments and a cold phase contract", "[ir][typed][list-literal]") {
    // A source native value declaration has hook phases, not Wiring.
    for (const std::string argument : {"1", "value"}) {
        Lowered unit{"module checks.native_list\nnative const fn identity(value: i64) -> i64\n"
            "fn observe(value: i64) -> i64 { when { let values = [identity(" + argument + ")]\nreturn len(values) } }\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(unit.diagnostics.has_errors());
    }
    for (const bool wiring : {false, true}) {
        const auto catalog = native_catalog(wiring ? std::vector{hgl::semantics::NativeCallPhase::Wiring, hgl::semantics::NativeCallPhase::Evaluation}
                                                   : std::vector{hgl::semantics::NativeCallPhase::Evaluation}, hgl::NativeExecutionRole::Value);
        for (const std::string argument : {"1.0", "value"}) {
            Lowered unit{"module checks.descriptor_list\nuse acme.stats::{blend}\n"
                "fn observe(value: f64) -> i64 { when { let values = [blend(" + argument + ", 3)]\nreturn len(values) } }\n", catalog};
            require_clean(unit);
            const bool admitted = wiring && argument == "1.0";
            CHECK(complete(unit) == admitted);
            INFO(unit.diagnostics.render(unit.file));
            CHECK(unit.diagnostics.has_errors() != admitted);
        }
    }
}

TEST_CASE("generic list helper coldness is independent of type substitutions", "[ir][typed][list-literal]") {
    for (const std::string argument : {"1", "value"}) {
        Lowered unit{"module checks.generic_list\nconst fn make<T>(x: T, unused: i64) -> list<T> => [x]\n"
            "fn observe(value: i64) -> i64 { when { let floats = make(1.0, value)\n"
            "let ints = make(" + argument + ", value)\nreturn len(floats) + len(ints) } }\n"};
        require_clean(unit);
        CHECK(complete(unit) == (argument == "1"));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(unit.diagnostics.has_errors() == (argument == "value"));
    }
}

TEST_CASE("ignored value arguments retain capability evaluation effects", "[ir][typed][list-literal]") {
    const std::string helpers =
        "const fn stamp() -> datetime { inject clock\nreturn clock.now }\n"
        "const fn first(x: i64, unused: datetime) -> i64 => x\n"
        "const fn defaulted(x: i64, unused: datetime = stamp()) -> i64 => x\n"
        "const fn nested(x: i64) -> i64 => first(x, stamp())\n";
    for (const std::string expression : {"first(1, clock.now)", "first(1, stamp())", "defaulted(1)", "nested(1)"}) {
        Lowered unit{"module checks.ignored_effect\n" + helpers +
            "fn bad(value: i64) -> i64 { inject clock\nwhen { let values = [" + expression + "]\nreturn len(values) } }\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(std::ranges::any_of(unit.diagnostics.diagnostics(), [](const auto &diagnostic) {
            return diagnostic.category == hgl::syntax::Category::Phase && diagnostic.code.empty() &&
                diagnostic.message == "a nonempty ordinary list literal requires constant elements";
        }));
    }
    Lowered control{"module checks.ignored_input\nconst fn first(x: i64, unused: i64) -> i64 => x\n"
        "fn good(value: i64) -> i64 { when { let values = [first(1, value)]\nreturn len(values) } }\n"};
    require_clean(control);
    CHECK(complete(control));
    INFO(control.diagnostics.render(control.file));
    CHECK_FALSE(control.diagnostics.has_errors());
}

TEST_CASE("value call effect admission is not cached from irrelevant argument coldness", "[ir][typed][list-literal]") {
    for (const bool effect_first : {false, true}) {
        const std::string pure = "let admitted = [first(1, value)]\n";
        const std::string effect = "let rejected = [first(1, stamp())]\n";
        Lowered unit{"module checks.effect_cache\nconst fn stamp() -> datetime { inject clock\nreturn clock.now }\n"
            "const fn first(x: i64, unused: datetime) -> i64 => x\n"
            "fn observe(value: datetime) -> i64 { when {\n" + (effect_first ? effect + pure : pure + effect) + "return 1 } }\n"};
        require_clean(unit);
        CHECK_FALSE(complete(unit));
        INFO(unit.diagnostics.render(unit.file));
        CHECK(std::ranges::count_if(unit.diagnostics.diagnostics(), [](const auto &diagnostic) {
            return diagnostic.message == "a nonempty ordinary list literal requires constant elements";
        }) == 1);
    }
}
