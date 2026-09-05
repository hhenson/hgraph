#include "ir/hir_printer.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>

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

        [[nodiscard]] std::optional<hir::SymbolId> referenced_symbol(std::string_view spelling) const {
            for (ast::ExprId expression = 0; expression < ast.exprs.size(); ++expression) {
                const ast::Expr &source  = ast.expr(expression);
                bool             matches = false;
                if (const auto *name = std::get_if<ast::NameRef>(&source.node)) {
                    matches = name->name.text == spelling;
                } else if (const auto *name = std::get_if<ast::QualifiedRef>(&source.node)) {
                    matches = name->name.text == spelling;
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
}  // namespace

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
            if (std::holds_alternative<ast::UseDecl>(lowered.ast.decl(declaration).node)) { continue; }
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

fn unwrap(box: Box<f64>) -> f64 => box.value
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
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    bool exact    = false;
    bool ordered  = false;
    bool distinct = false;
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
        }
    }
    CHECK(exact);
    CHECK(ordered);
    CHECK(distinct);
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
=> value + value

fn apply_double(value: f64) -> f64 => double(value)
)"};
    require_clean(lowered);
    REQUIRE(complete(lowered));

    Lowered rejected{R"(
module checks.operator_constraint_rejected

operator add<T>(lhs: T, rhs: T) -> T
impl fn add(lhs: f64, rhs: f64) -> f64 => lhs + rhs

fn double<T>(value: T) -> T
requires add(T, T) -> T
=> value + value

fn apply_double(value: i64) -> i64 => double(value)
)"};
    require_clean(rejected);
    CHECK_FALSE(complete(rejected));
    CHECK(rejected.diagnostics.render(rejected.file).find("operator requirement has no implementation") != std::string::npos);
    CHECK(rejected.hir.completion == hir::Completion::Resolved);
}

TEST_CASE("operator implementations inherit contract requirements", "[ir][typed][constraints][operators]") {
    Lowered lowered{R"(
module checks.inherited_operator_constraint

operator add<T>(lhs: T, rhs: T) -> T
impl fn add(lhs: f64, rhs: f64) -> f64 => lhs + rhs

operator double<T>(value: T) -> T
requires add(T, T) -> T

impl fn double(value: f64) -> f64 => value + value
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
