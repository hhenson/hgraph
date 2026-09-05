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

TEST_CASE("typed HIR selects a source operator implementation", "[ir][typed][operators]") {
    Lowered lowered{R"(
module checks.operators

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
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
        CHECK(expression.operation.candidate.valid());
        CHECK(lowered.hir.symbol(expression.operation.candidate).name == "choose");
        CHECK_FALSE(expression.operation.deferred);
    }
    CHECK(found);
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

TEST_CASE("typed HIR fails closed until callable requirements are evaluated", "[ir][typed][constraints]") {
    Lowered lowered{R"(
module checks.constraints

fn identity<T>(value: T) -> T
requires T in {i64, f64}
=> value
)"};
    require_clean(lowered);
    CHECK_FALSE(complete(lowered));
    CHECK(lowered.diagnostics.render(lowered.file).find("callable 'requires' evaluation is not implemented") != std::string::npos);
    CHECK(lowered.hir.completion == hir::Completion::Resolved);
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
  t0 scalar f64 signal [42..45)
  t1 scalar f64 signal [50..53)
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
