#include "ir/hir_printer.h"
#include "ir/lower.h"
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
