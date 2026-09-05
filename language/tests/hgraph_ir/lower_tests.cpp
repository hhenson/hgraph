#include "hgraph_ir/lower.h"
#include "hgraph_ir/printer.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
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
    namespace hir = hgl::ir::hir;

    bool has_operator(std::string_view name) { return name != "map"; }

    struct Lowered
    {
        hgl::syntax::SourceFile               file;
        hgl::syntax::DiagnosticSink           diagnostics{};
        std::optional<hgl::hgraph_ir::Module> graph{};

        explicit Lowered(std::string text, std::string path = "test.hgl") : file{std::move(path), std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved = hgl::semantics::resolve(file, ast, has_operator, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir::Module                     language  = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            const hgl::ir::OperatorResolver operators = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selected;
                selected.result   = query.expected_result;
                selected.deferred = true;
                return selected;
            };
            if (!hgl::ir::complete_hir(language, operators, diagnostics)) { return; }
            graph = hgl::hgraph_ir::lower_interfaces(language, diagnostics);
        }
    };

    const hgl::hgraph_ir::Callable *callable(const hgl::hgraph_ir::Module &module, std::string_view identity) {
        for (const hgl::hgraph_ir::Callable &candidate : module.callables) {
            if (candidate.identity == identity) { return &candidate; }
        }
        return nullptr;
    }
}  // namespace

TEST_CASE("every guide example lowers an hgraph IR interface", "[hgraph-ir][examples]") {
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
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE_FALSE(lowered.diagnostics.has_errors());
        REQUIRE(lowered.graph);
        CHECK(lowered.graph->completion == hgl::hgraph_ir::Completion::Interfaces);
        CHECK_FALSE(lowered.graph->types.empty());
        for (const hgl::hgraph_ir::Callable &function : lowered.graph->callables) {
            CHECK_FALSE(function.identity.empty());
            CHECK(function.result.valid());
            for (const hgl::hgraph_ir::Parameter &parameter : function.parameters) { CHECK(parameter.type.valid()); }
        }
    }
    CHECK(count >= 6);
}

TEST_CASE("hgraph IR separates a contract identity from its native registry key", "[hgraph-ir][operators]") {
    Lowered lowered{R"(
module checks.operator_identity

use hgraph.std::{map}

fn map_values(values: map<str, f64>) -> map<str, f64> =>
    map(values, fn(value) => value)
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    REQUIRE(lowered.graph->operators.size() == 1);
    const hgl::hgraph_ir::OperatorContract &op = lowered.graph->operators.front();
    CHECK(op.imported);
    CHECK(op.identity == "hgraph.std.map");
    CHECK(op.registry_name == "map_");
}

TEST_CASE("hgraph IR classifies runtime nodes and copies capabilities", "[hgraph-ir][runtime]") {
    Lowered lowered{R"(
module checks.runtime

export fn total(value: f64) -> f64 {
    state current: f64 = 0.0
    inject out, logger
    when modified(value) {
        current += value
        out = current
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::Callable *total = callable(*lowered.graph, "checks.runtime.total");
    REQUIRE(total != nullptr);
    CHECK(total->visibility == hgl::hgraph_ir::CallableVisibility::Export);
    CHECK(total->kind == hgl::hgraph_ir::CallableKind::RuntimeNode);
    REQUIRE(total->capabilities.size() == 2);
    CHECK(total->capabilities[0].name == "out");
    CHECK(total->capabilities[0].type == total->result);
    CHECK(total->capabilities[1].name == "logger");
    CHECK(total->capabilities[1].type.valid());
    CHECK(hir::has_effect(total->effects, hir::Effect::WriteState));
    CHECK(hir::has_effect(total->effects, hir::Effect::WriteOutput));
}

TEST_CASE("hgraph IR preserves source implementation candidates", "[hgraph-ir][operators]") {
    Lowered lowered{R"(
module checks.implementation

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    REQUIRE(lowered.graph->operators.size() == 1);
    CHECK(lowered.graph->operators.front().identity == "checks.implementation.choose");
    REQUIRE(lowered.graph->callables.size() == 1);
    const hgl::hgraph_ir::Callable &implementation = lowered.graph->callables.front();
    CHECK(implementation.visibility == hgl::hgraph_ir::CallableVisibility::Implementation);
    CHECK(implementation.operator_identity == "checks.implementation.choose");
    CHECK(implementation.identity == "checks.implementation.choose#2");
}

TEST_CASE("hgraph IR owns symbolic compile-time type arguments", "[hgraph-ir][types]") {
    Lowered lowered{R"(
module checks.const_types

operator recent<T, const N: i64>(values: rolling<T, N>) -> T
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const auto rolling = std::ranges::find_if(lowered.graph->types,
                                              [](const hgl::hgraph_ir::Type &type) { return type.kind == hir::TypeKind::Rolling; });
    REQUIRE(rolling != lowered.graph->types.end());
    REQUIRE(rolling->size.valid());
    const hgl::hgraph_ir::ConstExpr &size = lowered.graph->const_exprs[rolling->size.value];
    CHECK(size.kind == hgl::hgraph_ir::ConstExprKind::Parameter);
    CHECK(size.parameter == "N");
    CHECK(rolling->min_size == rolling->size);
}

TEST_CASE("hgraph IR preserves aggregate and temporal parameter defaults", "[hgraph-ir][defaults]") {
    Lowered lowered{R"(
module checks.defaults

fn configured<const N: i64>(
    value: f64,
    const sizes: list<i64> = [1, 2],
    const width: i64 = N,
    const venue: timezone = @[Europe/London]
) -> f64 => value
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::Callable *configured = callable(*lowered.graph, "checks.defaults.configured");
    REQUIRE(configured != nullptr);
    REQUIRE(configured->parameters.size() == 4);
    const hgl::hgraph_ir::ConstExprId sizes = configured->parameters[1].default_value;
    REQUIRE(sizes.valid());
    const hgl::hgraph_ir::ConstExpr &sequence = lowered.graph->const_exprs[sizes.value];
    CHECK(sequence.kind == hgl::hgraph_ir::ConstExprKind::Sequence);
    CHECK(sequence.elements.size() == 2);
    const hgl::hgraph_ir::ConstExprId width = configured->parameters[2].default_value;
    REQUIRE(width.valid());
    CHECK(lowered.graph->const_exprs[width.value].kind == hgl::hgraph_ir::ConstExprKind::Parameter);
    CHECK(lowered.graph->const_exprs[width.value].parameter == "N");
    const std::string dump = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(dump.find("@[Europe/London]") != std::string::npos);
}

TEST_CASE("hgraph IR lowering rejects unresolved HIR", "[hgraph-ir][completion]") {
    hir::Module                  unresolved;
    hgl::syntax::DiagnosticSink  diagnostics;
    const hgl::hgraph_ir::Module graph = hgl::hgraph_ir::lower_interfaces(unresolved, diagnostics);
    CHECK(diagnostics.has_errors());
    CHECK(graph.completion == hgl::hgraph_ir::Completion::Interfaces);
}
