#include "hgraph_ir/control_flow.h"
#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace
{
    namespace gir = hgl::hgraph_ir;
    namespace hir = hgl::ir::hir;

    struct Lowered
    {
        hgl::syntax::SourceFile     file{"control_flow.hgl", {}};
        hgl::syntax::DiagnosticSink diagnostics{};
        std::optional<gir::Module>  graph{};

        explicit Lowered(std::string source) : file{"control_flow.hgl", std::move(source)} {
            const hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            const auto resolved = hgl::semantics::resolve(file, ast, [](std::string_view) { return true; }, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir::Module module    = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            const auto  operators = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selection;
                selection.result   = query.expected_result;
                selection.deferred = true;
                return selection;
            };
            if (!hgl::ir::complete_hir(module, operators, diagnostics)) { return; }
            graph = gir::lower(module, diagnostics);
        }
    };

    gir::ValueId conditional_value(const gir::Module &module) {
        for (std::uint32_t index = 0; index < module.values.size(); ++index) {
            if (std::holds_alternative<gir::Conditional>(module.values[index].node)) { return gir::ValueId{index}; }
        }
        return {};
    }
}  // namespace

TEST_CASE("temporal conditional analysis produces a stable union capture signature", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_capture

fn choose(condition: bool, x: i64, y: i64, const scale: i64) -> i64 {
    if condition {
        let local = x + scale
        local
    } else {
        y + x
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.when_false);
    REQUIRE(plan.captures.size() == 3);
    CHECK(lowered.graph->bindings[plan.captures[0].binding.value].name == "x");
    CHECK(plan.captures[0].phase == hir::Phase::Wiring);
    CHECK(lowered.graph->bindings[plan.captures[1].binding.value].name == "scale");
    CHECK(plan.captures[1].phase == hir::Phase::Constant);
    CHECK(lowered.graph->bindings[plan.captures[2].binding.value].name == "y");
    CHECK(plan.captures[2].phase == hir::Phase::Wiring);
    CHECK(plan.when_true.captures.size() == 2);
    CHECK(plan.when_false->captures.size() == 2);
}

TEST_CASE("temporal conditional analysis records escaping assignment and return", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_escape

fn choose(condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        result = value
        return result
    } else {
        result = value + 1
    }
    result
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.when_false);
    REQUIRE(plan.when_true.assigned_outer.size() == 1);
    REQUIRE(plan.when_false->assigned_outer.size() == 1);
    CHECK(plan.when_true.assigned_outer.front() == plan.when_false->assigned_outer.front());
    CHECK(plan.when_true.returns);
    CHECK_FALSE(plan.when_false->returns);
}
