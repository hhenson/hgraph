#include "hgraph_ir/control_flow.h"
#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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

    const gir::Traversal *traversal(const gir::Module &module) {
        for (const gir::Statement &statement : module.statements) {
            if (const auto *result = std::get_if<gir::Traversal>(&statement.node)) { return result; }
        }
        return nullptr;
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
    CHECK(plan.has_otherwise);
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

TEST_CASE("outputless temporal conditional analysis permits an omitted else", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_sink
use hgraph.std::{debug_print}

fn observe(enabled: bool, value: f64) {
    if enabled {
        debug_print("enabled", value)
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.result.valid());
    CHECK(lowered.graph->types[plan.result.value].kind == hir::TypeKind::Void);
    CHECK_FALSE(plan.has_otherwise);
    CHECK_FALSE(plan.when_false);
    REQUIRE(plan.captures.size() == 1U);
    CHECK(lowered.graph->bindings[plan.captures.front().binding.value].name == "value");
}

TEST_CASE("temporal conditional analysis distinguishes else-if from an omitted else", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_else_if
use hgraph.std::{debug_print}

fn observe(first: bool, second: bool, value: f64) {
    if first {
        debug_print("first", value)
    } else if second {
        debug_print("second", value)
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    CHECK(plan.has_otherwise);
    CHECK_FALSE(plan.when_false);
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
    REQUIRE(plan.assigned_outer.size() == 1);
    CHECK(plan.assigned_outer.front() == plan.when_true.assigned_outer.front());
    CHECK(plan.when_true.returns);
    CHECK_FALSE(plan.when_false->returns);
}

TEST_CASE("temporal conditional analysis does not capture a result assigned earlier in its branch", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_reassignment

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        result = x + 1
        result = result * 2
    } else {
        result = y - 1
        result = result * 3
    }
    result
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.when_false);
    REQUIRE(plan.assigned_outer.size() == 1U);
    CHECK(lowered.graph->bindings[plan.assigned_outer.front().value].name == "result");
    CHECK(std::ranges::none_of(
        plan.captures, [&](const gir::ConditionalCapture &capture) { return capture.binding == plan.assigned_outer.front(); }));
}

TEST_CASE("temporal conditional analysis plans an implicit forwarding capture", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_forwarding

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64 = x
    if condition {
        result = y
    }
    result
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.assigned_outer.size() == 1U);
    const gir::BindingId result = plan.assigned_outer.front();
    REQUIRE(plan.captures.size() == 2U);
    CHECK(lowered.graph->bindings[plan.captures.back().binding.value].name == "result");
    CHECK_FALSE(gir::temporal_branch_forwards(plan, plan.when_true, result));
    CHECK(gir::temporal_branch_forwards(plan, plan.when_false.value_or(gir::ConditionalBranchPlan{}), result));
}

TEST_CASE("temporal conditional result planning preserves several escaping bindings", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_results

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    var offset: i64
    if condition {
        result = x
        offset = x + 1
    } else {
        result = y
        offset = y - 1
    }
    result + offset
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan    = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    const auto                 results = gir::plan_temporal_conditional_results(*lowered.graph, plan, false);
    REQUIRE(results.size() == 2U);
    CHECK(results[0].source == gir::ConditionalResultSource::Binding);
    CHECK(results[0].field_name == "result");
    CHECK(lowered.graph->bindings[results[0].binding.value].name == "result");
    CHECK(results[1].field_name == "offset");
    CHECK(lowered.graph->bindings[results[1].binding.value].name == "offset");
}

TEST_CASE("temporal conditional result planning combines an expression with escaping bindings", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_mixed_results

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var offset: i64
    let result = if condition {
        offset = x + 1
        x * 2
    } else {
        offset = y - 1
        y * 3
    }
    result + offset
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan    = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    const auto                 results = gir::plan_temporal_conditional_results(*lowered.graph, plan, true);
    REQUIRE(results.size() == 2U);
    CHECK(results[0].source == gir::ConditionalResultSource::Expression);
    CHECK(results[0].field_name == "value");
    CHECK(results[1].source == gir::ConditionalResultSource::Binding);
    CHECK(results[1].field_name == "offset");
    CHECK(lowered.graph->bindings[results[1].binding.value].name == "offset");
}

TEST_CASE("traversal analysis separates loop locals from escaping control flow", "[hgraph-ir][control-flow][iteration]") {
    Lowered lowered{R"(
module checks.traversal_escape

fn examine(samples: list<f64, 3>) -> f64 {
    var result: f64 = 0.0
    for sample in values(samples) {
        let local = sample
        result = local
        return local
    }
    return result
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);
    REQUIRE(traversal(*lowered.graph) != nullptr);

    const gir::TraversalPlan plan = gir::analyze_traversal(*lowered.graph, *traversal(*lowered.graph));
    REQUIRE(plan.assigned_outer.size() == 1U);
    CHECK(lowered.graph->bindings[plan.assigned_outer.front().value].name == "result");
    CHECK(plan.captures.empty());
    CHECK(plan.returns);
}

TEST_CASE("traversal analysis preserves temporal and scalar capture phases", "[hgraph-ir][control-flow][iteration]") {
    Lowered lowered{R"(
module checks.traversal_capture

fn examine(book: map<str, f64>, offset: f64, const scale: f64) {
    for value in values(book) {
        value + offset * scale
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);
    REQUIRE(traversal(*lowered.graph) != nullptr);

    const gir::TraversalPlan plan = gir::analyze_traversal(*lowered.graph, *traversal(*lowered.graph));
    REQUIRE(plan.captures.size() == 2U);
    CHECK(lowered.graph->bindings[plan.captures[0].binding.value].name == "offset");
    CHECK(plan.captures[0].phase == hir::Phase::Wiring);
    CHECK(lowered.graph->bindings[plan.captures[1].binding.value].name == "scale");
    CHECK(plan.captures[1].phase == hir::Phase::Constant);
    CHECK(plan.assigned_outer.empty());
    CHECK_FALSE(plan.returns);
}
