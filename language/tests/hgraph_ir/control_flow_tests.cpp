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

    struct ConditionalSite
    {
        gir::CallableId callable{};
        gir::BlockId    block{};
        gir::ValueId    value{};
        std::size_t     statement_index{};
    };

    ConditionalSite conditional_site(const gir::Module &module) {
        for (std::uint32_t callable_index = 0; callable_index < module.callables.size(); ++callable_index) {
            const gir::Callable &callable = module.callables[callable_index];
            if (!callable.block_body.valid()) { continue; }
            const gir::Block &block = module.blocks.at(callable.block_body.value);
            for (std::size_t index = 0; index < block.statements.size(); ++index) {
                const gir::Statement &statement = module.statements.at(block.statements[index].value);
                const auto           *evaluate  = std::get_if<gir::Evaluate>(&statement.node);
                if (evaluate == nullptr) { continue; }
                if (std::holds_alternative<gir::Conditional>(module.values.at(evaluate->value.value).node)) {
                    return ConditionalSite{gir::CallableId{callable_index}, callable.block_body, evaluate->value, index};
                }
            }
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

TEST_CASE("value-producing temporal conditional analysis retains an omitted else", "[hgraph-ir][control-flow]") {
    Lowered lowered{R"(
module checks.temporal_omitted_else

fn choose(condition: bool, value: i64) -> i64 {
    if condition {
        value + 1
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    REQUIRE(plan.result.valid());
    CHECK(lowered.graph->types[plan.result.value].kind == hir::TypeKind::Scalar);
    CHECK_FALSE(plan.has_otherwise);
    CHECK_FALSE(plan.when_false);
    const auto results = gir::plan_temporal_conditional_results(*lowered.graph, plan, true);
    REQUIRE(results.size() == 1U);
    CHECK(results.front().source == gir::ConditionalResultSource::Expression);
    CHECK(results.front().type == plan.result);
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
    CHECK_FALSE(plan.when_true.falls_through);
    CHECK(plan.when_false->falls_through);
}

TEST_CASE("temporal conditional analysis assigns the callable suffix to its falling branch",
          "[hgraph-ir][control-flow][continuation]") {
    Lowered lowered{R"(
module checks.temporal_early_return

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    }

    let r = y - 1
    return r * 2
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const ConditionalSite site = conditional_site(*lowered.graph);
    REQUIRE(site.callable.valid());
    const gir::Callable &callable = lowered.graph->callables.at(site.callable.value);
    const auto           continuation =
        gir::plan_temporal_continuation(*lowered.graph, site.block, site.statement_index + 1U, callable.result, site.value);
    REQUIRE(continuation.segments.size() == 1U);
    REQUIRE(continuation.segments.front().statements.size() == 2U);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, site.value, continuation);
    CHECK(plan.returns_from_callable);
    CHECK(plan.result == callable.result);
    REQUIRE(plan.when_false);
    CHECK(plan.when_true.returns);
    CHECK_FALSE(plan.when_true.falls_through);
    CHECK_FALSE(plan.when_true.continuation);
    CHECK(plan.when_false->returns);
    CHECK_FALSE(plan.when_false->falls_through);
    REQUIRE(plan.when_false->continuation);
    CHECK(plan.when_false->continuation->segments == continuation.segments);
    CHECK(plan.assigned_outer.empty());

    REQUIRE(plan.when_true.captures.size() == 1U);
    CHECK(lowered.graph->bindings[plan.when_true.captures.front().binding.value].name == "x");
    REQUIRE(plan.when_false->captures.size() == 1U);
    CHECK(lowered.graph->bindings[plan.when_false->captures.front().binding.value].name == "y");
    REQUIRE(plan.captures.size() == 2U);

    const auto results = gir::plan_temporal_conditional_results(*lowered.graph, plan, false);
    REQUIRE(results.size() == 1U);
    CHECK(results.front().source == gir::ConditionalResultSource::FunctionReturn);
    CHECK(results.front().type == callable.result);
}

TEST_CASE("terminal temporal branches retain their definite-assignment locals", "[hgraph-ir][control-flow][continuation]") {
    Lowered lowered{R"(
module checks.temporal_early_return_assignment

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        return x + 1
    }
    result = y - 1
    return result * 2
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const ConditionalSite site     = conditional_site(*lowered.graph);
    const gir::Callable  &callable = lowered.graph->callables.at(site.callable.value);
    const auto            continuation =
        gir::plan_temporal_continuation(*lowered.graph, site.block, site.statement_index + 1U, callable.result, site.value);
    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, site.value, continuation);

    CHECK(plan.returns_from_callable);
    CHECK(plan.assigned_outer.empty());
    REQUIRE(plan.when_false);
    CHECK(plan.when_true.assigned_outer.empty());
    REQUIRE(plan.when_false->assigned_outer.size() == 1U);
    CHECK(lowered.graph->bindings.at(plan.when_false->assigned_outer.front().value).name == "result");
    CHECK(std::ranges::none_of(plan.captures, [&](const gir::ConditionalCapture &capture) {
        return capture.binding == plan.when_false->assigned_outer.front();
    }));
}

TEST_CASE("a tail conditional is excluded from its own continuation", "[hgraph-ir][control-flow][continuation]") {
    Lowered lowered{R"(
module checks.temporal_tail_return

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x
    } else {
        y
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);
    REQUIRE(lowered.graph->callables.size() == 1U);

    const gir::Callable &callable = lowered.graph->callables.front();
    const gir::Block    &block    = lowered.graph->blocks.at(callable.block_body.value);
    REQUIRE(block.tail.valid());
    REQUIRE(std::holds_alternative<gir::Conditional>(lowered.graph->values.at(block.tail.value).node));

    const auto continuation =
        gir::plan_temporal_continuation(*lowered.graph, callable.block_body, block.statements.size(), callable.result, block.tail);
    CHECK(continuation.segments.empty());

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, block.tail, continuation);
    CHECK(plan.returns_from_callable);
    REQUIRE(plan.when_false);
    CHECK_FALSE(plan.when_true.continuation);
    REQUIRE(plan.when_false->continuation);
    CHECK(plan.when_false->continuation->segments.empty());
}

TEST_CASE("nested temporal continuation planning preserves every enclosing suffix", "[hgraph-ir][control-flow][continuation]") {
    Lowered lowered{R"(
module checks.temporal_nested_continuation

fn choose(outer: bool, inner: bool, x: i64, y: i64, z: i64) -> i64 {
    if outer {
        if inner {
            return x
        }
        y + 1
    }
    return z
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const ConditionalSite outer = conditional_site(*lowered.graph);
    REQUIRE(outer.callable.valid());
    const gir::Callable             &callable = lowered.graph->callables.at(outer.callable.value);
    gir::ConditionalContinuationPlan continuation =
        gir::plan_temporal_continuation(*lowered.graph, outer.block, outer.statement_index + 1U, callable.result, outer.value);
    REQUIRE(continuation.segments.size() == 1U);
    REQUIRE(continuation.segments.front().statements.size() == 1U);

    const auto       &outer_value = std::get<gir::Conditional>(lowered.graph->values.at(outer.value.value).node);
    const gir::Block &then_block  = lowered.graph->blocks.at(outer_value.then_block.value);
    REQUIRE_FALSE(then_block.statements.empty());
    const auto &inner_statement = lowered.graph->statements.at(then_block.statements.front().value);
    const auto &inner_evaluate  = std::get<gir::Evaluate>(inner_statement.node);

    continuation = gir::prepend_temporal_continuation(*lowered.graph, outer_value.then_block, 1U, std::move(continuation),
                                                      inner_evaluate.value);
    REQUIRE(continuation.segments.size() == 2U);
    CHECK(continuation.segments.front().statements.empty());
    CHECK(continuation.segments.front().tail == then_block.tail);
    CHECK(continuation.segments.back().statements.size() == 1U);
    CHECK(continuation.result == callable.result);

    const gir::ConditionalPlan inner = gir::analyze_temporal_conditional(*lowered.graph, inner_evaluate.value, continuation);
    CHECK(inner.returns_from_callable);
    REQUIRE(inner.when_false);
    REQUIRE(inner.when_false->continuation);
    CHECK(inner.when_false->continuation->segments.size() == 2U);
}

TEST_CASE("a terminating call argument stops branch capture analysis", "[hgraph-ir][control-flow][continuation]") {
    Lowered lowered{R"(
module checks.temporal_nested_return

fn passthrough(value: i64) -> i64 { value }

fn choose(condition: bool, x: i64, y: i64, unreachable: i64) -> i64 {
    if condition {
        passthrough({
            return x
            y
        })
        return unreachable
    } else {
        return y
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
    CHECK(plan.when_true.returns);
    CHECK_FALSE(plan.when_true.falls_through);
    REQUIRE(plan.when_true.captures.size() == 1U);
    CHECK(lowered.graph->bindings.at(plan.when_true.captures.front().binding.value).name == "x");
    CHECK(std::ranges::none_of(plan.when_true.captures, [&](const gir::ConditionalCapture &capture) {
        return lowered.graph->bindings.at(capture.binding.value).name == "unreachable";
    }));
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

namespace
{
    bool has_message(const hgl::syntax::DiagnosticSink &diagnostics, std::string_view fragment) {
        return std::ranges::any_of(diagnostics.diagnostics(), [&](const hgl::syntax::Diagnostic &diagnostic) {
            return diagnostic.message.find(fragment) != std::string::npos;
        });
    }

    bool has_issue(const std::vector<gir::PlanIssue> &issues, std::string_view fragment) {
        return std::ranges::any_of(issues,
                                   [&](const gir::PlanIssue &issue) { return issue.message.find(fragment) != std::string::npos; });
    }
}  // namespace

TEST_CASE("traversal analysis owns the first-pass loop rules", "[hgraph-ir][control-flow][iteration][rules]") {
    Lowered lowered{R"(
module checks.traversal_rules

fn examine(book: map<str, f64>, samples: list<f64, 3>, const scale: f64) -> f64 {
    var result: f64 = 0.0
    for value in values(book) {
        result = value * scale
    }
    for key in keys(book) {
        let seen = key
    }
    for sample in values(samples, modified) {
        let seen = sample
    }
    return result
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);
    std::vector<const gir::Traversal *> loops;
    for (const gir::Statement &statement : lowered.graph->statements) {
        if (const auto *loop = std::get_if<gir::Traversal>(&statement.node)) { loops.push_back(loop); }
    }
    REQUIRE(loops.size() == 3U);

    const gir::TraversalPlan dynamic = gir::analyze_traversal(*lowered.graph, *loops[0]);
    CHECK(has_issue(dynamic.issues, "assignment escaping a graph 'for' body is not defined yet"));
    CHECK(has_issue(dynamic.issues, "capturing scalar configuration in a dynamic graph 'for' body"));
    for (const gir::PlanIssue &issue : dynamic.issues) { CHECK(issue.context_free); }

    const gir::TraversalPlan keyed = gir::analyze_traversal(*lowered.graph, *loops[1]);
    CHECK(has_issue(keyed.issues, "graph-phase keys(...) traversal is not defined yet"));

    const gir::TraversalPlan predicate = gir::analyze_traversal(*lowered.graph, *loops[2]);
    CHECK(has_issue(predicate.issues, "graph-phase iterator predicates are not defined yet"));

    // Lowering reported every context-free rule once, so `hgl check` already
    // rejects the module before either backend runs.
    CHECK(lowered.diagnostics.has_errors());
    CHECK(has_message(lowered.diagnostics, "assignment escaping a graph 'for' body is not defined yet"));
    CHECK(has_message(lowered.diagnostics, "capturing scalar configuration in a dynamic graph 'for' body"));
    CHECK(has_message(lowered.diagnostics, "graph-phase keys(...) traversal is not defined yet"));
    CHECK(has_message(lowered.diagnostics, "graph-phase iterator predicates are not defined yet"));
}

TEST_CASE("a fixed-list traversal may read scalar configuration", "[hgraph-ir][control-flow][iteration][rules]") {
    Lowered lowered{R"(
module checks.fixed_traversal_rules

use hgraph.std::{null_sink}

fn examine(samples: list<f64, 3>, const scale: f64) {
    for sample in values(samples) {
        null_sink(sample * scale)
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);
    REQUIRE(traversal(*lowered.graph) != nullptr);
    const gir::TraversalPlan plan = gir::analyze_traversal(*lowered.graph, *traversal(*lowered.graph));
    CHECK(plan.issues.empty());
    CHECK_FALSE(lowered.diagnostics.has_errors());
}

TEST_CASE("temporal conditional analysis owns the first-pass branch rules", "[hgraph-ir][control-flow][rules]") {
    SECTION("else-if and scalar captures are context-free") {
        Lowered lowered{R"(
module checks.conditional_rules

use hgraph.std::{null_sink}

fn observe(first: bool, second: bool, value: f64, const offset: f64) {
    if first {
        null_sink(value + offset)
    } else if second {
        null_sink(value)
    }
}
)"};
        REQUIRE(lowered.graph);
        const gir::ConditionalPlan plan = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
        CHECK(has_issue(plan.issues, "temporal 'else if' is not supported"));
        CHECK(has_issue(plan.issues, "capturing scalar configuration in a time-series 'if' branch"));
        for (const gir::PlanIssue &issue : plan.issues) { CHECK(issue.context_free); }
        CHECK(has_message(lowered.diagnostics, "temporal 'else if' is not supported"));
        CHECK(has_message(lowered.diagnostics, "capturing scalar configuration in a time-series 'if' branch"));
    }

    SECTION("a branch return depends on the continuation the backend supplies") {
        Lowered lowered{R"(
module checks.conditional_return_rule

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    }
    return y - 1
}
)"};
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE(lowered.graph);
        // Without a continuation the return cannot terminate the callable, so
        // the plan carries the rule as context-dependent and lowering stays quiet.
        const gir::ConditionalPlan bare = gir::analyze_temporal_conditional(*lowered.graph, conditional_value(*lowered.graph));
        REQUIRE(has_issue(bare.issues, "return from a time-series 'if' branch"));
        for (const gir::PlanIssue &issue : bare.issues) { CHECK_FALSE(issue.context_free); }
        CHECK_FALSE(lowered.diagnostics.has_errors());

        const ConditionalSite site = conditional_site(*lowered.graph);
        REQUIRE(site.value.valid());
        const gir::Callable                   &callable = lowered.graph->callables.at(site.callable.value);
        const gir::ConditionalContinuationPlan continuation =
            gir::plan_temporal_continuation(*lowered.graph, site.block, site.statement_index + 1U, callable.result, site.value);
        const gir::ConditionalPlan planned = gir::analyze_temporal_conditional(*lowered.graph, site.value, continuation);
        CHECK(planned.returns_from_callable);
        CHECK(planned.issues.empty());
    }
}

TEST_CASE("hgraph IR lowering reports the shared first-pass rules once", "[hgraph-ir][rules]") {
    SECTION("an assignment place must be a plain binding") {
        Lowered lowered{R"(
module checks.assignment_rule

struct Quote { bid: f64 }

fn examine(value: f64) -> f64 {
    var quote: Quote = Quote(bid: value)
    quote.bid = value
    value
}
)"};
        CHECK(has_message(lowered.diagnostics, "assignment targets a local in the first pass"));
    }

    SECTION("map(...) with an anonymous function takes temporal map inputs of matching arity") {
        Lowered lowered{R"(
module checks.map_shape_rule

use hgraph.std::{map}

fn examine(prices: map<str, f64>, scale: f64) -> map<str, f64> =>
    map(prices, scale, fn(price) => price * 2.0)
)"};
        CHECK(has_message(lowered.diagnostics, "the first anonymous map slice takes temporal map inputs"));
        CHECK(has_message(lowered.diagnostics, "the map function parameter count must match its mapped inputs"));
    }

    SECTION("clearing an optional field through a sparse delta fails closed") {
        Lowered lowered{R"(
module checks.clear_rule

struct Quote {
    bid: f64
    note: str = null
}

fn examine(value: f64) -> Quote {
    when modified(value) {
        return delta<Quote>(note: null)
    }
}
)"};
        INFO(lowered.diagnostics.render(lowered.file));
        CHECK(has_message(lowered.diagnostics,
                          "clearing an optional struct field needs the distinct public hgraph clear-delta operation"));
    }
}
