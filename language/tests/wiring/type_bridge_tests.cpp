#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/type_bridge.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace
{
    struct Unit
    {
        hgl::syntax::SourceFile     file;
        hgl::syntax::DiagnosticSink diagnostics{};
        hgl::hgraph_ir::Module      graph{};

        explicit Unit(std::string text) : file{"test.hgl", std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved =
                hgl::semantics::resolve(file, ast, [](std::string_view) { return true; }, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::ir::hir::Module            hir       = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            const hgl::ir::OperatorResolver operators = [](const hgl::ir::hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selected;
                selected.result   = query.expected_result;
                selected.deferred = true;
                return selected;
            };
            if (!hgl::ir::complete_hir(hir, operators, diagnostics)) { return; }
            graph = hgl::hgraph_ir::lower(hir, diagnostics);
        }

        [[nodiscard]] const hgl::hgraph_ir::Callable &callable(std::string_view name) const {
            for (const hgl::hgraph_ir::Callable &candidate : graph.callables) {
                if (candidate.identity.ends_with(name)) { return candidate; }
            }
            throw std::runtime_error{"missing callable"};
        }

        [[nodiscard]] hgl::hgraph_ir::TypeId parameter(std::string_view callable_name, std::string_view parameter_name) const {
            for (const hgl::hgraph_ir::Parameter &candidate : callable(callable_name).parameters) {
                if (candidate.name == parameter_name) { return candidate.type; }
            }
            throw std::runtime_error{"missing parameter"};
        }
    };
}  // namespace

TEST_CASE("hgraph IR types materialize canonical runtime metadata", "[wiring][hgraph-ir][types]") {
    Unit unit{R"(
module checks.type_bridge

abstract struct Base<T> {
    first: T
}

struct Child<U>: Base<U> {
    second: U
}

struct Box<T> {
    value: T
}

struct Wrapper<T> {
    boxed: Box<T>
}

fn forms(
    atomic_child: atomic<Child<f64>>,
    bundle_child: Child<f64>,
    tuple_value: atomic<tuple<i64, f64>>,
    series_list: list<f64, 3>,
    series_set: set<str>,
    series_map: map<str, f64>,
    tick_window: rolling<f64, 20, 5>,
    duration_window: rolling<f64, 2s, 1s>,
    wrapped: atomic<Wrapper<i64>>,
    const count: i64 = 3,
    const delay: duration = 2s
) -> atomic<Child<f64>> => atomic_child
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    auto                   &registry = hgraph::TypeRegistry::instance();
    const auto              types    = hgraph::stdlib::register_standard_types();

    const auto *child = bridge.value(unit.parameter("forms", "bundle_child"));
    REQUIRE(child != nullptr);
    CHECK(child->is_named_bundle());
    CHECK(child->bundle_namespace() == "checks.type_bridge");
    CHECK(child->bundle_local_name() == "Child[float]");
    REQUIRE(child->field_count == 2);
    CHECK(std::string_view{child->fields[0].name} == "first");
    CHECK(child->fields[0].type == types.float_type);
    CHECK(std::string_view{child->fields[1].name} == "second");
    CHECK(child->fields[1].type == types.float_type);

    const hgl::hgraph_ir::Type &atomic_child = unit.graph.types[unit.parameter("forms", "atomic_child").value];
    CHECK(atomic_child.range.end > atomic_child.range.begin);

    CHECK(bridge.schema(unit.parameter("forms", "atomic_child")) == registry.ts(child));
    CHECK(bridge.schema(unit.parameter("forms", "bundle_child")) == registry.tsb(child));
    CHECK(bridge.schema(unit.parameter("forms", "tuple_value")) == registry.ts(registry.tuple({types.int_type, types.float_type})));
    CHECK(bridge.schema(unit.parameter("forms", "series_list")) == registry.tsl(registry.ts(types.float_type), 3));
    CHECK(bridge.schema(unit.parameter("forms", "series_set")) == registry.tss(types.str_type));
    CHECK(bridge.schema(unit.parameter("forms", "series_map")) == registry.tsd(types.str_type, registry.ts(types.float_type)));
    CHECK(bridge.schema(unit.parameter("forms", "tick_window")) == registry.tsw(types.float_type, 20, 5));
    CHECK(bridge.schema(unit.parameter("forms", "duration_window")) ==
          registry.tsw_duration(types.float_type, hgraph::TimeDelta{2'000'000}, hgraph::TimeDelta{1'000'000}));

    const auto *wrapper = bridge.value(unit.graph.types[unit.parameter("forms", "wrapped").value].children.front());
    REQUIRE(wrapper != nullptr);
    REQUIRE(wrapper->field_count == 1);
    const auto *box = wrapper->fields[0].type;
    REQUIRE(box != nullptr);
    CHECK(box->bundle_local_name() == "Box[int]");
    REQUIRE(box->field_count == 1);
    CHECK(box->fields[0].type == types.int_type);

    const auto box_contract     = std::ranges::find_if(unit.graph.structures, [](const hgl::hgraph_ir::StructContract &candidate) {
        return candidate.identity.ends_with(".Box");
    });
    const auto wrapper_contract = std::ranges::find_if(unit.graph.structures, [](const hgl::hgraph_ir::StructContract &candidate) {
        return candidate.identity.ends_with(".Wrapper");
    });
    REQUIRE(box_contract != unit.graph.structures.end());
    REQUIRE(wrapper_contract != unit.graph.structures.end());
    REQUIRE(box_contract->generics.size() == 1);
    REQUIRE(wrapper_contract->generics.size() == 1);
    CHECK(box_contract->generics.front().binding != wrapper_contract->generics.front().binding);

    const hgl::hgraph_ir::Callable    &forms = unit.callable("forms");
    const std::optional<hgraph::Value> count = bridge.literal(forms.parameters[9].default_value);
    const std::optional<hgraph::Value> delay = bridge.literal(forms.parameters[10].default_value);
    REQUIRE(count);
    REQUIRE(delay);
    CHECK(count->view().checked_as<hgraph::Int>() == 3);
    CHECK(delay->view().checked_as<hgraph::TimeDelta>() == hgraph::TimeDelta{2'000'000});
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("const generic runtime identity remains fail closed", "[wiring][hgraph-ir][types]") {
    Unit unit{R"(
module checks.const_generic

struct Vector<T, const size: i64> {
    values: list<T, size>
}

fn consume(value: atomic<Vector<f64, 2>>) -> atomic<Vector<f64, 2>> => value
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge     bridge{unit.graph, unit.diagnostics};
    const hgl::hgraph_ir::Type &atomic = unit.graph.types[unit.parameter("consume", "value").value];
    REQUIRE(atomic.children.size() == 1);
    CHECK(bridge.value(atomic.children.front()) == nullptr);
    CHECK(unit.diagnostics.has_errors());
    CHECK(unit.diagnostics.render(unit.file).find("typed constant Bundle metadata") != std::string::npos);
}

TEST_CASE("hgraph IR type caches follow registry resets", "[wiring][hgraph-ir][types]") {
    Unit unit{R"(
module checks.type_reset
fn consume(values: list<f64, 3>) -> list<f64, 3> => values
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    const auto              parameter = unit.parameter("consume", "values");
    REQUIRE(bridge.schema(parameter) != nullptr);

    hgraph::reset_all_registries();
    const auto refreshed = hgraph::stdlib::register_standard_types();
    CHECK(bridge.schema(parameter) ==
          hgraph::TypeRegistry::instance().tsl(hgraph::TypeRegistry::instance().ts(refreshed.float_type), 3));
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("hgraph IR type materialization rejects unusable fixed extents", "[wiring][hgraph-ir][types]") {
    SECTION("zero-sized fixed list") {
        Unit unit{R"(
module checks.zero_list
fn consume(values: list<f64, 0>) -> list<f64, 0> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
        CHECK(bridge.schema(unit.parameter("consume", "values")) == nullptr);
        CHECK(unit.diagnostics.render(unit.file).find("fixed list size must be positive") != std::string::npos);
    }

    SECTION("tick minimum exceeds capacity") {
        Unit unit{R"(
module checks.tick_window
fn consume(values: rolling<f64, 20, 25>) -> rolling<f64, 20, 25> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
        CHECK(bridge.schema(unit.parameter("consume", "values")) == nullptr);
        CHECK(unit.diagnostics.render(unit.file).find("minimum no larger than it") != std::string::npos);
    }

    SECTION("duration minimum exceeds maximum") {
        Unit unit{R"(
module checks.duration_window
fn consume(values: rolling<f64, 1s, 2s>) -> rolling<f64, 1s, 2s> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
        CHECK(bridge.schema(unit.parameter("consume", "values")) == nullptr);
        CHECK(unit.diagnostics.render(unit.file).find("minimum no larger than it") != std::string::npos);
    }
}
