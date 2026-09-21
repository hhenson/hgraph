#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/module_catalog.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/type_bridge.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/registry_reset.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
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
            complete(ast, resolved);
        }

        /// The importing form (ADR 0013): the catalog supplies the layouts of
        /// structs this module does not declare.
        Unit(std::string text, const hgl::semantics::ModuleCatalog &catalog) : file{"test.hgl", std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved =
                hgl::semantics::resolve(file, ast, catalog, [](std::string_view) { return true; }, diagnostics);
            if (diagnostics.has_errors()) { return; }
            complete(ast, resolved);
        }

        void complete(const hgl::syntax::ast::Module &ast, const hgl::semantics::ResolvedModule &resolved) {
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

    [[nodiscard]] hgl::semantics::ImportedType symbol(std::string identity) {
        hgl::semantics::ImportedType type;
        type.kind             = hgl::semantics::ImportedTypeKind::Symbol;
        type.nominal_identity = std::move(identity);
        return type;
    }

    /// A module that exports a struct family and a recursive struct, so the
    /// bridge has something to realize that this module does not declare
    /// (ADR 0013 slice 5).
    hgl::semantics::ModuleCatalog exported_shapes(const std::string &module_name = "checks.shapes") {
        hgl::semantics::ModuleCatalog    catalog;
        hgl::semantics::ImportableModule module;
        module.identity = module_name;

        hgl::semantics::ImportedStruct venue;
        venue.module_identity = module.identity;
        venue.name            = "Venue";
        venue.identity        = module_name + ".Venue";
        venue.fields          = {{"code", hgl::semantics::ImportedScalarType::I64, false, false}};

        hgl::semantics::ImportedStruct root;
        root.module_identity = module.identity;
        root.name            = "Root";
        root.identity        = module_name + ".Root";
        root.abstract        = true;
        root.fields          = {{"id", hgl::semantics::ImportedScalarType::I64, false, false},
                                {"venue", symbol(module_name + ".Venue"), false, false}};

        hgl::semantics::ImportedStruct base;
        base.module_identity = module.identity;
        base.name            = "Base";
        base.identity        = module_name + ".Base";
        base.abstract        = true;
        base.parents         = {symbol(module_name + ".Root")};
        base.fields          = {{"at", hgl::semantics::ImportedScalarType::I64, false, false}};

        // A recursive exported struct (ADR 0012): the edge is an `atomic<Node>`
        // boundary, which is how format 6 records it.
        hgl::semantics::ImportedType edge;
        edge.kind     = hgl::semantics::ImportedTypeKind::Atomic;
        edge.children = {symbol(module_name + ".Node")};

        hgl::semantics::ImportedStruct node;
        node.module_identity = module.identity;
        node.name            = "Node";
        node.identity        = module_name + ".Node";
        node.fields          = {{"label", hgl::semantics::ImportedScalarType::I64, false, false},
                                {"next", edge, false, true}};

        module.structs = {std::move(base), std::move(root), std::move(venue), std::move(node)};
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }
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
    reference: ref<map<str, f64>>,
    reference_list: list<ref<f64>, 3>,
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
    CHECK(bridge.schema(unit.parameter("forms", "reference")) ==
          registry.ref(registry.tsd(types.str_type, registry.ts(types.float_type))));
    CHECK(bridge.schema(unit.parameter("forms", "reference_list")) == registry.tsl(registry.ref(registry.ts(types.float_type)), 3));
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
    const std::optional<hgraph::Value> count = bridge.literal(forms.parameters[11].default_value);
    const std::optional<hgraph::Value> delay = bridge.literal(forms.parameters[12].default_value);
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

TEST_CASE("signal materializes the payload-erased hgraph input schema", "[wiring][hgraph-ir][types][signal]") {
    Unit unit{R"(
module checks.signal_type
fn observe(pulse: signal) -> bool => valid(pulse)
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    const auto              pulse = unit.parameter("observe", "pulse");
    CHECK(bridge.schema(pulse) == hgraph::TypeRegistry::instance().signal());
    CHECK(bridge.value(pulse) == nullptr);
    CHECK(unit.diagnostics.render(unit.file).find("input-only observation marker") != std::string::npos);
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

// The size rules are typed HIR completion's (#767 item 2); the bridge's own
// guards are internal assertions it never reaches from a checked module.
TEST_CASE("typed HIR distinguishes empty and invalid fixed extents before materialization", "[wiring][hgraph-ir][types]") {
    SECTION("zero-sized fixed list") {
        Unit unit{R"(
module checks.zero_list
fn consume(values: list<f64, 0>) -> list<f64, 0> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        REQUIRE_FALSE(unit.diagnostics.has_errors());
        hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
        const auto *schema = bridge.schema(unit.parameter("consume", "values"));
        REQUIRE(schema != nullptr);
        CHECK(schema->fixed_size() == 0);
        CHECK_FALSE(schema->is_unbounded_tsl());
    }

    SECTION("tick minimum exceeds capacity") {
        Unit unit{R"(
module checks.tick_window
fn consume(values: rolling<f64, 20, 25>) -> rolling<f64, 20, 25> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.diagnostics.render(unit.file).find("a rolling minimum size must be positive and no larger than the maximum") !=
              std::string::npos);
    }

    SECTION("duration minimum exceeds maximum") {
        Unit unit{R"(
module checks.duration_window
fn consume(values: rolling<f64, 1s, 2s>) -> rolling<f64, 1s, 2s> => values
)"};
        INFO(unit.diagnostics.render(unit.file));
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.diagnostics.render(unit.file).find(
                  "a rolling minimum duration must be non-negative and no longer than the maximum") != std::string::npos);
    }
}

// ADR 0012: a recursive edge is an owner of its target. Structs that reach one
// another through edges register as one batch; an edge that leaves its batch,
// such as one to an abstract parent, owns an already registered schema.
TEST_CASE("recursive structs realize their edges as owners", "[wiring][types][recursive]") {
    Unit unit{R"(
module checks.recursive_bridge

struct Node {
    value: i64
    next: atomic<Node> = null
}

struct A {
    b: atomic<B> = null
}

struct B {
    a: atomic<A> = null
}

struct Tree<T> {
    value: T
    left: atomic<Tree<T>> = null
}

struct Pair<X, Y> {
    first: X
    swapped: atomic<Pair<Y, X>> = null
}

abstract struct Expr {}

struct Add: Expr {
    lhs: atomic<Expr> = null
}

abstract struct Linked {
    next: atomic<Linked> = null
}

struct Item: Linked {
    value: i64
}

struct Holder {
    first: Node
}

fn forms(
    node: atomic<Node>,
    a: atomic<A>,
    tree: atomic<Tree<i64>>,
    pair: atomic<Pair<i64, str>>,
    add: atomic<Add>,
    item: atomic<Item>,
    holder: atomic<Holder>,
    temporal: Node
) -> atomic<Node> => node
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    auto                   &registry = hgraph::TypeRegistry::instance();
    const auto              value    = [&](std::string_view parameter) {
        const hgl::hgraph_ir::Type &boundary = unit.graph.types[unit.parameter("forms", parameter).value];
        const auto                 *meta     = bridge.value(boundary.children.front());
        REQUIRE(meta != nullptr);
        return meta;
    };
    const auto owned = [](const hgraph::ValueTypeMetaData *meta, std::size_t field) {
        REQUIRE(field < meta->field_count);
        REQUIRE(meta->fields[field].type->is_owned());
        return meta->fields[field].type->element_type;
    };

    const auto *node = value("node");
    CHECK(owned(node, 1) == node);
    CHECK(node->is_hashable());
    CHECK(node->is_equatable());
    CHECK(node->is_comparable());

    const auto *a = value("a");
    CHECK(owned(owned(a, 0), 0) == a);

    const auto *tree = value("tree");
    CHECK(tree->bundle_local_name() == "Tree[int]");
    CHECK(owned(tree, 1) == tree);

    const auto *pair    = value("pair");
    const auto *swapped = owned(pair, 1);
    CHECK(pair->bundle_local_name() == "Pair[int, str]");
    CHECK(swapped->bundle_local_name() == "Pair[str, int]");
    CHECK(owned(swapped, 1) == pair);

    const auto *add = value("add");
    CHECK(owned(add, 0)->bundle_local_name() == "Expr");
    CHECK(registry.value_is_a(add, owned(add, 0)));

    const auto *item = value("item");
    CHECK(owned(item, 0) == owned(owned(item, 0), 0));

    CHECK(value("holder")->fields[0].type == node);

    // Temporal Node is a finite bundle: `next` is one endpoint whose value is
    // the owner Node stores, so the bundle's value schema is Node itself.
    const auto *temporal = bridge.schema(unit.parameter("forms", "temporal"));
    REQUIRE(temporal != nullptr);
    REQUIRE(temporal->kind == hgraph::TSTypeKind::TSB);
    CHECK(temporal->fields()[1].type == registry.ts(node->fields[1].type));
    CHECK(temporal->value_schema == node);

    // A second bridge over the same module finds the registered batch.
    hgl::wiring::TypeBridge again{unit.graph, unit.diagnostics};
    CHECK(again.value(unit.graph.types[unit.parameter("forms", "a").value].children.front()) == a);
    CHECK_FALSE(unit.diagnostics.has_errors());

    // Equality and hashing run through the whole depth.
    const auto chain = [&](std::int64_t last) {
        hgraph::Value root{hgraph::ValuePlanFactory::instance().type_for(node)};
        auto          fields = root.as_bundle().begin_mutation();
        fields["value"].set(std::int64_t{1});
        auto second = fields["next"].as_bundle().begin_mutation();
        second["value"].set(std::int64_t{2});
        second["next"].as_bundle().begin_mutation()["value"].set(last);
        return root;
    };
    CHECK(chain(3).view().equals(chain(3).view()));
    CHECK(chain(3).view().hash() == chain(3).view().hash());
    CHECK_FALSE(chain(3).view().equals(chain(4).view()));
}

TEST_CASE("imported structs realize under the owning module's identity", "[wiring][types][struct-imports]") {
    // ADR 0013: the importer re-describes the owner's layout, and the bridge
    // registers it under the OWNER's qualified name. There is no copy in the
    // importer's namespace, so a value built by either module is one schema.
    const hgl::semantics::ModuleCatalog catalog = exported_shapes();
    Unit                                unit{R"(
module checks.import_wiring

use checks.shapes as shapes

struct Tick: shapes::Base
{
    bid: f64
}

fn reading(tick: atomic<Tick>, base: atomic<shapes::Base>, venue: atomic<shapes::Venue>) -> atomic<Tick> => tick
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    auto                   &registry = hgraph::TypeRegistry::instance();
    const auto              types    = hgraph::stdlib::register_standard_types();

    const auto *tick = bridge.value(unit.graph.types[unit.parameter("reading", "tick").value].children.front());
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(tick != nullptr);
    // The local child keeps ITS namespace ...
    CHECK(tick->bundle_namespace() == "checks.import_wiring");
    CHECK(tick->bundle_local_name() == "Tick");
    // ... and carries the whole inherited layout, ancestors first.
    REQUIRE(tick->field_count == 4);
    CHECK(std::string_view{tick->fields[0].name} == "id");
    CHECK(std::string_view{tick->fields[1].name} == "venue");
    CHECK(std::string_view{tick->fields[2].name} == "at");
    CHECK(std::string_view{tick->fields[3].name} == "bid");

    // The imported ancestry registered under checks.shapes, not here: the
    // parents had to be realized before the local child could name them.
    const auto *base = bridge.value(unit.graph.types[unit.parameter("reading", "base").value].children.front());
    REQUIRE(base != nullptr);
    CHECK(base->bundle_namespace() == "checks.shapes");
    CHECK(base->bundle_local_name() == "Base");
    CHECK(base == registry.named_bundle("checks.shapes", "Base"));
    CHECK(registry.named_bundle("checks.import_wiring", "Base") == nullptr);
    CHECK(registry.named_bundle("checks.shapes", "Root") != nullptr);

    // A field that names an imported struct resolves to that same schema.
    const auto *venue = bridge.value(unit.graph.types[unit.parameter("reading", "venue").value].children.front());
    REQUIRE(venue != nullptr);
    CHECK(venue == tick->fields[1].type);
    CHECK(venue->bundle_namespace() == "checks.shapes");
    REQUIRE(venue->field_count == 1);
    CHECK(venue->fields[0].type == types.int_type);
}

TEST_CASE("a recursive imported struct rebuilds its edges", "[wiring][types][struct-imports][recursive]") {
    // Acceptance 3: the closure walks the same edges format 6 records, so an
    // imported recursive struct registers exactly as a local one does.
    const hgl::semantics::ModuleCatalog catalog = exported_shapes();
    Unit                                unit{R"(
module checks.import_recursive

use checks.shapes as shapes

fn walking(node: atomic<shapes::Node>) -> atomic<shapes::Node> => node
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    auto                   &registry = hgraph::TypeRegistry::instance();
    const auto              types    = hgraph::stdlib::register_standard_types();

    const auto *node = bridge.value(unit.graph.types[unit.parameter("walking", "node").value].children.front());
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(node != nullptr);
    CHECK(node->bundle_namespace() == "checks.shapes");
    CHECK(node->bundle_local_name() == "Node");
    REQUIRE(node->field_count == 2);
    CHECK(node->fields[0].type == types.int_type);
    // The edge is an owner of the struct itself, so a value stays a finite tree.
    CHECK(node->fields[1].type == registry.owned(node));
}

TEST_CASE("an imported layout that disagrees with the registered schema is rejected", "[wiring][types][struct-imports]") {
    // Acceptance 4: an importer built against a layout that has since changed
    // must be told so, not silently mismatched. The identity is the owner's,
    // so the two descriptions meet in one registry slot and the disagreement
    // is detectable exactly there.
    auto      &registry = hgraph::TypeRegistry::instance();
    const auto types    = hgraph::stdlib::register_standard_types();
    // Stands in for the exporting module having been built with `code: str`.
    REQUIRE(registry.bundle("checks.skew", "Venue", {{"code", types.str_type}}) != nullptr);

    const hgl::semantics::ModuleCatalog catalog = exported_shapes("checks.skew");
    Unit                                unit{R"(
module checks.import_skew

use checks.skew as shapes

fn reading(venue: atomic<shapes::Venue>) -> atomic<shapes::Venue> => venue
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    CHECK(bridge.value(unit.graph.types[unit.parameter("reading", "venue").value].children.front()) == nullptr);
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    // Pointed: it names the struct, not just "a type mismatch".
    CHECK(rendered.find("Venue") != std::string::npos);
}

TEST_CASE("a recursive imported struct compares against the registered schema before the closure runs",
          "[wiring][types][struct-imports][recursive]") {
    // The recursive path cannot rely on the registry throwing: hgraph's
    // recursive closure answers from the registered type BEFORE it asks the
    // describer, so a skewed layout would be accepted silently. The bridge
    // compares first, which is the only place the two descriptions meet.
    auto      &registry = hgraph::TypeRegistry::instance();
    const auto types    = hgraph::stdlib::register_standard_types();
    REQUIRE(registry.bundle("checks.rskew", "Node", {{"label", types.str_type}}) != nullptr);

    const hgl::semantics::ModuleCatalog catalog = exported_shapes("checks.rskew");
    Unit                                unit{R"(
module checks.import_rskew

use checks.rskew as shapes

fn walking(node: atomic<shapes::Node>) -> atomic<shapes::Node> => node
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    static_cast<void>(bridge.value(unit.graph.types[unit.parameter("walking", "node").value].children.front()));
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("Node") != std::string::npos);
}
