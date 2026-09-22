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

        // A two-struct closure, so a member OTHER than the root can be the one
        // already registered.
        hgl::semantics::ImportedType to_tail;
        to_tail.kind     = hgl::semantics::ImportedTypeKind::Atomic;
        to_tail.children = {symbol(module_name + ".Tail")};

        hgl::semantics::ImportedType to_head;
        to_head.kind     = hgl::semantics::ImportedTypeKind::Atomic;
        to_head.children = {symbol(module_name + ".Head")};

        hgl::semantics::ImportedStruct head;
        head.module_identity = module.identity;
        head.name            = "Head";
        head.identity        = module_name + ".Head";
        head.fields          = {{"mark", hgl::semantics::ImportedScalarType::I64, false, false},
                                {"tail", to_tail, false, true}};

        hgl::semantics::ImportedStruct tail;
        tail.module_identity = module.identity;
        tail.name            = "Tail";
        tail.identity        = module_name + ".Tail";
        tail.fields          = {{"size", hgl::semantics::ImportedScalarType::I64, false, false},
                                {"head", to_head, false, true}};

        module.structs = {std::move(base),  std::move(root), std::move(venue),
                          std::move(node),  std::move(head), std::move(tail)};
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

# ONLY the local child is wired. Base arrives through its parent, Root through
# Base's, and Venue only through Root's `venue` FIELD -- none is spelled here,
# and a backend cannot register a family whose shape it has part of.
fn reading(tick: atomic<Tick>) -> atomic<Tick> => tick
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
    const auto *base = registry.named_bundle("checks.shapes", "Base");
    REQUIRE(base != nullptr);
    CHECK(base->bundle_namespace() == "checks.shapes");
    CHECK(base->bundle_local_name() == "Base");
    CHECK(registry.named_bundle("checks.import_wiring", "Base") == nullptr);
    // Root is reached only through Base's parent link ...
    CHECK(registry.named_bundle("checks.shapes", "Root") != nullptr);

    // ... and Venue only through Root's FIELD. Nothing here spells either.
    const auto *venue = registry.named_bundle("checks.shapes", "Venue");
    REQUIRE(venue != nullptr);
    CHECK(venue == tick->fields[1].type);
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
    // Reporting is not enough: the backend aborts only on a null result, so
    // handing back the incompatible metadata would let a run continue against
    // the wrong layout.
    CHECK(bridge.value(unit.graph.types[unit.parameter("walking", "node").value].children.front()) == nullptr);
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("Node") != std::string::npos);
}

TEST_CASE("a recursive imported struct whose field TYPE changed is rejected", "[wiring][types][struct-imports][recursive]") {
    // The shape a version bump actually takes: same field names, same arity,
    // one field's type changed. Comparing kind, count and names would call
    // that a match and hand back the other build's schema -- and the recursive
    // closure never asks the describer afterwards, so nothing downstream would
    // notice.
    auto      &registry = hgraph::TypeRegistry::instance();
    const auto types    = hgraph::stdlib::register_standard_types();
    // Stands in for the exporting module having been built with `label: str`.
    REQUIRE(registry.bundle("checks.tskew", "Node", {{"label", types.str_type}, {"next", types.str_type}}) != nullptr);

    const hgl::semantics::ModuleCatalog catalog = exported_shapes("checks.tskew");
    Unit                                unit{R"(
module checks.import_tskew

use checks.tskew as shapes

fn walking(node: atomic<shapes::Node>) -> atomic<shapes::Node> => node
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    // Reporting is not enough: the backend aborts only on a null result, so
    // handing back the incompatible metadata would let a run continue against
    // the wrong layout.
    CHECK(bridge.value(unit.graph.types[unit.parameter("walking", "node").value].children.front()) == nullptr);
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    // Pointed at the field that disagrees, not just at the struct.
    CHECK(rendered.find("field 'label'") != std::string::npos);
}

TEST_CASE("a recursive imported edge that targets a different struct is rejected", "[wiring][types][struct-imports][recursive]") {
    // Names and arity match, and the edge IS an owner of a named bundle -- it
    // just owns the wrong one. That is version skew of exactly the kind the
    // recursive closure cannot catch on its own, because it answers from the
    // registered type before it asks the describer.
    auto      &registry = hgraph::TypeRegistry::instance();
    const auto types    = hgraph::stdlib::register_standard_types();
    const auto *other   = registry.bundle("checks.rtarget", "Other", {{"tag", types.str_type}});
    REQUIRE(other != nullptr);
    REQUIRE(registry.bundle("checks.rtarget", "Node",
                            {{"label", types.int_type}, {"next", registry.owned(other)}}) != nullptr);

    const hgl::semantics::ModuleCatalog catalog = exported_shapes("checks.rtarget");
    Unit                                unit{R"(
module checks.import_rtarget

use checks.rtarget as shapes

fn walking(node: atomic<shapes::Node>) -> atomic<shapes::Node> => node
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    CHECK(bridge.value(unit.graph.types[unit.parameter("walking", "node").value].children.front()) == nullptr);
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("checks.rtarget::Other") != std::string::npos);
}

TEST_CASE("a recursive closure member registered incompatibly is rejected", "[wiring][types][struct-imports][recursive]") {
    // The ROOT agrees and its edge still names the right target, so a
    // root-only check passes -- but the target is already registered with a
    // different layout, and the closure describes only what is missing. `Head`
    // would have been registered against somebody else's `Tail`, with usable
    // metadata and no diagnostic.
    auto      &registry = hgraph::TypeRegistry::instance();
    const auto types    = hgraph::stdlib::register_standard_types();
    const auto *other   = registry.bundle("checks.member", "Other", {{"tag", types.str_type}});
    REQUIRE(other != nullptr);
    // `Tail` is described as {size: i64, head: atomic<Head>}; this is not that.
    REQUIRE(registry.bundle("checks.member", "Tail",
                            {{"size", types.str_type}, {"head", registry.owned(other)}}) != nullptr);

    const hgl::semantics::ModuleCatalog catalog = exported_shapes("checks.member");
    Unit                                unit{R"(
module checks.import_member

use checks.member as shapes

fn walking(head: atomic<shapes::Head>) -> atomic<shapes::Head> => head
)",
                                             catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    CHECK(bridge.value(unit.graph.types[unit.parameter("walking", "head").value].children.front()) == nullptr);
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    // Named at the member that disagrees, not at the root.
    CHECK(rendered.find("Tail") != std::string::npos);
}

TEST_CASE("an imported field naming an absent module is reported at the import", "[wiring][types][struct-imports]") {
    // Whatever crosses a module boundary crosses whole, or is refused by name.
    // A field naming a struct from a module the package target does not supply
    // used to be skipped: `hgl check` finished against a field whose type
    // nothing describes, and direct wiring failed later at an unknown nominal
    // type -- a name the source never mentions.
    hgl::semantics::ModuleCatalog    catalog;
    hgl::semantics::ImportableModule module;
    module.identity = "checks.partial";

    hgl::semantics::ImportedStruct order;
    order.module_identity = module.identity;
    order.name            = "Order";
    order.identity        = "checks.partial.Order";
    // `checks.elsewhere` is never added to the catalog.
    order.fields = {{"id", hgl::semantics::ImportedScalarType::I64, false, false},
                    {"venue", symbol("checks.elsewhere.Venue"), false, false}};

    module.structs = {std::move(order)};
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_partial

use checks.partial as shapes

fn reading(order: atomic<shapes::Order>) -> atomic<shapes::Order> => order
)",
              catalog};
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    // Named at the module that is missing, not at a nominal type downstream.
    CHECK(rendered.find("checks.elsewhere.Venue") != std::string::npos);
    CHECK(rendered.find("package target") != std::string::npos);
}

TEST_CASE("a deep imported struct chain lowers whole, and realization says where its limit is",
          "[wiring][types][struct-imports]") {
    // A descriptor is an input: `A0` holding `A1` holding `A2` ... is as deep
    // as the supplying module chose, and each hop is a SHALLOW type, so the
    // per-type depth budget never fires -- only the number of hops grows.
    //
    // Deep enough to matter: both walks that follow this closure -- the
    // resolver's binding and typed HIR's lowering -- are iterative, so the
    // chain costs heap rather than stack. A per-struct recursion at this depth
    // does not survive a default stack.
    //
    // REALIZING it is a different question, and this case used to leave it
    // unasked: `Unit` stops after hgraph IR, so a green result said nothing
    // about the bridge. The bridge still descends one frame per nominal, and
    // measured against this very chain it died somewhere past ten thousand
    // links -- so it now reports a bound instead, and this asks it to.
    constexpr std::size_t             depth = 20000;
    hgl::semantics::ModuleCatalog     catalog;
    hgl::semantics::ImportableModule  module;
    module.identity = "checks.chain";
    for (std::size_t index = 0; index < depth; ++index) {
        hgl::semantics::ImportedStruct link;
        link.module_identity = module.identity;
        link.name            = "A" + std::to_string(index);
        link.identity        = module.identity + ".A" + std::to_string(index);
        link.fields          = {{"id", hgl::semantics::ImportedScalarType::I64, false, false}};
        if (index + 1 < depth) {
            link.fields.push_back({"next", symbol(module.identity + ".A" + std::to_string(index + 1)), false, false});
        }
        module.structs.push_back(std::move(link));
    }
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_chain

use checks.chain as shapes

fn reading(head: atomic<shapes::A0>) -> atomic<shapes::A0> => head
fn shallow(near: atomic<shapes::A19990>) -> atomic<shapes::A19990> => near
fn temporal(head: shapes::A0) -> shapes::A0 => head
)",
              catalog};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    hgl::wiring::TypeBridge bridge{unit.graph, unit.diagnostics};
    [[maybe_unused]] const auto standard = hgraph::stdlib::register_standard_types();

    // The tail is ten links from the end; the head is 20,000. Realization
    // walks the chain on the heap like the other two, so BOTH come back -- a
    // bound here was the stack's, and a chain this deep is the supplying
    // module's choice to make.
    CHECK(bridge.value(unit.parameter("shallow", "near")) != nullptr);
    const hgraph::ValueTypeMetaData *head = bridge.value(unit.parameter("reading", "head"));
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(head != nullptr);
    CHECK_FALSE(unit.diagnostics.has_errors());
    // Realized whole, not truncated: the head owns the link that names A1.
    REQUIRE(head->is_named_bundle());
    CHECK(head->bundle_local_name() == "A0");
    CHECK(head->field_count == 2);

    // The temporal side walks the same chain on its own -- it asks for the
    // value type first, but that has finished before it reaches a field's
    // schema, so it needs its own worklist rather than riding on that one.
    CHECK(bridge.schema(unit.parameter("temporal", "head")) != nullptr);
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("an imported family is lowered ancestors first however its members are named",
          "[wiring][types][struct-imports]") {
    // The ordering trap. `Holder` names EVERY member of an inheritance chain
    // as a field, ancestor-to-descendant, so lowering queues them all as roots
    // and then takes the deepest first. A single "have we started this one"
    // set cannot tell a root that is merely WAITING from one already lowered,
    // so the deepest descendant skipped each of its parents as "started" and
    // was flattened against an ancestry nothing had described yet.
    //
    // A descendant carries its parents' fields (hgraph's `bundle()` rule), so
    // getting this wrong is not a crash -- it is a struct that silently loses
    // every inherited field.
    constexpr std::size_t            depth = 64;
    hgl::semantics::ModuleCatalog    catalog;
    hgl::semantics::ImportableModule module;
    module.identity = "checks.family";
    for (std::size_t index = 0; index < depth; ++index) {
        const std::string              suffix = std::to_string(index);
        hgl::semantics::ImportedStruct link;
        link.module_identity = module.identity;
        link.name            = "A" + suffix;
        link.identity        = module.identity + ".A" + suffix;
        link.fields          = {{"f" + suffix, hgl::semantics::ImportedScalarType::I64, false, false}};
        if (index > 0) { link.parents = {symbol(module.identity + ".A" + std::to_string(index - 1))}; }
        module.structs.push_back(std::move(link));
    }
    hgl::semantics::ImportedStruct holder;
    holder.module_identity = module.identity;
    holder.name            = "Holder";
    holder.identity        = module.identity + ".Holder";
    // Ancestor-to-descendant: the driver queues roots in this order and pops
    // the LAST one first, so the deepest descendant is lowered while every
    // one of its parents is queued-but-not-lowered.
    for (std::size_t index = 0; index < depth; ++index) {
        holder.fields.push_back(
            {"m" + std::to_string(index), symbol(module.identity + ".A" + std::to_string(index)), false, false});
    }
    module.structs.push_back(std::move(holder));
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_family

use checks.family as shapes

fn reading(held: atomic<shapes::Holder>) -> atomic<shapes::Holder> => held
fn inherited(held: atomic<shapes::Holder>) -> i64 => held.m63.f0
)",
              catalog};
    INFO(unit.diagnostics.render(unit.file));
    // `f0` is declared by the chain's ROOT and read off its deepest
    // descendant: it is only there if every ancestor was described before the
    // descendant that flattens it.
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("an imported layout cycle through ordinary fields is rejected", "[wiring][types][struct-imports]") {
    // Two records are enough. A cycle through fields that are not recursive
    // edges is not a layout, it is an infinite value -- the local rule rejects
    // one (ADR 0012 rule 2: an edge must be an optional `atomic`), and an
    // imported layout is not exempt because another module wrote it. Realizing
    // it recurses `register_value(A) -> value(B) -> register_value(A)` and
    // takes the process with it.
    hgl::semantics::ModuleCatalog    catalog;
    hgl::semantics::ImportableModule module;
    module.identity = "checks.cycle";

    hgl::semantics::ImportedStruct a;
    a.module_identity = module.identity;
    a.name            = "A";
    a.identity        = "checks.cycle.A";
    a.fields          = {{"b", symbol("checks.cycle.B"), false, /*recursive=*/false}};

    hgl::semantics::ImportedStruct b;
    b.module_identity = module.identity;
    b.name            = "B";
    b.identity        = "checks.cycle.B";
    b.fields          = {{"a", symbol("checks.cycle.A"), false, /*recursive=*/false}};

    module.structs = {std::move(a), std::move(b)};
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_cycle

use checks.cycle as shapes

fn reading(a: atomic<shapes::A>) -> atomic<shapes::A> => a
)",
              catalog};
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("layout cycle") != std::string::npos);
}

TEST_CASE("an imported cycle through an edge and inheritance is rejected", "[wiring][types][struct-imports]") {
    // `Base { child: atomic<Leaf> }` with `Leaf: Base`. The edge alone is
    // bounded (ADR 0012), and the parent link alone is not a cycle -- together
    // they are one, and the local resolver rejects it. Removing owned edges
    // before looking for cycles saw only `Leaf -> Base` and missed it, leaving
    // direct wiring to go round `recursive_value` and `value` until the stack
    // was gone.
    hgl::semantics::ModuleCatalog    catalog;
    hgl::semantics::ImportableModule module;
    module.identity = "checks.mixed";

    hgl::semantics::ImportedType edge;
    edge.kind     = hgl::semantics::ImportedTypeKind::Atomic;
    edge.children = {symbol("checks.mixed.Leaf")};

    hgl::semantics::ImportedStruct base;
    base.module_identity = module.identity;
    base.name            = "Base";
    base.identity        = "checks.mixed.Base";
    base.abstract        = true;
    base.fields          = {{"child", edge, true, /*recursive=*/true}};

    hgl::semantics::ImportedStruct leaf;
    leaf.module_identity = module.identity;
    leaf.name            = "Leaf";
    leaf.identity        = "checks.mixed.Leaf";
    leaf.parents         = {symbol("checks.mixed.Base")};
    leaf.fields          = {{"child", edge, true, /*recursive=*/true},
                            {"code", hgl::semantics::ImportedScalarType::I64, false, false}};

    module.structs = {std::move(base), std::move(leaf)};
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_mixed

use checks.mixed as shapes

fn reading(l: atomic<shapes::Leaf>) -> atomic<shapes::Leaf> => l
)",
              catalog};
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("layout cycle") != std::string::npos);
}

TEST_CASE("an ordinary link inside an otherwise-owned component is still rejected",
          "[wiring][types][struct-imports]") {
    // `A -owned-> C`, `A -> B`, `C -owned-> B`, `B -owned-> A`. Judged by back
    // edge, the all-owned path closes `B` first and the ordinary `A -> B` then
    // looks at a finished node and says nothing -- so the answer depended on
    // field order. Judged per component, the ordinary link is inside a cyclic
    // one and the layout is unbounded however the fields are ordered.
    hgl::semantics::ModuleCatalog    catalog;
    hgl::semantics::ImportableModule module;
    module.identity = "checks.scc";

    const auto edge_to = [](const std::string &identity) {
        hgl::semantics::ImportedType type;
        type.kind     = hgl::semantics::ImportedTypeKind::Atomic;
        type.children = {symbol(identity)};
        return type;
    };

    hgl::semantics::ImportedStruct a;
    a.module_identity = module.identity;
    a.name            = "A";
    a.identity        = "checks.scc.A";
    // The OWNED edge first, so the all-owned path is explored first.
    a.fields = {{"c", edge_to("checks.scc.C"), true, /*recursive=*/true},
                {"b", symbol("checks.scc.B"), false, /*recursive=*/false}};

    hgl::semantics::ImportedStruct b;
    b.module_identity = module.identity;
    b.name            = "B";
    b.identity        = "checks.scc.B";
    b.fields          = {{"a", edge_to("checks.scc.A"), true, /*recursive=*/true}};

    hgl::semantics::ImportedStruct c;
    c.module_identity = module.identity;
    c.name            = "C";
    c.identity        = "checks.scc.C";
    c.fields          = {{"b", edge_to("checks.scc.B"), true, /*recursive=*/true}};

    module.structs = {std::move(a), std::move(b), std::move(c)};
    REQUIRE_FALSE(catalog.add(std::move(module)));

    Unit unit{R"(
module checks.import_scc

use checks.scc as shapes

fn reading(a: atomic<shapes::A>) -> atomic<shapes::A> => a
)",
              catalog};
    REQUIRE(unit.diagnostics.has_errors());
    const std::string rendered = unit.diagnostics.render(unit.file);
    INFO(rendered);
    CHECK(rendered.find("layout cycle") != std::string::npos);
}

