#include <dynamic-collection-iteration.h>
#include <fixed-list-iteration.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/graph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string_view>

using namespace hgraph;
namespace fixed_iteration   = examples::fixed_list_iteration;
namespace dynamic_iteration = examples::dynamic_collection_iteration;

namespace
{
    GraphBuilder compose_observer(bool with_items) {
        hgl::wiring::ensure_session();
        Wiring                  w;
        auto                    first  = wire<stdlib::const_, TS<Float>>(w, Float{1.0});
        auto                    second = wire<stdlib::const_, TS<Float>>(w, Float{2.0});
        auto                    third  = wire<stdlib::const_, TS<Float>>(w, Float{3.0});
        Port<TSL<TS<Float>, 3>> samples{stdlib::to_tsl<TSL<TS<Float>, 3>>(w, first, second, third).erased()};
        if (with_items) {
            fixed_iteration::observe_items::compose(w, samples);
        } else {
            fixed_iteration::observe::compose(w, samples);
        }
        return std::move(w).finish();
    }

    GraphBuilder compose_dynamic_map_observer(bool with_items) {
        hgl::wiring::ensure_session();
        Wiring w;
        auto   book = wire<stdlib::const_, TSD<Str, TS<Float>>>(
            w, stdlib::make_map<Str, Float>({{Str{"A"}, Float{1.0}}, {Str{"B"}, Float{2.0}}}));
        auto offset = wire<stdlib::const_, TS<Float>>(w, Float{10.0});
        if (with_items) {
            dynamic_iteration::observe_items::compose(w, book, offset);
        } else {
            dynamic_iteration::observe_values::compose(w, book, offset);
        }
        return std::move(w).finish();
    }

    GraphBuilder compose_dynamic_list_observer(bool with_items) {
        hgl::wiring::ensure_session();
        Wiring w;
        auto   samples = wire<stdlib::const_, TSL<TS<Float>>>(w, stdlib::make_list<Float>({Float{1.0}, Float{2.0}}));
        auto   offset  = wire<stdlib::const_, TS<Float>>(w, Float{10.0});
        if (with_items) {
            dynamic_iteration::observe_list_items::compose(w, samples, offset);
        } else {
            dynamic_iteration::observe_list_elements::compose(w, samples, offset);
        }
        return std::move(w).finish();
    }

    GraphBuilder compose_dynamic_list_capture() {
        hgl::wiring::ensure_session();
        Wiring w;
        auto   samples = wire<stdlib::const_, TSL<TS<Float>>>(w, stdlib::make_list<Float>({Float{1.0}, Float{2.0}}));
        auto   peers   = wire<stdlib::const_, TSL<TS<Float>>>(w, stdlib::make_list<Float>({Float{3.0}, Float{4.0}}));
        dynamic_iteration::observe_list_capture::compose(w, samples, peers);
        return std::move(w).finish();
    }

    std::size_t map_nodes(const GraphBuilder &graph) {
        return std::ranges::count_if(graph.nodes(), [](const NodeBuilder &node) {
            const NodeTypeMetaData *type = node.type().schema();
            return type != nullptr && type->node_kind == NodeKind::Nested && type->display_name != nullptr &&
                   std::string_view{type->display_name} == "map_";
        });
    }
}  // namespace

TEST_CASE("generated fixed-list graph iteration wires one sink per child", "[codegen][generated][iteration]") {
    for (const bool with_items : {false, true}) {
        const GraphBuilder graph = compose_observer(with_items);
        const auto         sinks = std::ranges::count_if(
            graph.nodes(), [](const NodeBuilder &node) { return node.type().schema()->node_kind == NodeKind::Sink; });
        CHECK(sinks == 3);
    }
}

TEST_CASE("generated dynamic graph iteration wires one child-map owner", "[codegen][generated][iteration]") {
    for (const bool with_items : {false, true}) {
        CHECK(map_nodes(compose_dynamic_map_observer(with_items)) == 1U);
        CHECK(map_nodes(compose_dynamic_list_observer(with_items)) == 1U);
    }
    CHECK(map_nodes(compose_dynamic_list_capture()) == 1U);
}
