#include <fixed-list-iteration.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/graph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>

using namespace hgraph;
namespace fixed_iteration = examples::fixed_list_iteration;

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
}  // namespace

TEST_CASE("generated fixed-list graph iteration wires one sink per child", "[codegen][generated][iteration]") {
    for (const bool with_items : {false, true}) {
        const GraphBuilder graph = compose_observer(with_items);
        const auto         sinks = std::ranges::count_if(
            graph.nodes(), [](const NodeBuilder &node) { return node.type().schema()->node_kind == NodeKind::Sink; });
        CHECK(sinks == 3);
    }
}
