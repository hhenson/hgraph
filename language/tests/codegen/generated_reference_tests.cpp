#include <reference-routing.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace reference_routing = examples::reference_routing;

namespace
{
    struct RouteFirst
    {
        static Port<TS<Float>>
        compose(Wiring &w, Port<TS<Float>> first, Port<TS<Float>> second, Port<TS<Float>> third) {
            auto index = wire<stdlib::const_, TS<Int>>(w, Int{0});
            auto values = stdlib::to_tsl<TSL<TS<Float>, 3>>(w, first, second, third);
            return wire<reference_routing::operators::route3>(w, index, values).as<TS<Float>>();
        }
    };
}  // namespace

TEST_CASE("generated reference operators retain their public schema", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_routing::register_operators();

    CHECK(hgl::wiring::has_operator("examples.reference_routing.forward"));
}

TEST_CASE("generated reference routing follows the selected source", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_routing::register_operators();

    CHECK(eval_node<RouteFirst>(values<Float>(1.0, 2.0), values<Float>(10.0, 20.0), values<Float>(100.0, 200.0)) ==
          values<Float>(1.0, 2.0));
}
