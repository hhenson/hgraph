#include <reference-routing.h>
#include <reference-identity-equality.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace reference_routing = examples::reference_routing;
namespace reference_identity = examples::reference_identity_equality;

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

TEST_CASE("generated reference identity compares endpoints through public composition", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_identity::register_operators();
    const auto source = values<Int>(7, 8, std::nullopt, 7);
    const auto step = values<Int>(1, 1, 1, 1);
    CHECK(eval_node<reference_identity::public_aliased>(source, step) == values<Bool>(true, true, true, true));
    CHECK(eval_node<reference_identity::public_aliased_unequal>(source, step) == values<Bool>(false, false, false, false));
    CHECK(eval_node<reference_identity::public_distinct>(source, source, step) == values<Bool>(false, false, false, false));
    CHECK(eval_node<reference_identity::public_distinct_unequal>(source, source, step) == values<Bool>(true, true, true, true));
    CHECK(eval_node<reference_identity::generic_aliased>(source, step) == values<Bool>(true, true, true, true));
    const auto floating = values<Float>(7.0, 8.0, std::nullopt, 7.0);
    CHECK(eval_node<reference_identity::generic_distinct>(floating, floating, step) == values<Bool>(false, false, false, false));
}

TEST_CASE("generated composite reference identity preserves distinct endpoints", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_identity::register_operators();
    const auto step = values<Int>(1, 1, 1);
    const auto list = values<Value>(list_delta<TS<Int>>({{0, 7}, {1, 8}}));
    CHECK(eval_node<reference_identity::aliased_list>(list, step) == values<Bool>(true, true, true));
    CHECK(eval_node<reference_identity::distinct_lists>(list, list, step) == values<Bool>(false, false, false));
    using Fields = reference_identity::IdentityFields::time_series;
    const auto bundle = values<Value>(tsb_delta<Fields>(Int{7}, Int{8}));
    CHECK(eval_node<reference_identity::aliased_bundle>(bundle, step) == values<Bool>(true, true, true));
    CHECK(eval_node<reference_identity::distinct_bundles>(bundle, bundle, step) == values<Bool>(false, false, false));
}
