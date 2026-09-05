#include <operators-and-generics.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace generics = examples::operators_and_generics;

TEST_CASE("generated duration windows preserve their concrete period", "[codegen][generated][generic]") {
    using Window       = TSWDuration<Float, 300000000, 300000000>;
    const auto *schema = schema_descriptor<Window>::ts_meta();

    REQUIRE(schema->is_duration_based());
    REQUIRE(schema->time_range() == TimeDelta{300000000});
    REQUIRE(schema->min_time_range() == TimeDelta{300000000});
}

TEST_CASE("generated generic operator implementations register normally", "[codegen][generated][generic]") {
    hgl::wiring::ensure_session();
    const auto provider = generics::register_operators();

    REQUIRE(provider.active());
    REQUIRE(hgl::wiring::has_operator("examples.operators_and_generics.summarize"));
    REQUIRE(hgl::wiring::has_operator("examples.operators_and_generics.summarize_full_window"));
    REQUIRE(hgl::wiring::has_operator("examples.operators_and_generics.summarize_recent"));
}

TEST_CASE("generated generic operator resolves for a concrete fixed window", "[codegen][generated][generic]") {
    hgl::wiring::ensure_session();
    generics::register_operators();

    CHECK_OUTPUT(eval_node<generics::summarize_full_window>(values<Float>(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0,
                                                                          12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0)),
                 values<Float>(none, none, none, none, none, none, none, none, none, none, none, none, none, none, none, none, none,
                               none, none, 10.5));
}

TEST_CASE("generated duration window graphs retain their concrete schema", "[codegen][generated][generic]") {
    hgl::wiring::ensure_session();
    generics::register_operators();

    CHECK_OUTPUT(eval_node<generics::summarize_recent>(values<Float>(1.0, 2.0)), values<Float>(none, none));
}
