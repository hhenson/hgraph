#include <structural-types.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

#include <string_view>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace structural = examples::structural_types;

namespace
{
    using BidDelta =
        Operator<"examples.structural_types.bid_delta", In<"value", TS<Float>>, Out<typename structural::Quote::time_series>>;
}

TEST_CASE("generated structural types preserve nominal metadata", "[codegen][generated][struct]") {
    const auto *instrument = scalar_descriptor<structural::Instrument::value_type>::value_meta();
    const auto *future     = scalar_descriptor<structural::Future::value_type>::value_meta();
    const auto *box        = scalar_descriptor<structural::Box<Float>::value_type>::value_meta();
    const auto *value_box  = scalar_descriptor<structural::ValueBox<Float>::value_type>::value_meta();
    const auto *labeled    = scalar_descriptor<structural::LabeledValue<Float>::value_type>::value_meta();

    REQUIRE(instrument->name() == "examples.structural_types::Instrument");
    REQUIRE(instrument->is_abstract_bundle());
    REQUIRE(future->bundle_hierarchy->parents == std::vector<const ValueTypeMetaData *>{instrument});
    REQUIRE(box->name() == "examples.structural_types::Box[float]");
    REQUIRE(box->bundle_generic_arguments() ==
            std::vector<const ValueTypeMetaData *>{TypeRegistry::instance().value_type("float")});
    REQUIRE(value_box->is_abstract_bundle());
    REQUIRE(labeled->bundle_hierarchy->parents == std::vector<const ValueTypeMetaData *>{value_box});
    REQUIRE(schema_descriptor<structural::Box<Float>::time_series>::ts_meta()->value_schema == box);
}

TEST_CASE("generated structural deltas remain sparse", "[codegen][generated][struct]") {
    hgl::wiring::ensure_session();
    structural::register_operators();

    CHECK_OUTPUT(eval_node<BidDelta>(values<Float>(1.25)), values<Value>(tsb_delta<typename structural::Quote::time_series>(
                                                               Float{1.25}, std::nullopt, std::nullopt, std::nullopt)));
}
