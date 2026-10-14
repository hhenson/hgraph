#include <hgl/ordinary_values.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <catch2/catch_test_macros.hpp>

TEST_CASE("delta observations borrow ordinary ticks and retain sampled values", "[delta][lifetime]") {
    using namespace hgraph;
    const auto *shape = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
    TSOutput output{shape};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*shape, TSEndpointSchema::peered(shape))};
    input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    const auto publish = [&](Int number, DateTime time) {
        const Value value{number};
        auto mutation = output.view(time).begin_mutation(time);
        REQUIRE(mutation.copy_value_from(value.view()));
    };
    const auto tick = MIN_ST + MIN_TD;
    publish(7, tick);
    auto ordinary = input.view(nullptr, tick);
    REQUIRE_FALSE(ordinary.delta_is_sampled_rebind());
    hgl::ordinary::DeltaObservation borrowed{ordinary};
    const auto original = ordinary.delta_value();
    CHECK(borrowed.view().data() == original.data());
    hgl::ordinary::DeltaObservation borrowed_alias{borrowed};
    CHECK(borrowed_alias.view().data() == original.data());

    const auto sample = tick + MIN_TD;
    auto sampled = input.view(nullptr, sample);
    sampled.bind_output_sampled(output.view(sample), sample);
    REQUIRE(sampled.delta_is_sampled_rebind());
    auto retained = [&] {
        hgl::ordinary::DeltaObservation first{sampled};
        hgl::ordinary::DeltaObservation alias{first};
        return alias;
    }();
    publish(99, sample + MIN_TD);
    // Computing the view after the move also covers inline scalar ownership.
    CHECK(retained.view().checked_as<Int>() == 7);
}
