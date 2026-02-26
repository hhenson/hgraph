#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/delta_view.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value.h>

#include <cstdint>

namespace {

using hgraph::ViewData;

struct FakeComputedDeltaState {
    hgraph::value::Value delta;
    bool has_delta{false};
};

hgraph::value::View fake_delta_value(const ViewData& vd) {
    const auto* state = static_cast<const FakeComputedDeltaState*>(vd.delta_data);
    if (state == nullptr || !state->delta.has_value()) {
        return {};
    }
    return state->delta.view();
}

bool fake_has_delta(const ViewData& vd) {
    const auto* state = static_cast<const FakeComputedDeltaState*>(vd.delta_data);
    return state != nullptr && state->has_delta;
}

const hgraph::ts_ops k_fake_delta_ops = [] {
    hgraph::ts_ops ops{};
    ops.delta_value = &fake_delta_value;
    ops.has_delta = &fake_has_delta;
    return ops;
}();

}  // namespace

TEST_CASE("DeltaView default is invalid and empty", "[delta_view]") {
    hgraph::DeltaView delta;
    REQUIRE_FALSE(delta.valid());
    REQUIRE(delta.empty());
    REQUIRE(delta.change_count() == 0);
    REQUIRE(delta.schema() == nullptr);
}

TEST_CASE("DeltaView stored backing surfaces scalar payload", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    Value payload(int_meta);
    payload.emplace();
    payload.as<int64_t>() = 42;

    hgraph::DeltaView delta = hgraph::DeltaView::from_stored(payload.view());
    REQUIRE(delta.backing() == hgraph::DeltaView::Backing::STORED);
    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);
    REQUIRE(delta.schema() == int_meta);
    REQUIRE(delta.value().as<int64_t>() == 42);
}

TEST_CASE("DeltaView computed backing respects ts_ops has_delta gate", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    FakeComputedDeltaState state{Value(int_meta), true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 7;

    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    REQUIRE(delta.backing() == hgraph::DeltaView::Backing::COMPUTED);
    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);

    state.has_delta = false;
    REQUIRE(delta.empty());
}
