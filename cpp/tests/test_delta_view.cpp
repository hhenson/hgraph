#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/delta_view.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <chrono>

namespace {

using hgraph::ViewData;

struct FakeComputedDeltaState {
    hgraph::value::Value delta;
    bool has_delta{false};
    hgraph::engine_time_t expected_time{hgraph::MIN_DT};
    bool require_engine_time{false};
    mutable size_t delta_value_calls{0};
};

hgraph::value::View fake_delta_value(const ViewData& vd) {
    const auto* state = static_cast<const FakeComputedDeltaState*>(vd.delta_data);
    if (state == nullptr || !state->delta.has_value()) {
        return {};
    }
    state->delta_value_calls += 1;
    if (state->require_engine_time) {
        if (vd.engine_time_ptr == nullptr || *vd.engine_time_ptr != state->expected_time) {
            return {};
        }
    }
    return state->delta.view();
}

bool fake_has_delta(const ViewData& vd) {
    const auto* state = static_cast<const FakeComputedDeltaState*>(vd.delta_data);
    if (state == nullptr || !state->has_delta) {
        return false;
    }
    if (state->require_engine_time) {
        return vd.engine_time_ptr != nullptr && *vd.engine_time_ptr == state->expected_time;
    }
    return true;
}

nb::object fake_delta_to_python(const ViewData& vd, hgraph::engine_time_t current_time) {
    const auto* state = static_cast<const FakeComputedDeltaState*>(vd.delta_data);
    if (state == nullptr) {
        return nb::none();
    }
    if (state->require_engine_time) {
        if (vd.engine_time_ptr == nullptr || *vd.engine_time_ptr != state->expected_time) {
            return nb::none();
        }
    }
    if (state->expected_time != hgraph::MIN_DT && current_time != state->expected_time) {
        return nb::none();
    }
    return nb::int_(123);
}

const hgraph::ts_ops k_fake_delta_ops = [] {
    hgraph::ts_ops ops{};
    ops.delta_value = &fake_delta_value;
    ops.has_delta = &fake_has_delta;
    ops.delta_to_python = &fake_delta_to_python;
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

    const hgraph::engine_time_t current_time{std::chrono::microseconds{10}};
    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, current_time);
    REQUIRE(delta.backing() == hgraph::DeltaView::Backing::COMPUTED);
    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);

    state.has_delta = false;
    REQUIRE(delta.empty());
}

TEST_CASE("DeltaView computed backing uses explicit current_time when engine_time_ptr is absent", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    const hgraph::engine_time_t expected_time{std::chrono::microseconds{42}};
    FakeComputedDeltaState state{Value(int_meta), true, expected_time, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 99;

    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = nullptr;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, expected_time);
    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);
    REQUIRE(delta.value().as<int64_t>() == 99);
}

TEST_CASE("DeltaView computed backing freezes current_time snapshot from engine_time_ptr", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    const hgraph::engine_time_t expected_time{std::chrono::microseconds{88}};
    FakeComputedDeltaState state{Value(int_meta), true, expected_time, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 123;

    hgraph::engine_time_t mutable_time = expected_time;
    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = &mutable_time;

    // Deliberately omit explicit current_time so DeltaView must snapshot
    // the source pointer value at construction time.
    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    mutable_time = hgraph::engine_time_t{std::chrono::microseconds{99}};

    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);
    REQUIRE(delta.value().as<int64_t>() == 123);
}

TEST_CASE("DeltaView computed backing freezes MIN_DT snapshot from engine_time_ptr", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    FakeComputedDeltaState state{Value(int_meta), true, hgraph::MIN_DT, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 456;

    hgraph::engine_time_t mutable_time = hgraph::MIN_DT;
    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = &mutable_time;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    mutable_time = hgraph::engine_time_t{std::chrono::microseconds{1}};

    REQUIRE(delta.valid());
    REQUIRE_FALSE(delta.empty());
    REQUIRE(delta.change_count() == 1);
    REQUIRE(delta.value().as<int64_t>() == 456);
}

TEST_CASE("DeltaView strict semantics honor has_delta at MIN_DT", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    FakeComputedDeltaState state{Value(int_meta), false, hgraph::MIN_DT, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 17;

    hgraph::engine_time_t now = hgraph::MIN_DT;
    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = &now;
    vd.delta_semantics = hgraph::DeltaSemantics::Strict;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    REQUIRE_FALSE(delta.valid());
    REQUIRE(state.delta_value_calls == 0);
}

TEST_CASE("DeltaView allow-pre-tick semantics bypass has_delta at MIN_DT", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    FakeComputedDeltaState state{Value(int_meta), false, hgraph::MIN_DT, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 19;

    hgraph::engine_time_t now = hgraph::MIN_DT;
    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = &now;
    vd.delta_semantics = hgraph::DeltaSemantics::AllowPreTickDelta;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    REQUIRE(delta.valid());
    REQUIRE(delta.value().as<int64_t>() == 19);
    REQUIRE(state.delta_value_calls == 1);
}

TEST_CASE("DeltaView computed backing materializes payload on first value() call", "[delta_view]") {
    using namespace hgraph::value;

    const TypeMeta* int_meta = scalar_type_meta<int64_t>();
    FakeComputedDeltaState state{Value(int_meta), true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 7;

    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, hgraph::MIN_DT);
    REQUIRE(delta.value().as<int64_t>() == 7);
    REQUIRE(state.delta_value_calls == 1);

    // Mutate backing storage after first read: DeltaView should keep the
    // materialized snapshot for subsequent value() calls.
    state.delta.as<int64_t>() = 11;
    REQUIRE(delta.value().as<int64_t>() == 7);
    REQUIRE(state.delta_value_calls == 1);
}

TEST_CASE("DeltaView computed to_python uses explicit current_time when engine_time_ptr is absent", "[delta_view]") {
    const hgraph::engine_time_t expected_time{std::chrono::microseconds{77}};
    FakeComputedDeltaState state{hgraph::value::Value(hgraph::value::scalar_type_meta<int64_t>()), true, expected_time, true};
    state.delta.emplace();
    state.delta.as<int64_t>() = 1;

    hgraph::ViewData vd{};
    vd.delta_data = &state;
    vd.ops = &k_fake_delta_ops;
    vd.engine_time_ptr = nullptr;

    hgraph::DeltaView delta = hgraph::DeltaView::from_computed(vd, expected_time);
    nb::object py_delta = delta.to_python();
    REQUIRE_FALSE(py_delta.is_none());
    REQUIRE(nb::cast<int>(py_delta) == 123);
}
