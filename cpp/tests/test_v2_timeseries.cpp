#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/types/timeseries/ts_input_view.h>
#include <hgraph/v2/types/value/value.h>

#include <string>
#include <type_traits>

namespace
{
    using namespace hgraph;
    using namespace hgraph::v2;

    static_assert(!std::is_copy_constructible_v<TsView>);
    static_assert(!std::is_copy_assignable_v<TsView>);
    static_assert(std::is_move_constructible_v<TsView>);
    static_assert(std::is_move_assignable_v<TsView>);

    static_assert(!std::is_copy_constructible_v<TsInputView>);
    static_assert(!std::is_copy_assignable_v<TsInputView>);
    static_assert(std::is_move_constructible_v<TsInputView>);
    static_assert(std::is_move_assignable_v<TsInputView>);

    static_assert(!std::is_copy_constructible_v<TsOutputView>);
    static_assert(!std::is_copy_assignable_v<TsOutputView>);
    static_assert(std::is_move_constructible_v<TsOutputView>);
    static_assert(std::is_move_assignable_v<TsOutputView>);
}  // namespace

TEST_CASE("v2 TS builders bridge TS schemas to the underlying value builders", "[v2 timeseries]") {
    const ValueTypeMetaData   *int_meta = value::scalar_type_meta<int>();
    const TSValueTypeMetaData *int_ts   = TypeRegistry::instance().ts(int_meta);

    const TsValueBuilder  &ts_builder     = TsValueBuilder::checked(int_ts);
    const TsInputBuilder  &input_builder  = TsInputBuilder::checked(int_ts);
    const TsOutputBuilder &output_builder = TsOutputBuilder::checked(int_ts);

    REQUIRE(ts_builder.type() == int_ts);
    REQUIRE(ts_builder.value_type() == int_meta);
    REQUIRE(ts_builder.value_builder() == &value::scalar_value_builder<int>());
    REQUIRE(ts_builder.value_binding() == value::scalar_value_builder<int>().binding());
    REQUIRE(input_builder.binding() != nullptr);
    REQUIRE(output_builder.binding() != nullptr);
    REQUIRE(input_builder.binding()->type_meta == int_ts);
    REQUIRE(output_builder.binding()->type_meta == int_ts);
    REQUIRE(input_builder.checked_binding().checked_ops().checked_value_binding().type_meta == int_meta);
    REQUIRE(output_builder.checked_binding().checked_ops().checked_value_binding().type_meta == int_meta);
    CHECK(static_cast<const void *>(input_builder.binding()) != static_cast<const void *>(output_builder.binding()));
    REQUIRE(input_builder.ts_value_builder() == &ts_builder);
    REQUIRE(output_builder.ts_value_builder() == &ts_builder);
}

TEST_CASE("v2 TS outputs own value storage and inputs bind by reference", "[v2 timeseries]") {
    const ValueTypeMetaData   *int_meta = value::scalar_type_meta<int>();
    const TSValueTypeMetaData *int_ts   = TypeRegistry::instance().ts(int_meta);

    TsOutput output(*int_ts);
    output.value().set(42);

    TsInput input(*int_ts);
    REQUIRE(input.binding() != nullptr);
    REQUIRE(output.binding() != nullptr);
    REQUIRE_FALSE(input.has_value());
    REQUIRE_FALSE(input.is_bound());
    REQUIRE(input.value().binding() == output.value().binding());

    input.bind_output(output);
    REQUIRE(input.has_value());
    REQUIRE(input.is_bound());
    CHECK(input.value().as<int>() == 42);

    output.value().set(64);
    CHECK(input.value().as<int>() == 64);

    input.unbind_output();
    REQUIRE_FALSE(input.has_value());
    REQUIRE_FALSE(input.is_bound());
    REQUIRE(input.value().binding() == output.value().binding());
}

TEST_CASE("v2 TS input and output views expose root bind and read semantics", "[v2 timeseries]") {
    const ValueTypeMetaData   *int_meta = value::scalar_type_meta<int>();
    const TSValueTypeMetaData *int_ts   = TypeRegistry::instance().ts(int_meta);

    TsOutput output(*int_ts);
    output.value().set(7);

    TsInput input(*int_ts);
    auto    output_view = output.view(MIN_DT + engine_time_delta_t{5});
    auto    input_view  = input.view(MIN_DT + engine_time_delta_t{5});

    REQUIRE(output_view.type() == int_ts);
    REQUIRE(output_view.value_type() == int_meta);
    REQUIRE(output_view.evaluation_time() == MIN_DT + engine_time_delta_t{5});
    REQUIRE_FALSE(input_view.has_value());

    input_view.bind_output(output_view);
    REQUIRE(input_view.is_bound());
    CHECK(input_view.value().as<int>() == 7);

    output.value().set(8);
    CHECK(input_view.value().as<int>() == 8);

    input_view.unbind_output();
    REQUIRE_FALSE(input_view.has_value());
    REQUIRE_FALSE(input_view.is_bound());
}

TEST_CASE("v2 TS specialized views expose bundle and list child TS values", "[v2 timeseries]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const ValueTypeMetaData   *int_meta    = value::scalar_type_meta<int>();
    const ValueTypeMetaData   *string_meta = value::scalar_type_meta<std::string>();
    const TSValueTypeMetaData *int_ts      = registry.ts(int_meta);
    const TSValueTypeMetaData *string_ts   = registry.ts(string_meta);
    const TSValueTypeMetaData *bundle_ts   = registry.tsb({{"count", int_ts}, {"label", string_ts}}, "Pair");
    const TSValueTypeMetaData *list_ts     = registry.tsl(int_ts, 2);

    TsOutput bundle_output(*bundle_ts);
    auto     bundle_view = bundle_output.view().as_tsb();

    bundle_view.set("count", value::value_for(3).view());
    bundle_view.set("label", value::value_for(std::string("alpha")).view());

    CHECK(bundle_view["count"].type() == int_ts);
    CHECK(bundle_view["count"].value().as<int>() == 3);
    CHECK(bundle_view["label"].type() == string_ts);
    CHECK(bundle_view["label"].value().as<std::string>() == "alpha");
    CHECK(bundle_view.value().to_string() == "Pair{count: 3, label: alpha}");

    TsOutput list_output(*list_ts);
    auto     list_view = list_output.view().as_tsl();

    list_view.set(0, value::value_for(11).view());
    list_view.set(1, value::value_for(12).view());

    CHECK(list_view[0].type() == int_ts);
    CHECK(list_view[0].value().as<int>() == 11);
    CHECK(list_view[1].value().as<int>() == 12);
    CHECK(list_view.value().to_string() == "[11, 12]");
}

TEST_CASE("v2 TS specialized views expose set and dict semantics", "[v2 timeseries]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const ValueTypeMetaData   *int_meta    = value::scalar_type_meta<int>();
    const ValueTypeMetaData   *string_meta = value::scalar_type_meta<std::string>();
    const TSValueTypeMetaData *string_ts   = registry.ts(string_meta);
    const TSValueTypeMetaData *set_ts      = registry.tss(int_meta);
    const TSValueTypeMetaData *dict_ts     = registry.tsd(int_meta, string_ts);

    TsOutput set_output(*set_ts);
    auto     set_view = set_output.view().as_tss();

    const Value one   = value::value_for(1);
    const Value three = value::value_for(3);
    CHECK(set_view.add(one.view()));
    CHECK(set_view.add(three.view()));
    CHECK(set_view.contains(one.view()));
    CHECK(set_view.size() == 2);

    TsOutput dict_output(*dict_ts);
    auto     dict_view = dict_output.view().as_tsd();

    const Value key   = value::value_for(7);
    const Value alpha = value::value_for(std::string("alpha"));
    dict_view.set(key.view(), alpha.view());

    CHECK(dict_view.contains(key.view()));
    CHECK(dict_view.at(key.view()).type() == string_ts);
    CHECK(dict_view.at(key.view()).value().as<std::string>() == "alpha");
    CHECK(dict_view.size() == 1);
}

TEST_CASE("v2 TSW view exposes window metadata and current payload projection", "[v2 timeseries]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const ValueTypeMetaData   *int_meta    = value::scalar_type_meta<int>();
    const TSValueTypeMetaData *tick_win_ts = registry.tsw(int_meta, 5, 3);
    const TSValueTypeMetaData *time_win_ts = registry.tsw_duration(int_meta, engine_time_delta_t{10}, engine_time_delta_t{4});

    TsOutput tick_output(*tick_win_ts);
    tick_output.value().set(21);
    auto tick_view = tick_output.view().as_tsw();

    CHECK_FALSE(tick_view.is_duration_based());
    CHECK(tick_view.period() == 5);
    CHECK(tick_view.min_period() == 3);
    CHECK(tick_view.element_type() == int_meta);
    CHECK(tick_view.value().as<int>() == 21);

    TsOutput time_output(*time_win_ts);
    time_output.value().set(34);
    auto time_view = time_output.view().as_tsw();

    CHECK(time_view.is_duration_based());
    CHECK(time_view.time_range() == engine_time_delta_t{10});
    CHECK(time_view.min_time_range() == engine_time_delta_t{4});
    CHECK(time_view.value().as<int>() == 34);
}

TEST_CASE("default constructed v2 TS inputs can adopt schema when bound", "[v2 timeseries]") {
    const ValueTypeMetaData   *int_meta = value::scalar_type_meta<int>();
    const TSValueTypeMetaData *int_ts   = TypeRegistry::instance().ts(int_meta);

    TsOutput output(*int_ts);
    output.value().set(99);

    TsInput input;
    REQUIRE(input.builder() == nullptr);
    REQUIRE_FALSE(input.has_value());

    input.bind_output(output);
    REQUIRE(input.builder() != nullptr);
    REQUIRE(input.type() == int_ts);
    REQUIRE(input.is_bound());
    CHECK(input.value().as<int>() == 99);
}
