#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <algorithm>
#include <string>
#include <type_traits>
#include <vector>

namespace hgraph
{
    namespace test_detail
    {
        struct ExposedTSValue : TSValue
        {
            using TSValue::TSValue;
            using TSValue::atomic_value;
            using TSValue::bundle_delta_value;
            using TSValue::bundle_value;
            using TSValue::dict_delta_value;
            using TSValue::dict_value;
            using TSValue::list_delta_value;
            using TSValue::list_value;
            using TSValue::set_delta_value;
            using TSValue::set_value;
            using TSValue::value;
            using TSValue::view_context;
            using TSValue::window_value;
        };

        struct ExposedTSOutput : TSOutput
        {
            using TSOutput::TSOutput;
            using TSValue::atomic_value;
            using TSValue::view_context;
        };

        struct ExposedTSInput : TSInput
        {
            using TSInput::TSInput;
            using TSInput::view;
            using TSValue::view_context;
        };

        const BaseState &as_base_state(void *state)
        {
            return *static_cast<BaseState *>(state);
        }
    }  // namespace test_detail
}  // namespace hgraph

TEST_CASE("TSValue picks the correct root state for each schema kind", "[ts_value]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *set_ts = ts_registry.tss(int_type);
    const auto *dict_ts = ts_registry.tsd(str_type, scalar_ts);
    const auto *list_ts = ts_registry.tsl(scalar_ts, 3);
    const auto *window_ts = ts_registry.tsw(int_type, 4, 1);

    std::vector<const hgraph::TSMeta *> cases{
        scalar_ts,
        set_ts,
        dict_ts,
        list_ts,
        window_ts,
    };

    for (const auto *schema : cases) {
        hgraph::test_detail::ExposedTSValue value{*schema};
        auto state = value.view_context().ts_state;

        REQUIRE(state != nullptr);

        const hgraph::BaseState &base = hgraph::test_detail::as_base_state(state);
        CHECK(base.index == 0);
        CHECK(base.last_modified_time == hgraph::MIN_DT);
    }
}

TEST_CASE("TSValue stores collection values with delta tracking for time-series use", "[ts_value]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *str_type = value_registry.register_type<std::string>("str");

    SECTION("set delta exposes added and removed elements") {
        hgraph::test_detail::ExposedTSValue value{*ts_registry.tss(int_type)};

        {
            auto mutation = value.set_value().begin_mutation();
            mutation.adding(hgraph::Value{1}.view()).adding(hgraph::Value{2}.view());
        }

        auto delta = value.set_delta_value();
        std::vector<int> added;
        for (hgraph::View element : delta.added()) { added.push_back(element.as_atomic().as<int>()); }
        CHECK(added == std::vector<int>{1, 2});

        {
            auto mutation = value.set_value().begin_mutation();
            mutation.removing(hgraph::Value{1}.view());
        }

        delta = value.set_delta_value();
        std::vector<int> removed;
        for (hgraph::View element : delta.removed()) { removed.push_back(element.as_atomic().as<int>()); }
        CHECK(removed == std::vector<int>{1});
    }

    SECTION("map delta exposes removed items by stable slot") {
        hgraph::test_detail::ExposedTSValue value{*ts_registry.tsd(str_type, ts_registry.ts(int_type))};

        {
            auto mutation = value.dict_value().begin_mutation();
            mutation.setting(hgraph::Value{std::string{"a"}}.view(), hgraph::Value{1}.view());
            mutation.setting(hgraph::Value{std::string{"b"}}.view(), hgraph::Value{2}.view());
        }

        {
            auto mutation = value.dict_value().begin_mutation();
            mutation.removing(hgraph::Value{std::string{"a"}}.view());
        }

        auto delta = value.dict_delta_value();
        std::vector<std::pair<std::string, int>> removed;
        for (auto [key, item_value] : delta.removed_items()) {
            removed.emplace_back(key.as_atomic().as<std::string>(), item_value.as_atomic().as<int>());
        }

        REQUIRE(removed.size() == 1);
        CHECK(removed.front().first == "a");
        CHECK(removed.front().second == 1);
    }

    SECTION("list and bundle delta surfaces are available from TSValue") {
        hgraph::test_detail::ExposedTSValue list_value{*ts_registry.tsl(ts_registry.ts(int_type), 2)};
        {
            auto mutation = list_value.list_value().begin_mutation();
            mutation.setting(0, hgraph::Value{7}.view());
        }
        std::vector<size_t> updated_indices;
        for (size_t index : list_value.list_delta_value().updated_indices()) { updated_indices.push_back(index); }
        CHECK(updated_indices == std::vector<size_t>{0});

        const auto *bundle_schema = ts_registry.tsb({{"lhs", ts_registry.ts(int_type)}, {"rhs", ts_registry.ts(int_type)}}, "Pair");
        hgraph::test_detail::ExposedTSValue bundle_value{*bundle_schema};
        {
            auto mutation = bundle_value.bundle_value().begin_mutation();
            mutation.setting_field("lhs", hgraph::Value{11}.view());
        }
        std::vector<std::string> updated_keys;
        for (std::string_view key : bundle_value.bundle_delta_value().updated_keys()) {
            updated_keys.emplace_back(key);
        }
        CHECK(updated_keys == std::vector<std::string>{"lhs"});
    }
}

TEST_CASE("TSValue exposes nested TS navigation through cached TS dispatch", "[ts_value]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *str_type = value_registry.register_type<std::string>("str");

    SECTION("bundle children resolve field schemas and payloads") {
        const auto *bundle_schema = ts_registry.tsb({{"lhs", ts_registry.ts(int_type)},
                                                     {"rhs", ts_registry.tsl(ts_registry.ts(int_type), 2)}},
                                                    "PairList");
        hgraph::test_detail::ExposedTSValue value{*bundle_schema};

        {
            auto mutation = value.bundle_value().begin_mutation();
            mutation.setting_field("lhs", hgraph::Value{11}.view());

            auto rhs = value.bundle_value().field("rhs").as_list().begin_mutation();
            rhs.setting(0, hgraph::Value{7}.view());
            rhs.setting(1, hgraph::Value{13}.view());
        }

        hgraph::TSOutputView root{value.view_context()};
        auto bundle_view = root.as_bundle();

        const auto lhs = bundle_view.field("lhs");
        CHECK(lhs.value().as_atomic().as<int>() == 11);

        const auto rhs = bundle_view.field("rhs").as_list();
        REQUIRE(rhs.size() == 2);
        CHECK(rhs.at(0).value().as_atomic().as<int>() == 7);
        CHECK(rhs.at(1).value().as_atomic().as<int>() == 13);
    }

    SECTION("dict children resolve by key and by storage slot") {
        const auto *dict_schema = ts_registry.tsd(str_type, ts_registry.ts(int_type));
        hgraph::test_detail::ExposedTSValue value{*dict_schema};

        {
            auto mutation = value.dict_value().begin_mutation();
            mutation.setting(hgraph::Value{std::string{"a"}}.view(), hgraph::Value{1}.view());
            mutation.setting(hgraph::Value{std::string{"b"}}.view(), hgraph::Value{2}.view());
        }

        size_t slot_a = SIZE_MAX;
        size_t slot_b = SIZE_MAX;
        {
            const auto delta = value.dict_delta_value();
            for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                if (!delta.slot_occupied(slot)) { continue; }
                const auto key = delta.key_at_slot(slot).as_atomic().as<std::string>();
                if (key == "a") { slot_a = slot; }
                if (key == "b") { slot_b = slot; }
            }
        }

        REQUIRE(slot_a != SIZE_MAX);
        REQUIRE(slot_b != SIZE_MAX);

        {
            auto mutation = value.dict_value().begin_mutation();
            mutation.removing(hgraph::Value{std::string{"a"}}.view());
        }

        hgraph::TSOutputView root{value.view_context()};
        auto dict_view = root.as_dict();

        const hgraph::Value key_b{std::string{"b"}};
        const auto by_key = dict_view.at(key_b.view());
        CHECK(by_key.value().as_atomic().as<int>() == 2);

        const auto by_live_slot = dict_view.at(slot_b);
        CHECK(by_live_slot.value().as_atomic().as<int>() == 2);

        const auto by_removed_slot = dict_view.at(slot_a);
        CHECK(by_removed_slot.value().as_atomic().as<int>() == 1);

        CHECK(dict_view.size() == 1);

        const auto delta = value.dict_delta_value();
        CHECK(delta.slot_removed(slot_a));

        std::vector<std::pair<std::string, int>> items;
        for (auto [key, child] : dict_view.items()) {
            items.emplace_back(key.as_atomic().as<std::string>(), child.value().as_atomic().as<int>());
        }
        std::sort(items.begin(), items.end());
        CHECK((items == std::vector<std::pair<std::string, int>>{{"b", 2}}));
    }
}

TEST_CASE("TSInput can represent a top-level bundle field bound through a target link", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");

    hgraph::test_detail::ExposedTSOutput output{scalar_ts};
    const auto output_view = output.view();

    hgraph::test_detail::ExposedTSInput input{input_schema};
    input.view().as_bundle()[0].bind_output(output_view);

    auto *root_state = static_cast<hgraph::TSBState *>(input.view_context().ts_state);
    REQUIRE(root_state != nullptr);
    REQUIRE(root_state->child_states.size() == 1);
    REQUIRE(root_state->child_states[0] != nullptr);
    REQUIRE(std::holds_alternative<hgraph::TargetLinkState>(*root_state->child_states[0]));

    const auto &link_state = std::get<hgraph::TargetLinkState>(*root_state->child_states[0]);
    CHECK(link_state.is_bound());
    CHECK(link_state.target.schema == scalar_ts);
    CHECK(link_state.target.ts_state == static_cast<hgraph::BaseState *>(output.view_context().ts_state));

    hgraph::TSOutputView input_root{input.view_context()};
    auto linked_leaf = input_root.as_bundle().field("ts");
    CHECK_FALSE(linked_leaf.valid());

    output.atomic_value().set(1.5f);
    CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(1.5f));

    output.atomic_value().set(2.5f);
    CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(2.5f));
}

TEST_CASE("TSInput target-link rebinding and teardown release old output subscriptions", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");

    hgraph::test_detail::ExposedTSOutput first_output{scalar_ts};
    hgraph::test_detail::ExposedTSOutput second_output{scalar_ts};
    const auto first_output_view = first_output.view();
    const auto second_output_view = second_output.view();
    first_output.atomic_value().set(1.0f);
    second_output.atomic_value().set(2.0f);

    auto *first_state = static_cast<hgraph::BaseState *>(first_output.view_context().ts_state);
    auto *second_state = static_cast<hgraph::BaseState *>(second_output.view_context().ts_state);
    REQUIRE(first_state != nullptr);
    REQUIRE(second_state != nullptr);

    {
        hgraph::test_detail::ExposedTSInput input{input_schema};
        input.view().as_bundle()[0].bind_output(first_output_view);
        CHECK(first_state->subscribers.size() == 1);
        CHECK(second_state->subscribers.empty());

        input.view().as_bundle()[0].bind_output(second_output_view);
        CHECK(first_state->subscribers.empty());
        CHECK(second_state->subscribers.size() == 1);

        hgraph::TSOutputView input_root{input.view_context()};
        auto linked_leaf = input_root.as_bundle().field("ts");
        CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(2.0f));

        first_output.atomic_value().set(3.0f);
        CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(2.0f));

        second_output.atomic_value().set(4.0f);
        CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(4.0f));
    }

    CHECK(first_state->subscribers.empty());
    CHECK(second_state->subscribers.empty());
}
