#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

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
