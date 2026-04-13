#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/v2/ref.h>

#include <algorithm>
#include <chrono>
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
            using TSValue::bundle_value;
            using TSValue::dict_delta_value;
            using TSValue::dict_value;
            using TSValue::list_value;
            using TSValue::set_value;
            using TSValue::view_context;
        };

        struct ExposedTSInput : TSInput
        {
            using TSInput::TSInput;
            using TSInput::view;
            using TSValue::view_context;
        };

        struct RecordingNotifiable : Notifiable
        {
            void notify(engine_time_t et) override { notifications.push_back(et); }

            std::vector<engine_time_t> notifications;
        };

        [[nodiscard]] TSInputConstructionPlan single_link_plan(const TSMeta *input_schema,
                                                               size_t field_index,
                                                               TSInputBindingRef binding = {})
        {
            TSInputConstructionPlan plan{input_schema};
            plan.root().children()[field_index] =
                TSInputConstructionSlot::create_link_terminal(input_schema->fields()[field_index].ts_type, std::move(binding));
            return plan;
        }

        [[nodiscard]] const TSInputBuilder &builder_for(const TSInputConstructionPlan &plan)
        {
            return TSInputBuilderFactory::checked_builder_for(plan);
        }

        [[nodiscard]] const TSOutputBuilder &output_builder_for(const TSMeta *schema)
        {
            return TSOutputBuilderFactory::checked_builder_for(schema);
        }

        [[nodiscard]] engine_time_t tick(int offset)
        {
            return MIN_DT + std::chrono::microseconds{offset};
        }

        [[nodiscard]] engine_time_t next_mutation_tick()
        {
            static int offset = 1000;
            return tick(offset++);
        }

        const BaseState &as_base_state(void *state)
        {
            return *static_cast<BaseState *>(state);
        }

        [[nodiscard]] BaseState *state_of(TimeSeriesStateV &state)
        {
            return std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, state);
        }

        [[nodiscard]] BaseState *child_state(BaseCollectionState &state, size_t index)
        {
            if (index >= state.child_states.size() || state.child_states[index] == nullptr) { return nullptr; }
            return state_of(*state.child_states[index]);
        }

        [[nodiscard]] TargetLinkState &linked_field(ExposedTSInput &input, size_t field_index = 0)
        {
            auto *root_state = static_cast<TSBState *>(input.view_context().ts_state);
            REQUIRE(root_state != nullptr);
            REQUIRE(field_index < root_state->child_states.size());
            REQUIRE(root_state->child_states[field_index] != nullptr);
            REQUIRE(std::holds_alternative<TargetLinkState>(*root_state->child_states[field_index]));
            return std::get<TargetLinkState>(*root_state->child_states[field_index]);
        }

        [[nodiscard]] size_t find_live_dict_slot(ExposedTSOutput &output, const Value &key)
        {
            const auto delta = output.dict_delta_value();
            for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                if (!delta.slot_occupied(slot) || delta.slot_removed(slot)) { continue; }
                if (delta.key_at_slot(slot).equals(key.view())) { return slot; }
            }
            return SIZE_MAX;
        }

        void mark_output_view_modified(const TSOutputView &view, engine_time_t modified_time)
        {
            LinkedTSContext context = view.linked_context();
            REQUIRE(context.ts_state != nullptr);
            context.ts_state->mark_modified(modified_time);
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
            auto mutation = value.set_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.adding(hgraph::Value{1}.view()).adding(hgraph::Value{2}.view());
        }

        auto delta = value.set_delta_value();
        std::vector<int> added;
        for (hgraph::View element : delta.added()) { added.push_back(element.as_atomic().as<int>()); }
        CHECK(added == std::vector<int>{1, 2});

        {
            auto mutation = value.set_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
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
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.setting(hgraph::Value{std::string{"a"}}.view(), hgraph::Value{1}.view());
            mutation.setting(hgraph::Value{std::string{"b"}}.view(), hgraph::Value{2}.view());
        }

        {
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
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

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
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
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
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
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.removing(hgraph::Value{std::string{"a"}}.view());
        }

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
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

    SECTION("cached dict child views survive parent reserve") {
        const auto *dict_schema = ts_registry.tsd(str_type, ts_registry.ts(int_type));
        hgraph::test_detail::ExposedTSValue value{*dict_schema};
        const hgraph::Value key_a{std::string{"a"}};

        {
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.setting(key_a.view(), hgraph::Value{1}.view());
        }

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
        const auto cached_child = root.as_dict().at(key_a.view());
        REQUIRE(cached_child.value().as_atomic().as<int>() == 1);
        const hgraph::BaseState *cached_state = cached_child.context_ref().ts_state;
        const void             *cached_value_data = cached_child.context_ref().resolved().value_data;

        {
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.reserve(64);
            for (int i = 0; i < 16; ++i) {
                mutation.setting(hgraph::Value{std::string{"k" + std::to_string(i)}}.view(), hgraph::Value{i + 10}.view());
            }
        }

        CHECK(cached_child.context_ref().ts_state == cached_state);
        CHECK(cached_child.context_ref().resolved().value_data == cached_value_data);
        CHECK(cached_child.value().as_atomic().as<int>() == 1);
    }

    SECTION("cached nested dict child views survive outer parent reserve") {
        const auto *inner_dict_schema = ts_registry.tsd(str_type, ts_registry.ts(int_type));
        const auto *outer_dict_schema = ts_registry.tsd(str_type, inner_dict_schema);
        hgraph::test_detail::ExposedTSValue inner_value{*inner_dict_schema};
        hgraph::test_detail::ExposedTSValue value{*outer_dict_schema};
        const hgraph::Value outer_key{std::string{"outer"}};
        const hgraph::Value inner_key{std::string{"leaf"}};

        {
            auto mutation = inner_value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.setting(inner_key.view(), hgraph::Value{7}.view());
        }

        {
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.setting(outer_key.view(), inner_value.value());
        }

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
        const auto cached_outer_child = root.as_dict().at(outer_key.view());
        const auto cached_inner_child = cached_outer_child.as_dict().at(inner_key.view());
        REQUIRE(cached_outer_child.value().as_map().size() == 1);
        REQUIRE(cached_inner_child.value().as_atomic().as<int>() == 7);
        const hgraph::BaseState *cached_outer_state = cached_outer_child.context_ref().ts_state;
        const hgraph::BaseState *cached_inner_state = cached_inner_child.context_ref().ts_state;
        const void             *cached_outer_value_data = cached_outer_child.context_ref().resolved().value_data;
        const void             *cached_inner_value_data = cached_inner_child.context_ref().resolved().value_data;

        {
            auto mutation = value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
            mutation.reserve(64);
            for (int i = 0; i < 16; ++i) {
                hgraph::test_detail::ExposedTSValue extra_inner{*inner_dict_schema};
                auto inner_mutation = extra_inner.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
                inner_mutation.setting(inner_key.view(), hgraph::Value{i}.view());
                mutation.setting(hgraph::Value{std::string{"outer_" + std::to_string(i)}}.view(), extra_inner.value());
            }
        }

        CHECK(cached_outer_child.context_ref().ts_state == cached_outer_state);
        CHECK(cached_inner_child.context_ref().ts_state == cached_inner_state);
        CHECK(cached_outer_child.context_ref().resolved().value_data == cached_outer_value_data);
        CHECK(cached_inner_child.context_ref().resolved().value_data == cached_inner_value_data);
        CHECK(cached_outer_child.value().as_map().size() == 1);
        CHECK(cached_inner_child.value().as_atomic().as<int>() == 7);
    }
}

TEST_CASE("TS collection views report recursive all_valid state", "[ts_view][ts_value]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(int_type);

    SECTION("bundle all_valid requires every child to be valid") {
        const auto *bundle_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
        hgraph::test_detail::ExposedTSValue value{*bundle_schema};

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
        auto bundle = root.as_bundle();
        auto lhs = bundle.field("lhs");
        auto rhs = bundle.field("rhs");

        CHECK_FALSE(bundle.all_valid());

        lhs.value().set_scalar(11);
        hgraph::test_detail::mark_output_view_modified(lhs, hgraph::test_detail::tick(1));
        CHECK_FALSE(bundle.all_valid());

        rhs.value().set_scalar(13);
        hgraph::test_detail::mark_output_view_modified(rhs, hgraph::test_detail::tick(2));
        CHECK(bundle.all_valid());
    }

    SECTION("list all_valid requires every element to be valid") {
        hgraph::test_detail::ExposedTSValue value{*ts_registry.tsl(scalar_ts, 2)};

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
        auto list = root.as_list();
        auto first = list[0];
        auto second = list[1];

        CHECK_FALSE(list.all_valid());

        first.value().set_scalar(7);
        hgraph::test_detail::mark_output_view_modified(first, hgraph::test_detail::tick(3));
        CHECK_FALSE(list.all_valid());

        second.value().set_scalar(13);
        hgraph::test_detail::mark_output_view_modified(second, hgraph::test_detail::tick(4));
        CHECK(list.all_valid());
    }

    SECTION("dict all_valid follows live child validity") {
        hgraph::test_detail::ExposedTSValue value{*ts_registry.tsd(str_type, scalar_ts)};
        const hgraph::Value key{std::string{"a"}};

        hgraph::TSOutputView root{value.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
        auto dict = root.as_dict();

        CHECK(dict.all_valid());

        value.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick()).setting(key.view(), hgraph::Value{1}.view());
        auto child = dict.at(key.view());
        CHECK_FALSE(dict.all_valid());

        hgraph::test_detail::mark_output_view_modified(child, hgraph::test_detail::tick(5));
        CHECK(dict.all_valid());
    }
}

TEST_CASE("TS collection views expose collection delta through root delta_value", "[ts_view][ts_value]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(int_type);

    SECTION("bundle root delta_value returns bundle delta state") {
        const auto *bundle_ts = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(bundle_ts)};

        auto mutation = output.bundle_value().begin_mutation();
        mutation.setting_field("lhs", hgraph::Value{7}.view());

        const hgraph::BundleDeltaView delta{output.view().delta_value()};
        std::vector<std::string_view> updated_keys;
        for (std::string_view key : delta.updated_keys()) { updated_keys.push_back(key); }
        CHECK(updated_keys == std::vector<std::string_view>{"lhs"});
    }

    SECTION("list root delta_value returns list delta state") {
        const auto *list_ts = ts_registry.tsl(scalar_ts, 2);
        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(list_ts)};

        auto mutation = output.list_value().begin_mutation();
        mutation.setting(1, hgraph::Value{11}.view());

        const hgraph::ListDeltaView delta{output.view().delta_value()};
        std::vector<size_t> updated_indices;
        for (size_t index : delta.updated_indices()) { updated_indices.push_back(index); }
        CHECK(updated_indices == std::vector<size_t>{1});
    }

    SECTION("dict root delta_value returns map delta state") {
        const auto *dict_ts = ts_registry.tsd(str_type, scalar_ts);
        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
        const hgraph::Value key{std::string{"k"}};

        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(30));
        mutation.setting(key.view(), hgraph::Value{13}.view());

        const hgraph::MapDeltaView delta{output.view().delta_value()};
        std::vector<std::string> added_keys;
        for (const hgraph::View &added_key : delta.added_keys()) { added_keys.push_back(added_key.as_atomic().as<std::string>()); }
        CHECK(added_keys == std::vector<std::string>{"k"});
    }

    SECTION("set root delta_value returns set delta state") {
        const auto *set_ts = ts_registry.tss(int_type);
        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(set_ts)};

        auto mutation = output.set_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        CHECK(mutation.add(hgraph::Value{17}.view()));

        const hgraph::SetDeltaView delta{output.view().delta_value()};
        std::vector<int> added_values;
        for (const hgraph::View &added_value : delta.added()) { added_values.push_back(added_value.as_atomic().as<int>()); }
        CHECK(added_values == std::vector<int>{17});
    }
}

TEST_CASE("TSInput construction plans prebuild top-level target-link terminals", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
    const auto output_view = output.view();

    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
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

    hgraph::TSOutputView input_root{input.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
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
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput first_output{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput second_output{hgraph::test_detail::output_builder_for(scalar_ts)};
    const auto first_output_view = first_output.view();
    const auto second_output_view = second_output.view();
    first_output.atomic_value().set(1.0f);
    second_output.atomic_value().set(2.0f);

    auto *first_state = static_cast<hgraph::BaseState *>(first_output.view_context().ts_state);
    auto *second_state = static_cast<hgraph::BaseState *>(second_output.view_context().ts_state);
    REQUIRE(first_state != nullptr);
    REQUIRE(second_state != nullptr);

    {
        hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
        input.view().as_bundle()[0].bind_output(first_output_view);
        CHECK(first_state->subscribers.size() == 1);
        CHECK(second_state->subscribers.empty());

        input.view().as_bundle()[0].bind_output(second_output_view);
        CHECK(first_state->subscribers.empty());
        CHECK(second_state->subscribers.size() == 1);

        hgraph::TSOutputView input_root{input.view_context(), hgraph::TSViewContext::none(), hgraph::MIN_DT};
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

TEST_CASE("TSInput construction plans prebuild native prefixes and nested target links", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *nested_bundle_schema = ts_registry.tsb({{"ts", scalar_ts}}, "Nested");
    const auto *input_schema = ts_registry.tsb({{"grp", nested_bundle_schema}}, "InputBundle");

    hgraph::TSInputConstructionPlan plan{input_schema};
    plan.root().children()[0] = hgraph::TSInputConstructionSlot::create_non_peered_collection(nested_bundle_schema);
    plan.root().children()[0].children()[0] = hgraph::TSInputConstructionSlot::create_link_terminal(scalar_ts);

    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    auto *root_state = static_cast<hgraph::TSBState *>(input.view_context().ts_state);
    REQUIRE(root_state != nullptr);
    REQUIRE(root_state->child_states.size() == 1);
    REQUIRE(root_state->child_states[0] != nullptr);
    REQUIRE(std::holds_alternative<hgraph::TSBState>(*root_state->child_states[0]));

    auto &nested_state = std::get<hgraph::TSBState>(*root_state->child_states[0]);
    REQUIRE(nested_state.child_states.size() == 1);
    REQUIRE(nested_state.child_states[0] != nullptr);
    REQUIRE(std::holds_alternative<hgraph::TargetLinkState>(*nested_state.child_states[0]));

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
    auto linked_leaf = input.view().as_bundle()[0].as_bundle()[0];
    CHECK_FALSE(linked_leaf.valid());

    linked_leaf.bind_output(output.view());
    output.atomic_value().set(3.5f);
    CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(3.5f));
}

TEST_CASE("TSInputBuilder constructs TSInput instances from construction plans", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");

    const hgraph::TSInputBuilder &builder = hgraph::test_detail::builder_for(hgraph::test_detail::single_link_plan(input_schema, 0));
    hgraph::TSInput input = builder.make_input();

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
    auto linked_leaf = input.view().as_bundle()[0];
    linked_leaf.bind_output(output.view());
    output.atomic_value().set(6.25f);
    CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(6.25f));
}

TEST_CASE("TS builders can construct default-initialized TSValue, TSInput, and TSOutput instances", "[ts_input][ts_output]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");

    SECTION("TSValue can be default-constructed and later bound by its builder") {
        hgraph::test_detail::ExposedTSValue value;
        hgraph::TSValueBuilderFactory::checked_builder_for(*scalar_ts).construct_value(value);

        CHECK(value.view_context().schema == scalar_ts);
        CHECK(value.view_context().ts_state != nullptr);
        CHECK(value.atomic_value().as<float>() == Catch::Approx(0.0f));
    }

    SECTION("TSInput can be default-constructed and later bound by its builder") {
        const hgraph::TSInputBuilder &builder =
            hgraph::test_detail::builder_for(hgraph::test_detail::single_link_plan(input_schema, 0));

        hgraph::test_detail::ExposedTSInput input;
        builder.construct_input(input);

        auto linked_leaf = input.view().as_bundle()[0];
        CHECK_FALSE(linked_leaf.valid());

        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
        linked_leaf.bind_output(output.view());
        output.atomic_value().set(8.5f);
        CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(8.5f));
    }

    SECTION("TSOutput can be default-constructed and later bound by its builder") {
        const hgraph::TSOutputBuilder &builder = hgraph::test_detail::output_builder_for(scalar_ts);

        hgraph::test_detail::ExposedTSOutput output;
        builder.construct_output(output);

        CHECK(output.view_context().schema == scalar_ts);
        CHECK(output.view_context().ts_state != nullptr);
        CHECK(output.atomic_value().as<float>() == Catch::Approx(0.0f));
    }

    SECTION("TSInput can be constructed and copy-constructed into caller-provided memory") {
        const hgraph::TSInputBuilder &builder =
            hgraph::test_detail::builder_for(hgraph::test_detail::single_link_plan(input_schema, 0));

        hgraph::test_detail::ExposedTSInput input;
        void *memory = builder.allocate();
        builder.construct_input(input, memory);

        auto linked_leaf = input.view().as_bundle()[0];
        CHECK_FALSE(linked_leaf.valid());

        hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
        linked_leaf.bind_output(output.view());
        output.atomic_value().set(3.25f);
        CHECK(linked_leaf.value().as_atomic().as<float>() == Catch::Approx(3.25f));

        hgraph::test_detail::ExposedTSInput copied_input;
        void *copy_memory = builder.allocate();
        builder.copy_construct_input(copied_input, input, copy_memory);

        auto copied_leaf = copied_input.view().as_bundle()[0];
        CHECK(copied_leaf.value().as_atomic().as<float>() == Catch::Approx(3.25f));

        builder.destruct_input(copied_input);
        builder.destruct_input(input);
        builder.deallocate(copy_memory);
        builder.deallocate(memory);
    }

    SECTION("TSOutput can be constructed and copy-constructed into caller-provided memory") {
        const hgraph::TSOutputBuilder &builder = hgraph::test_detail::output_builder_for(scalar_ts);

        hgraph::test_detail::ExposedTSOutput output;
        void *memory = builder.allocate();
        builder.construct_output(output, memory);
        output.atomic_value().set(4.75f);

        hgraph::test_detail::ExposedTSOutput copied_output;
        void *copy_memory = builder.allocate();
        builder.copy_construct_output(copied_output, output, copy_memory);

        CHECK(copied_output.atomic_value().as<float>() == Catch::Approx(4.75f));

        builder.destruct_output(copied_output);
        builder.destruct_output(output);
        builder.deallocate(copy_memory);
        builder.deallocate(memory);
    }
}

TEST_CASE("TSInput construction plan compiler builds link terminals from edge paths", "[ts_input][wiring_stub]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *nested_bundle_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", ts_registry.ts(int_type)}}, "Nested");
    const auto *input_schema = ts_registry.tsb({{"nested", nested_bundle_schema}}, "InputBundle");

    std::vector<hgraph::TSInputConstructionEdge> edges{
        hgraph::TSInputConstructionEdge{
            .input_path = {0, 0},
            .binding = hgraph::TSInputBindingRef{.src_node = 7, .output_path = {3, 1}},
        },
    };

    const hgraph::TSInputConstructionPlan plan = hgraph::TSInputConstructionPlanCompiler::compile(*input_schema, edges);

    REQUIRE(plan.root().kind() == hgraph::TSInputSlotKind::NonPeeredCollection);
    REQUIRE(plan.root().children().size() == 1);
    CHECK(plan.root().children()[0].kind() == hgraph::TSInputSlotKind::NonPeeredCollection);
    REQUIRE(plan.root().children()[0].children().size() == 2);
    CHECK(plan.root().children()[0].children()[0].kind() == hgraph::TSInputSlotKind::LinkTerminal);
    CHECK(plan.root().children()[0].children()[0].schema() == scalar_ts);
    CHECK(plan.root().children()[0].children()[0].binding().src_node == 7);
    CHECK(plan.root().children()[0].children()[0].binding().output_path == std::vector<int64_t>({3, 1}));
    CHECK(plan.root().children()[0].children()[1].kind() == hgraph::TSInputSlotKind::Empty);

    const hgraph::TSInputBuilder &builder = hgraph::TSInputBuilderFactory::checked_builder_for(plan);
    hgraph::test_detail::ExposedTSInput input{builder};
    auto *root_state = static_cast<hgraph::TSBState *>(input.view_context().ts_state);
    REQUIRE(root_state != nullptr);
    REQUIRE(root_state->child_states.size() == 1);
    REQUIRE(root_state->child_states[0] != nullptr);
    REQUIRE(std::holds_alternative<hgraph::TSBState>(*root_state->child_states[0]));
    const auto &nested_state = std::get<hgraph::TSBState>(*root_state->child_states[0]);
    REQUIRE(nested_state.child_states.size() == 2);
    REQUIRE(nested_state.child_states[0] != nullptr);
    CHECK(std::holds_alternative<hgraph::TargetLinkState>(*nested_state.child_states[0]));
    CHECK(nested_state.child_states[1] == nullptr);
}

TEST_CASE("TSInput leaf activation subscribes and unsubscribes through the target-link boundary", "[ts_input][active]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    input.view().as_bundle()[0].bind_output(output.view());

    auto &link_state = hgraph::test_detail::linked_field(input);
    hgraph::test_detail::RecordingNotifiable recorder;

    auto *output_state = static_cast<hgraph::BaseState *>(output.view_context().ts_state);
    REQUIRE(output_state != nullptr);
    CHECK(link_state.subscribers.empty());

    // Pass the recorder as root scheduling_notifier; boundary_notifier wires
    // it through to the TargetLinkState's SchedulingNotifier automatically.
    auto leaf_view = input.view(&recorder).as_bundle()[0];

    CHECK_FALSE(leaf_view.active());
    leaf_view.make_active();
    CHECK(leaf_view.active());
    CHECK(link_state.subscribers.size() == 1);

    output.atomic_value().set(1.5f);
    output_state->mark_modified(hgraph::test_detail::tick(1));
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(1)});

    leaf_view.make_passive();
    CHECK_FALSE(leaf_view.active());
    CHECK(link_state.subscribers.empty());

    output.atomic_value().set(2.5f);
    output_state->mark_modified(hgraph::test_detail::tick(2));
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(1)});
}

TEST_CASE("TSInput collection activation on a bound list is hierarchical", "[ts_input][active]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *list_ts = ts_registry.tsl(scalar_ts, 2);
    const auto *input_schema = ts_registry.tsb({{"ts", list_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(list_ts)};
    {
        auto mutation = output.list_value().begin_mutation();
        mutation.setting(0, hgraph::Value{1.0f}.view());
        mutation.setting(1, hgraph::Value{2.0f}.view());
    }

    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    input.view().as_bundle()[0].bind_output(output.view());

    auto &link_state = hgraph::test_detail::linked_field(input);
    auto *list_state = static_cast<hgraph::TSLState *>(output.view_context().ts_state);
    REQUIRE(list_state != nullptr);

    static_cast<void>(output.view().as_list()[0]);
    static_cast<void>(output.view().as_list()[1]);

    auto *first_child_state = hgraph::test_detail::child_state(*list_state, 0);
    auto *second_child_state = hgraph::test_detail::child_state(*list_state, 1);
    REQUIRE(first_child_state != nullptr);
    REQUIRE(second_child_state != nullptr);

    SECTION("an active list root is notified by any child modification") {
        hgraph::test_detail::RecordingNotifiable recorder;

        // boundary_notifier wires link_state.scheduling_notifier → recorder
        auto list_view = input.view(&recorder).as_bundle()[0];

        CHECK_FALSE(list_view.active());
        list_view.make_active();
        CHECK(list_view.active());
        CHECK(link_state.subscribers.size() == 1);

        first_child_state->mark_modified(hgraph::test_detail::tick(11));
        second_child_state->mark_modified(hgraph::test_detail::tick(12));
        CHECK(recorder.notifications ==
              std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(11), hgraph::test_detail::tick(12)});

        list_view.make_passive();
        CHECK_FALSE(list_view.active());
        CHECK(link_state.subscribers.empty());

        second_child_state->mark_modified(hgraph::test_detail::tick(13));
        CHECK(recorder.notifications ==
              std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(11), hgraph::test_detail::tick(12)});
    }

    SECTION("an active list element is notified only by that element") {
        hgraph::test_detail::RecordingNotifiable recorder;

        // boundary_notifier wires link_state.scheduling_notifier → recorder
        auto element_view = input.view(&recorder).as_bundle()[0].as_list()[1];

        CHECK_FALSE(element_view.active());
        element_view.make_active();
        CHECK(element_view.active());

        first_child_state->mark_modified(hgraph::test_detail::tick(21));
        CHECK(recorder.notifications.empty());

        second_child_state->mark_modified(hgraph::test_detail::tick(22));
        CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(22)});

        list_state->mark_modified(hgraph::test_detail::tick(23));
        CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(22)});

        element_view.make_passive();
        CHECK_FALSE(element_view.active());

        second_child_state->mark_modified(hgraph::test_detail::tick(24));
        CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(22)});
    }
}

TEST_CASE("TSInput dict-key activation survives a simple remove and re-add of the same key", "[ts_input][active]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *dict_ts = ts_registry.tsd(str_type, scalar_ts);
    const auto *input_schema = ts_registry.tsb({{"ts", dict_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    input.view().as_bundle()[0].bind_output(output.view());

    const hgraph::Value key{std::string{"k"}};
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(31));
        mutation.setting(key.view(), hgraph::Value{1.0f}.view());
    }

    auto *dict_state = static_cast<hgraph::TSDState *>(output.view_context().ts_state);
    REQUIRE(dict_state != nullptr);

    const size_t initial_slot = hgraph::test_detail::find_live_dict_slot(output, key);
    REQUIRE(initial_slot != SIZE_MAX);
    auto *initial_child_state = hgraph::test_detail::child_state(*dict_state, initial_slot);
    REQUIRE(initial_child_state != nullptr);

    hgraph::test_detail::RecordingNotifiable recorder;

    // boundary_notifier wires TargetLinkState.scheduling_notifier → recorder
    auto key_view = input.view(&recorder).as_bundle()[0].as_dict().at(key.view());

    key_view.make_active();
    initial_child_state->mark_modified(hgraph::test_detail::tick(31));
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(31)});

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(32));
        mutation.removing(key.view());
    }

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(30));
        mutation.setting(key.view(), hgraph::Value{2.0f}.view());
    }

    const size_t rebound_slot = hgraph::test_detail::find_live_dict_slot(output, key);
    REQUIRE(rebound_slot != SIZE_MAX);
    auto *rebound_child_state = hgraph::test_detail::child_state(*dict_state, rebound_slot);
    REQUIRE(rebound_child_state != nullptr);

    rebound_child_state->mark_modified(hgraph::test_detail::tick(32));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(31), hgraph::test_detail::tick(32)});
}

TEST_CASE("TSD child state tracks slot reuse directly from mutations", "[ts_value][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *dict_ts = ts_registry.tsd(str_type, scalar_ts);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    auto *dict_state = static_cast<hgraph::TSDState *>(output.view_context().ts_state);
    REQUIRE(dict_state != nullptr);

    const hgraph::Value key_a{std::string{"a"}};
    const hgraph::Value key_b{std::string{"b"}};

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(31));
        mutation.setting(key_a.view(), hgraph::Value{1.0f}.view());
    }

    const size_t first_slot = hgraph::test_detail::find_live_dict_slot(output, key_a);
    REQUIRE(first_slot != SIZE_MAX);
    auto *first_child_state = hgraph::test_detail::child_state(*dict_state, first_slot);
    REQUIRE(first_child_state != nullptr);

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(32));
        mutation.removing(key_a.view());
    }

    // Removed dict payloads remain retained by stable slot until the next
    // outer mutation begins, so the TS child subtree is still present here.
    CHECK(hgraph::test_detail::child_state(*dict_state, first_slot) == first_child_state);

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
    }

    // Once the next mutation epoch begins, removed slots are physically
    // released and the parallel TS child state must be torn down without any
    // extra navigation step.
    CHECK(hgraph::test_detail::child_state(*dict_state, first_slot) == nullptr);

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_b.view(), hgraph::Value{2.0f}.view());
    }

    const size_t rebound_slot = hgraph::test_detail::find_live_dict_slot(output, key_b);
    REQUIRE(rebound_slot != SIZE_MAX);
    CHECK(rebound_slot == first_slot);

    auto *rebound_child_state = hgraph::test_detail::child_state(*dict_state, rebound_slot);
    REQUIRE(rebound_child_state != nullptr);
}

TEST_CASE("TSInput linked TSD key re-add notifies when state already modified at current tick", "[ts_input][active]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *str_type = value_registry.register_type<std::string>("str");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *dict_ts = ts_registry.tsd(str_type, scalar_ts);
    const auto *input_schema = ts_registry.tsb({{"ts", dict_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    input.view().as_bundle()[0].bind_output(output.view());

    const hgraph::Value key{std::string{"k"}};

    // Add key and navigate so the slot is materialized.
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(30));
        mutation.setting(key.view(), hgraph::Value{1.0f}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(30)).as_dict().at(key.view()),
                                                   hgraph::test_detail::tick(30));

    auto *dict_state = static_cast<hgraph::TSDState *>(output.view_context().ts_state);
    REQUIRE(dict_state != nullptr);

    const size_t initial_slot = hgraph::test_detail::find_live_dict_slot(output, key);
    REQUIRE(initial_slot != SIZE_MAX);
    auto *child_state = hgraph::test_detail::child_state(*dict_state, initial_slot);
    REQUIRE(child_state != nullptr);

    hgraph::test_detail::RecordingNotifiable recorder;
    const auto eval_time = hgraph::test_detail::tick(40);

    // Activate the key at evaluation time so initial-notification logic is live.
    auto key_view = input.view(&recorder, eval_time).as_bundle()[0].as_dict().at(key.view());
    key_view.make_active();

    // The output child is not modified at the current tick, so no initial notification.
    CHECK(recorder.notifications.empty());

    // Now mark modified, confirm we receive the notification.
    child_state->mark_modified(eval_time);
    REQUIRE(recorder.notifications.size() == 1);
    CHECK(recorder.notifications[0] == eval_time);

    // Remove the key — evicts the active trie subtree to pending.
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(31));
        mutation.removing(key.view());
    }

    // Re-add the key and mark the new slot as already modified at the
    // current evaluation tick BEFORE the input navigates to it.
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::tick(32));
        mutation.setting(key.view(), hgraph::Value{3.0f}.view());
    }

    const size_t rebound_slot = hgraph::test_detail::find_live_dict_slot(output, key);
    REQUIRE(rebound_slot != SIZE_MAX);
    auto *rebound_child_state = hgraph::test_detail::child_state(*dict_state, rebound_slot);
    REQUIRE(rebound_child_state != nullptr);
    rebound_child_state->mark_modified(eval_time);

    // Navigate the input to the rebound key — this should resolve_pending
    // and, since the trie subtree was preserved with locally_active, the
    // subscription should be re-established. The state is already modified
    // at the current tick, so we expect an immediate notification.
    auto rebound_view = input.view(&recorder, eval_time).as_bundle()[0].as_dict().at(key.view());

    // We should have received the initial notification for the rebound slot.
    CHECK(recorder.notifications.size() == 2);
    CHECK(recorder.notifications[1] == eval_time);
}

TEST_CASE("TSInput make_active during evaluation notifies immediately if state already modified", "[ts_input][active]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *input_schema = ts_registry.tsb({{"ts", scalar_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};
    input.view().as_bundle()[0].bind_output(output.view());

    auto *output_state = static_cast<hgraph::BaseState *>(output.view_context().ts_state);
    REQUIRE(output_state != nullptr);

    // Mark the output as modified at tick(5) before activation.
    output_state->mark_modified(hgraph::test_detail::tick(5));

    hgraph::test_detail::RecordingNotifiable recorder;

    SECTION("no initial notification when evaluation_time is MIN_DT (wiring phase)") {
        auto leaf_view = input.view(&recorder).as_bundle()[0];
        leaf_view.make_active();
        CHECK(recorder.notifications.empty());
    }

    SECTION("initial notification when evaluation_time matches modified time") {
        auto leaf_view = input.view(&recorder, hgraph::test_detail::tick(5)).as_bundle()[0];
        leaf_view.make_active();
        REQUIRE(recorder.notifications.size() == 1);
        CHECK(recorder.notifications[0] == hgraph::test_detail::tick(5));
    }

    SECTION("no initial notification when evaluation_time is after modified time") {
        auto leaf_view = input.view(&recorder, hgraph::test_detail::tick(6)).as_bundle()[0];
        leaf_view.make_active();
        CHECK(recorder.notifications.empty());
    }
}

TEST_CASE("TSOutput wraps TSD values into cached REF alternatives", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const auto *ref_dict_ts = ts_registry.tsd(int_type, ts_registry.ref(scalar_ts));
    const hgraph::Value key_one{1};
    const hgraph::Value key_two{2};

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{10}.view());
        mutation.setting(key_two.view(), hgraph::Value{20}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(200)), hgraph::test_detail::tick(200));

    const auto alternative_a = output.bindable_view(output.view(hgraph::test_detail::tick(200)), ref_dict_ts);
    const auto alternative_b = output.bindable_view(output.view(hgraph::test_detail::tick(200)), ref_dict_ts);

    CHECK(alternative_a.linked_context().ts_state == alternative_b.linked_context().ts_state);
    CHECK(alternative_a.linked_context().value_data == alternative_b.linked_context().value_data);

    auto wrapped_dict = alternative_a.as_dict();
    const auto ref_one = wrapped_dict.at(key_one.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();
    const auto ref_two = wrapped_dict.at(key_two.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();

    CHECK(ref_one.is_peered());
    CHECK(ref_two.is_peered());
    CHECK(ref_one.target_view(hgraph::test_detail::tick(200)).value().as_atomic().as<int>() == 10);
    CHECK(ref_two.target_view(hgraph::test_detail::tick(200)).value().as_atomic().as<int>() == 20);

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{15}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(201)).as_dict().at(key_one.view()),
                                                   hgraph::test_detail::tick(201));

    const auto updated_ref =
        alternative_a.as_dict().at(key_one.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();
    CHECK(updated_ref.target_view(hgraph::test_detail::tick(201)).value().as_atomic().as<int>() == 15);

    const hgraph::MapDeltaView delta{alternative_a.delta_value()};
    std::vector<int> updated_keys;
    for (const hgraph::View &key : delta.updated_keys()) { updated_keys.push_back(key.as_atomic().as<int>()); }
    CHECK(updated_keys == std::vector<int>{1});
}

TEST_CASE("TSOutput TSD REF alternatives follow key removal and reinsertion", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const auto *ref_dict_ts = ts_registry.tsd(int_type, ts_registry.ref(scalar_ts));
    const hgraph::Value key{1};

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key.view(), hgraph::Value{10}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(210)), hgraph::test_detail::tick(210));

    const auto alternative = output.bindable_view(output.view(hgraph::test_detail::tick(210)), ref_dict_ts);
    REQUIRE(alternative.as_dict().at(key.view()).valid());

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.removing(key.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(211)), hgraph::test_detail::tick(211));

    CHECK_FALSE(alternative.as_dict().at(key.view()).valid());

    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key.view(), hgraph::Value{30}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(212)), hgraph::test_detail::tick(212));

    const auto rebound_ref =
        alternative.as_dict().at(key.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();
    CHECK(rebound_ref.is_peered());
    CHECK(rebound_ref.target_view(hgraph::test_detail::tick(212)).value().as_atomic().as<int>() == 30);

    const hgraph::MapDeltaView delta{alternative.delta_value()};
    std::vector<int> added_keys;
    for (const hgraph::View &added_key : delta.added_keys()) { added_keys.push_back(added_key.as_atomic().as<int>()); }
    CHECK(added_keys == std::vector<int>{1});
}

TEST_CASE("TSOutput TSD REF alternatives do not double-notify the root for child-only updates", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const auto *ref_dict_ts = ts_registry.tsd(int_type, ts_registry.ref(scalar_ts));
    const hgraph::Value key{1};

    hgraph::test_detail::ExposedTSOutput output{hgraph::test_detail::output_builder_for(dict_ts)};
    {
        auto mutation = output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key.view(), hgraph::Value{10}.view());
    }
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(220)), hgraph::test_detail::tick(220));
    {
        // Clear the initial dict delta bookkeeping so the next tick exercises
        // only child-state propagation, not a replay of the original insert.
        output.dict_value().clear_delta_tracking();
    }

    const auto alternative = output.bindable_view(output.view(hgraph::test_detail::tick(220)), ref_dict_ts);
    auto *alternative_root = alternative.linked_context().ts_state;
    REQUIRE(alternative_root != nullptr);

    hgraph::test_detail::RecordingNotifiable recorder;
    alternative_root->subscribe(&recorder);

    output.view(hgraph::test_detail::tick(221)).as_dict().at(key.view()).value().as_atomic().set(15);
    hgraph::test_detail::mark_output_view_modified(output.view(hgraph::test_detail::tick(221)).as_dict().at(key.view()),
                                                   hgraph::test_detail::tick(221));

    const auto source_delta = output.dict_delta_value();
    const size_t source_slot = hgraph::test_detail::find_live_dict_slot(output, key);
    REQUIRE(source_slot != SIZE_MAX);
    CHECK_FALSE(source_delta.slot_added(source_slot));
    CHECK_FALSE(source_delta.slot_updated(source_slot));

    const auto updated_ref =
        alternative.as_dict().at(key.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();
    CHECK(updated_ref.target_view(hgraph::test_detail::tick(221)).value().as_atomic().as<int>() == 15);
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(221)});
}

TEST_CASE("TSOutput can dereference direct TSD child REF values", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *dict_ref_ts = ts_registry.tsd(int_type, ref_scalar_ts);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const hgraph::Value key_one{1};
    const hgraph::Value key_two{2};

    hgraph::test_detail::ExposedTSOutput first_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput second_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput rebound_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput dict_output{hgraph::test_detail::output_builder_for(dict_ref_ts)};

    first_value.atomic_value().set(50);
    second_value.atomic_value().set(60);
    rebound_value.atomic_value().set(70);
    hgraph::test_detail::mark_output_view_modified(first_value.view(hgraph::test_detail::tick(225)), hgraph::test_detail::tick(225));
    hgraph::test_detail::mark_output_view_modified(second_value.view(hgraph::test_detail::tick(225)), hgraph::test_detail::tick(225));
    hgraph::test_detail::mark_output_view_modified(rebound_value.view(hgraph::test_detail::tick(226)), hgraph::test_detail::tick(226));

    {
        auto mutation = dict_output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_value.view())}.view());
        mutation.setting(key_two.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(second_value.view())}.view());
    }
    hgraph::test_detail::mark_output_view_modified(dict_output.view(hgraph::test_detail::tick(225)), hgraph::test_detail::tick(225));

    const auto alternative_a = dict_output.bindable_view(dict_output.view(hgraph::test_detail::tick(225)), dict_ts);
    const auto alternative_b = dict_output.bindable_view(dict_output.view(hgraph::test_detail::tick(225)), dict_ts);
    CHECK(alternative_a.linked_context().ts_state == alternative_b.linked_context().ts_state);
    CHECK(alternative_a.linked_context().value_data == alternative_b.linked_context().value_data);

    auto dict_view = alternative_a.as_dict();
    CHECK(dict_view.at(key_one.view()).value().as_atomic().as<int>() == 50);
    CHECK(dict_view.at(key_two.view()).value().as_atomic().as<int>() == 60);

    {
        auto mutation = dict_output.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(rebound_value.view())}.view());
        mutation.removing(key_two.view());
    }
    hgraph::test_detail::mark_output_view_modified(dict_output.view(hgraph::test_detail::tick(226)), hgraph::test_detail::tick(226));

    dict_view = alternative_a.as_dict();
    CHECK(dict_view.at(key_one.view()).value().as_atomic().as<int>() == 70);
    CHECK_FALSE(dict_view.at(key_two.view()).valid());

    rebound_value.atomic_value().set(75);
    hgraph::test_detail::mark_output_view_modified(rebound_value.view(hgraph::test_detail::tick(227)), hgraph::test_detail::tick(227));

    dict_view = alternative_a.as_dict();
    CHECK(dict_view.at(key_one.view()).value().as_atomic().as<int>() == 75);
}

TEST_CASE("TSOutput dereferenced REF TSD roots can wrap child values as REF alternatives", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const auto *ref_dict_ts = ts_registry.tsd(int_type, ts_registry.ref(scalar_ts));
    const auto *ref_to_dict_ts = ts_registry.ref(dict_ts);
    const hgraph::Value key_one{1};
    const hgraph::Value key_two{2};
    const hgraph::Value key_three{3};

    hgraph::test_detail::ExposedTSOutput first_dict{hgraph::test_detail::output_builder_for(dict_ts)};
    hgraph::test_detail::ExposedTSOutput second_dict{hgraph::test_detail::output_builder_for(dict_ts)};
    hgraph::test_detail::ExposedTSOutput ref_output{hgraph::test_detail::output_builder_for(ref_to_dict_ts)};

    {
        auto mutation = first_dict.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{10}.view());
        mutation.setting(key_two.view(), hgraph::Value{20}.view());
    }
    hgraph::test_detail::mark_output_view_modified(first_dict.view(hgraph::test_detail::tick(230)), hgraph::test_detail::tick(230));

    {
        auto mutation = second_dict.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{30}.view());
        mutation.setting(key_three.view(), hgraph::Value{40}.view());
    }
    hgraph::test_detail::mark_output_view_modified(second_dict.view(hgraph::test_detail::tick(231)), hgraph::test_detail::tick(231));

    ref_output.atomic_value().set(hgraph::v2::TimeSeriesReference::make(first_dict.view()));
    hgraph::test_detail::mark_output_view_modified(ref_output.view(hgraph::test_detail::tick(230)), hgraph::test_detail::tick(230));

    const auto alternative = ref_output.bindable_view(ref_output.view(hgraph::test_detail::tick(230)), ref_dict_ts);
    auto wrapped_dict = alternative.as_dict();

    auto first_ref = wrapped_dict.at(key_one.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>();
    CHECK(first_ref.is_peered());
    CHECK(first_ref.target_view(hgraph::test_detail::tick(230)).value().as_atomic().as<int>() == 10);
    CHECK(wrapped_dict.at(key_two.view()).value().as_atomic().checked_as<hgraph::v2::TimeSeriesReference>()
              .target_view(hgraph::test_detail::tick(230))
              .value()
              .as_atomic()
              .as<int>() == 20);

    ref_output.atomic_value().set(hgraph::v2::TimeSeriesReference::make(second_dict.view()));
    hgraph::test_detail::mark_output_view_modified(ref_output.view(hgraph::test_detail::tick(231)), hgraph::test_detail::tick(231));

    wrapped_dict = alternative.as_dict();
    CHECK_FALSE(wrapped_dict.at(key_two.view()).valid());
    CHECK(wrapped_dict.at(key_three.view()).valid());
    CHECK(wrapped_dict.at(key_one.view())
              .value()
              .as_atomic()
              .checked_as<hgraph::v2::TimeSeriesReference>()
              .target_view(hgraph::test_detail::tick(231))
              .value()
              .as_atomic()
              .as<int>() == 30);
    CHECK(wrapped_dict.at(key_three.view())
              .value()
              .as_atomic()
              .checked_as<hgraph::v2::TimeSeriesReference>()
              .target_view(hgraph::test_detail::tick(231))
              .value()
              .as_atomic()
              .as<int>() == 40);

    const hgraph::MapDeltaView delta{alternative.delta_value()};
    std::vector<int> removed_keys;
    for (const hgraph::View &removed_key : delta.removed_keys()) { removed_keys.push_back(removed_key.as_atomic().as<int>()); }
    std::vector<int> added_keys;
    for (const hgraph::View &added_key : delta.added_keys()) { added_keys.push_back(added_key.as_atomic().as<int>()); }
    std::vector<int> updated_keys;
    for (const hgraph::View &updated_key : delta.updated_keys()) { updated_keys.push_back(updated_key.as_atomic().as<int>()); }
    CHECK(removed_keys == std::vector<int>{2});
    CHECK(added_keys == std::vector<int>{3});
    CHECK(updated_keys == std::vector<int>{1});
}

TEST_CASE("TSOutput dereferenced REF TSD roots can dereference child REF values", "[ts_output][ref][tsd]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *dict_ref_ts = ts_registry.tsd(int_type, ref_scalar_ts);
    const auto *dict_ts = ts_registry.tsd(int_type, scalar_ts);
    const auto *ref_to_dict_ref_ts = ts_registry.ref(dict_ref_ts);
    const hgraph::Value key_one{1};
    const hgraph::Value key_two{2};

    hgraph::test_detail::ExposedTSOutput first_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput second_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput rebound_value{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput first_dict{hgraph::test_detail::output_builder_for(dict_ref_ts)};
    hgraph::test_detail::ExposedTSOutput second_dict{hgraph::test_detail::output_builder_for(dict_ref_ts)};
    hgraph::test_detail::ExposedTSOutput ref_output{hgraph::test_detail::output_builder_for(ref_to_dict_ref_ts)};

    first_value.atomic_value().set(50);
    second_value.atomic_value().set(60);
    rebound_value.atomic_value().set(70);
    hgraph::test_detail::mark_output_view_modified(first_value.view(hgraph::test_detail::tick(240)), hgraph::test_detail::tick(240));
    hgraph::test_detail::mark_output_view_modified(second_value.view(hgraph::test_detail::tick(240)), hgraph::test_detail::tick(240));
    hgraph::test_detail::mark_output_view_modified(rebound_value.view(hgraph::test_detail::tick(241)), hgraph::test_detail::tick(241));

    {
        auto mutation = first_dict.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_value.view())}.view());
        mutation.setting(key_two.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(second_value.view())}.view());
    }
    hgraph::test_detail::mark_output_view_modified(first_dict.view(hgraph::test_detail::tick(240)), hgraph::test_detail::tick(240));

    {
        auto mutation = second_dict.dict_value().begin_mutation(hgraph::test_detail::next_mutation_tick());
        mutation.setting(key_one.view(), hgraph::Value{hgraph::v2::TimeSeriesReference::make(rebound_value.view())}.view());
    }
    hgraph::test_detail::mark_output_view_modified(second_dict.view(hgraph::test_detail::tick(241)), hgraph::test_detail::tick(241));

    ref_output.atomic_value().set(hgraph::v2::TimeSeriesReference::make(first_dict.view()));
    hgraph::test_detail::mark_output_view_modified(ref_output.view(hgraph::test_detail::tick(240)), hgraph::test_detail::tick(240));

    const auto alternative = ref_output.bindable_view(ref_output.view(hgraph::test_detail::tick(240)), dict_ts);
    auto dict_view = alternative.as_dict();
    CHECK(dict_view.at(key_one.view()).value().as_atomic().as<int>() == 50);
    CHECK(dict_view.at(key_two.view()).value().as_atomic().as<int>() == 60);

    ref_output.atomic_value().set(hgraph::v2::TimeSeriesReference::make(second_dict.view()));
    hgraph::test_detail::mark_output_view_modified(ref_output.view(hgraph::test_detail::tick(241)), hgraph::test_detail::tick(241));

    dict_view = alternative.as_dict();
    CHECK(dict_view.at(key_one.view()).value().as_atomic().as<int>() == 70);
    CHECK_FALSE(dict_view.at(key_two.view()).valid());
}

TEST_CASE("TSInput active dereferenced bundle child rehomes subscriptions when the REF retargets", "[ts_input][active][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *pair_ts = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
    const auto *ref_pair_ts = ts_registry.tsb({{"lhs", ref_scalar_ts}, {"rhs", ref_scalar_ts}}, "RefPair");
    const auto *input_schema = ts_registry.tsb({{"ts", pair_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput first_lhs{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput first_rhs{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput rebound_lhs{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput ref_output{hgraph::test_detail::output_builder_for(ref_pair_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};

    first_lhs.atomic_value().set(1.0f);
    first_rhs.atomic_value().set(2.0f);
    rebound_lhs.atomic_value().set(3.0f);
    hgraph::test_detail::mark_output_view_modified(first_lhs.view(), hgraph::test_detail::tick(50));
    hgraph::test_detail::mark_output_view_modified(first_rhs.view(), hgraph::test_detail::tick(50));

    {
        auto mutation = ref_output.bundle_value().begin_mutation();
        mutation.setting_field("lhs", hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_lhs.view())}.view());
        mutation.setting_field("rhs", hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_rhs.view())}.view());
    }

    input.view().as_bundle()[0].bind_output(ref_output.view());

    auto lhs_view = input.view().as_bundle()[0].as_bundle()[0];
    auto rhs_view = input.view().as_bundle()[0].as_bundle()[1];
    REQUIRE(lhs_view.valid());
    REQUIRE(rhs_view.valid());

    hgraph::test_detail::RecordingNotifiable recorder;
    auto active_lhs = input.view(&recorder).as_bundle()[0].as_bundle()[0];
    active_lhs.make_active();
    REQUIRE(active_lhs.active());

    hgraph::test_detail::mark_output_view_modified(first_lhs.view(), hgraph::test_detail::tick(51));
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(51)});

    {
        auto mutation = ref_output.bundle_value().begin_mutation();
        mutation.setting_field("lhs", hgraph::Value{hgraph::v2::TimeSeriesReference::make(rebound_lhs.view())}.view());
    }
    hgraph::test_detail::mark_output_view_modified(ref_output.view().as_bundle()[0], hgraph::test_detail::tick(52));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(51), hgraph::test_detail::tick(52)});

    hgraph::test_detail::mark_output_view_modified(first_lhs.view(), hgraph::test_detail::tick(53));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(51), hgraph::test_detail::tick(52)});

    hgraph::test_detail::mark_output_view_modified(rebound_lhs.view(), hgraph::test_detail::tick(54));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(51), hgraph::test_detail::tick(52),
                                             hgraph::test_detail::tick(54)});
}

TEST_CASE("TSInput active dereferenced list element rehomes subscriptions when the REF retargets", "[ts_input][active][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *float_type = value_registry.register_type<float>("float");
    const auto *scalar_ts = ts_registry.ts(float_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *list_ts = ts_registry.tsl(scalar_ts, 2);
    const auto *ref_list_ts = ts_registry.tsl(ref_scalar_ts, 2);
    const auto *input_schema = ts_registry.tsb({{"ts", list_ts}}, "InputBundle");
    const auto plan = hgraph::test_detail::single_link_plan(input_schema, 0);

    hgraph::test_detail::ExposedTSOutput first_zero{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput first_one{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput rebound_one{hgraph::test_detail::output_builder_for(scalar_ts)};
    hgraph::test_detail::ExposedTSOutput ref_output{hgraph::test_detail::output_builder_for(ref_list_ts)};
    hgraph::test_detail::ExposedTSInput input{hgraph::test_detail::builder_for(plan)};

    first_zero.atomic_value().set(4.0f);
    first_one.atomic_value().set(5.0f);
    rebound_one.atomic_value().set(6.0f);
    hgraph::test_detail::mark_output_view_modified(first_zero.view(), hgraph::test_detail::tick(60));
    hgraph::test_detail::mark_output_view_modified(first_one.view(), hgraph::test_detail::tick(60));

    {
        auto mutation = ref_output.list_value().begin_mutation();
        mutation.setting(0, hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_zero.view())}.view());
        mutation.setting(1, hgraph::Value{hgraph::v2::TimeSeriesReference::make(first_one.view())}.view());
    }

    input.view().as_bundle()[0].bind_output(ref_output.view());

    auto first_view = input.view().as_bundle()[0].as_list()[0];
    auto second_view = input.view().as_bundle()[0].as_list()[1];
    REQUIRE(first_view.valid());
    REQUIRE(second_view.valid());

    hgraph::test_detail::RecordingNotifiable recorder;
    auto active_second = input.view(&recorder).as_bundle()[0].as_list()[1];
    active_second.make_active();
    REQUIRE(active_second.active());

    hgraph::test_detail::mark_output_view_modified(first_one.view(), hgraph::test_detail::tick(61));
    CHECK(recorder.notifications == std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(61)});

    {
        auto mutation = ref_output.list_value().begin_mutation();
        mutation.setting(1, hgraph::Value{hgraph::v2::TimeSeriesReference::make(rebound_one.view())}.view());
    }
    hgraph::test_detail::mark_output_view_modified(ref_output.view().as_list()[1], hgraph::test_detail::tick(62));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(61), hgraph::test_detail::tick(62)});

    hgraph::test_detail::mark_output_view_modified(first_one.view(), hgraph::test_detail::tick(63));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(61), hgraph::test_detail::tick(62)});

    hgraph::test_detail::mark_output_view_modified(rebound_one.view(), hgraph::test_detail::tick(64));
    CHECK(recorder.notifications ==
          std::vector<hgraph::engine_time_t>{hgraph::test_detail::tick(61), hgraph::test_detail::tick(62),
                                             hgraph::test_detail::tick(64)});
}
