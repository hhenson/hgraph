#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <vector>

namespace
{
    struct ExposedValueView : hgraph::View
    {
        explicit ExposedValueView(const hgraph::View &view)
            : hgraph::View(view)
        {
        }

        using hgraph::View::data;
    };
}  // namespace

TEST_CASE("Set values support add contains and remove")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto set = value.view().as_set();

    auto one = hgraph::value_for(int32_t{1});
    auto two = hgraph::value_for(int32_t{2});

    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.add(one.view()));
        CHECK(mutation.add(two.view()));
        CHECK_FALSE(mutation.add(one.view()));
    }
    CHECK(set.contains(one.view()));
    CHECK(set.contains(two.view()));
    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.remove(one.view()));
    }
    CHECK_FALSE(set.contains(one.view()));
    CHECK(set.size() == 1);
}

TEST_CASE("Set mutation views support exact reserve", "[time_series][value][associative]")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto set = value.set_view();

    auto mutation = set.begin_mutation();
    mutation.reserving(32).adding(int32_t{1}).adding(int32_t{2}).adding(int32_t{3});

    CHECK(set.size() == 3);
    CHECK(set.contains(hgraph::value_for(int32_t{1}).view()));
    CHECK(set.contains(hgraph::value_for(int32_t{2}).view()));
    CHECK(set.contains(hgraph::value_for(int32_t{3}).view()));
}

TEST_CASE("Set values retain removed payloads by slot until reuse")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto set = value.set_view();
    auto delta = set.delta();

    auto one = hgraph::value_for(int32_t{1});
    auto two = hgraph::value_for(int32_t{2});

    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.add(one.view()));
        CHECK(mutation.add(two.view()));
    }

    size_t removed_slot = static_cast<size_t>(-1);
    for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
        if (delta.slot_occupied(slot) && delta.at_slot(slot).as_atomic().as<int32_t>() == 1) {
            removed_slot = slot;
            break;
        }
    }

    REQUIRE(removed_slot != static_cast<size_t>(-1));
    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.remove(one.view()));
    }
    CHECK(delta.slot_occupied(removed_slot));
    CHECK(delta.slot_removed(removed_slot));
    CHECK(delta.at_slot(removed_slot).as_atomic().as<int32_t>() == 1);

    auto mutation_set = set.begin_mutation();
    CHECK_FALSE(delta.slot_occupied(removed_slot));
}

TEST_CASE("Map values support lookup and require live values")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.map(hgraph::value::scalar_type_meta<std::string>(),
                                      hgraph::value::scalar_type_meta<int32_t>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto map = value.view().as_map();

    auto key = hgraph::value_for(std::string{"alpha"});
    auto val = hgraph::value_for(int32_t{42});

    {
        auto mutation = map.begin_mutation();
        mutation.set(key.view(), val.view());
    }
    CHECK(map.contains(key.view()));
    CHECK(map.at(key.view()).as_atomic().as<int32_t>() == 42);

    CHECK_THROWS([&]() {
        auto mutation = map.begin_mutation();
        mutation.set(key.view(), hgraph::View::invalid_for(hgraph::value::scalar_type_meta<int32_t>()));
    }());
}

TEST_CASE("Map mutation views support exact reserve", "[time_series][value][associative]")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto map = value.map_view();

    auto mutation = map.begin_mutation();
    mutation.reserving(24).setting(int32_t{1}, int32_t{10}).setting(int32_t{2}, int32_t{20});

    CHECK(map.size() == 2);
    CHECK(map.at(hgraph::value_for(int32_t{1}).view()).as_atomic().as<int32_t>() == 10);
    CHECK(map.at(hgraph::value_for(int32_t{2}).view()).as_atomic().as<int32_t>() == 20);
}

TEST_CASE("Map values retain removed key and value payloads by slot until reuse")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema =
        registry.map(hgraph::value::scalar_type_meta<std::string>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto map = value.map_view();
    auto delta = map.delta();

    auto key = hgraph::value_for(std::string{"alpha"});
    auto val = hgraph::value_for(int32_t{42});
    auto other_key = hgraph::value_for(std::string{"beta"});
    auto other_val = hgraph::value_for(int32_t{7});

    {
        auto mutation = map.begin_mutation();
        mutation.set(key.view(), val.view());
        mutation.set(other_key.view(), other_val.view());
    }

    size_t removed_slot = static_cast<size_t>(-1);
    for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
        if (delta.slot_occupied(slot) && delta.key_at_slot(slot).as_atomic().as<std::string>() == "alpha") {
            removed_slot = slot;
            break;
        }
    }

    REQUIRE(removed_slot != static_cast<size_t>(-1));
    {
        auto mutation = map.begin_mutation();
        CHECK(mutation.remove(key.view()));
    }
    CHECK(delta.slot_occupied(removed_slot));
    CHECK(delta.slot_removed(removed_slot));
    CHECK(delta.key_at_slot(removed_slot).as_atomic().as<std::string>() == "alpha");
    CHECK(delta.value_at_slot(removed_slot).as_atomic().as<int32_t>() == 42);

    auto mutation_map = map.begin_mutation();
    CHECK_FALSE(delta.slot_occupied(removed_slot));
}

TEST_CASE("Delta map value slot payload addresses stay stable across reserve")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema =
        registry.map(hgraph::value::scalar_type_meta<std::string>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto map = value.map_view();
    auto delta = map.delta();

    {
        auto mutation = map.begin_mutation();
        mutation.set(hgraph::value_for(std::string{"alpha"}).view(), hgraph::value_for(int32_t{42}).view());
    }

    size_t alpha_slot = static_cast<size_t>(-1);
    for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
        if (!delta.slot_occupied(slot)) { continue; }
        if (delta.key_at_slot(slot).as_atomic().as<std::string>() == "alpha") {
            alpha_slot = slot;
            break;
        }
    }

    REQUIRE(alpha_slot != static_cast<size_t>(-1));
    const void *alpha_value_data = ExposedValueView{delta.value_at_slot(alpha_slot)}.data();

    {
        auto mutation = map.begin_mutation();
        mutation.reserving(64);
        for (int32_t i = 0; i < 16; ++i) {
            mutation.setting(hgraph::value_for(std::string{"k" + std::to_string(i)}).view(),
                             hgraph::value_for(i + 10).view());
        }
    }

    CHECK(ExposedValueView{delta.value_at_slot(alpha_slot)}.data() == alpha_value_data);
    CHECK(delta.value_at_slot(alpha_slot).as_atomic().as<int32_t>() == 42);
}

TEST_CASE("Set values handle larger churn without losing membership semantics")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
    auto set = value.set_view();

    {
        auto mutation = set.begin_mutation();
        for (int32_t i = 0; i < 5000; ++i) {
            INFO(i);
            CHECK(mutation.add(hgraph::value_for(i).view()));
        }
    }
    CHECK(set.size() == 5000);

    {
        auto mutation = set.begin_mutation();
        for (int32_t i = 0; i < 5000; i += 2) {
            INFO(i);
            CHECK(mutation.remove(hgraph::value_for(i).view()));
        }
    }
    CHECK(set.size() == 2500);

    for (int32_t i = 1; i < 5000; i += 2) {
        INFO(i);
        CHECK(set.contains(hgraph::value_for(i).view()));
    }

    {
        auto mutation = set.begin_mutation();
        for (int32_t i = 0; i < 5000; i += 2) {
            INFO(i);
            CHECK(mutation.add(hgraph::value_for(i).view()));
        }
    }
    CHECK(set.size() == 5000);
}

TEST_CASE("Map values handle larger churn and overwrites")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
    auto map = value.map_view();

    {
        auto mutation = map.begin_mutation();
        for (int32_t i = 0; i < 3000; ++i) {
            mutation.set(hgraph::value_for(i).view(), hgraph::value_for(i * 10).view());
        }
    }
    CHECK(map.size() == 3000);

    {
        auto mutation = map.begin_mutation();
        for (int32_t i = 0; i < 3000; i += 3) {
            CHECK(mutation.remove(hgraph::value_for(i).view()));
        }
    }

    for (int32_t i = 1; i < 3000; i += 3) {
        CHECK(map.contains(hgraph::value_for(i).view()));
        CHECK(map.at(hgraph::value_for(i).view()).as_atomic().as<int32_t>() == i * 10);
    }

    {
        auto mutation = map.begin_mutation();
        for (int32_t i = 0; i < 3000; i += 3) {
            mutation.set(hgraph::value_for(i).view(), hgraph::value_for(i * 100).view());
            CHECK(map.contains(hgraph::value_for(i).view()));
            CHECK(map.at(hgraph::value_for(i).view()).as_atomic().as<int32_t>() == i * 100);
        }
    }
}

TEST_CASE("Associative mutation scopes support nesting with depth tracking")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value set_value{*set_schema, hgraph::MutationTracking::Delta};
    auto set = set_value.set_view();
    auto set_delta = set.delta();
    auto one = hgraph::value_for(int32_t{1});
    {
        auto outer = set.begin_mutation();
        CHECK(outer.add(one.view()));
        CHECK(outer.remove(one.view()));
        {
            auto inner = set.begin_mutation();
            CHECK_FALSE(set_delta.slot_occupied(0));
        }
        CHECK_FALSE(set_delta.slot_occupied(0));
    }
    {
        auto reopen = set.begin_mutation();
        CHECK_FALSE(set_delta.slot_occupied(0));
        CHECK_FALSE(reopen.remove(one.view()));
    }

    hgraph::Value map_value{*map_schema, hgraph::MutationTracking::Delta};
    auto map = map_value.map_view();
    auto map_delta = map.delta();
    auto key = hgraph::value_for(int32_t{7});
    auto value = hgraph::value_for(int32_t{70});
    {
        auto outer = map.begin_mutation();
        outer.set(key.view(), value.view());
        CHECK(outer.remove(key.view()));
        {
            auto inner = map.begin_mutation();
            CHECK_FALSE(map_delta.slot_occupied(0));
        }
        CHECK_FALSE(map_delta.slot_occupied(0));
    }
    {
        auto reopen = map.begin_mutation();
        CHECK_FALSE(map_delta.slot_occupied(0));
        CHECK_FALSE(reopen.remove(key.view()));
    }

    hgraph::Value existing_set_value{*set_schema, hgraph::MutationTracking::Delta};
    auto existing_set = existing_set_value.set_view();
    auto existing_set_delta = existing_set.delta();
    existing_set.begin_mutation().adding(int32_t{2});
    {
        auto outer = existing_set.begin_mutation();
        CHECK(outer.remove(hgraph::value_for(int32_t{2}).view()));
        {
            auto inner = existing_set.begin_mutation();
            CHECK(existing_set_delta.slot_occupied(0));
        }
        CHECK(existing_set_delta.slot_occupied(0));
    }

    hgraph::Value existing_map_value{*map_schema, hgraph::MutationTracking::Delta};
    auto existing_map = existing_map_value.map_view();
    auto existing_map_delta = existing_map.delta();
    existing_map.begin_mutation().setting(int32_t{9}, int32_t{90});
    {
        auto outer = existing_map.begin_mutation();
        CHECK(outer.remove(hgraph::value_for(int32_t{9}).view()));
        {
            auto inner = existing_map.begin_mutation();
            CHECK(existing_map_delta.slot_occupied(0));
        }
        CHECK(existing_map_delta.slot_occupied(0));
    }
}

TEST_CASE("Associative builders keep plain storage smaller than delta storage")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    CHECK(hgraph::ValueBuilderFactory::checked_builder_for(set_schema, hgraph::MutationTracking::Plain).size() <
          hgraph::ValueBuilderFactory::checked_builder_for(set_schema, hgraph::MutationTracking::Delta).size());
    CHECK(hgraph::ValueBuilderFactory::checked_builder_for(map_schema, hgraph::MutationTracking::Plain).size() <
          hgraph::ValueBuilderFactory::checked_builder_for(map_schema, hgraph::MutationTracking::Delta).size());
}

TEST_CASE("Associative delta views expose net added and removed slots")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value set_value{*set_schema, hgraph::MutationTracking::Delta};
    auto set = set_value.set_view();
    auto set_delta = set.delta();
    set.begin_mutation().adding(int32_t{1}).adding(int32_t{2}).removing(int32_t{1});
    CHECK_FALSE(set.contains(hgraph::value_for(int32_t{1}).view()));
    CHECK(set.contains(hgraph::value_for(int32_t{2}).view()));
    std::vector<int32_t> set_added;
    for (auto entry : set_delta.added()) {
        set_added.push_back(entry.as_atomic().as<int32_t>());
    }
    CHECK(set_added == std::vector<int32_t>{2});
    for (auto entry : set_delta.removed()) {
        FAIL_CHECK("Unexpected removed set delta entry: " << entry.as_atomic().as<int32_t>());
    }

    hgraph::Value map_value{*map_schema, hgraph::MutationTracking::Delta};
    auto map = map_value.map_view();
    auto map_delta = map.delta();
    map.begin_mutation().setting(int32_t{7}, int32_t{70}).setting(int32_t{8}, int32_t{80}).removing(int32_t{7});
    CHECK_FALSE(map.contains(hgraph::value_for(int32_t{7}).view()));
    CHECK(map.contains(hgraph::value_for(int32_t{8}).view()));
    std::vector<std::pair<int32_t, int32_t>> map_added;
    for (auto [key, value] : map_delta.added_items()) {
        map_added.emplace_back(key.as_atomic().as<int32_t>(), value.as_atomic().as<int32_t>());
    }
    CHECK(map_added == std::vector<std::pair<int32_t, int32_t>>{{8, 80}});
    for (auto [key, value] : map_delta.removed_items()) {
        FAIL_CHECK("Unexpected removed map delta entry: "
                   << key.as_atomic().as<int32_t>() << " -> " << value.as_atomic().as<int32_t>());
    }
}

TEST_CASE("Map delta views expose updated items for existing keys")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value map_value{*map_schema, hgraph::MutationTracking::Delta};
    auto map = map_value.map_view();
    auto delta = map.delta();

    map.begin_mutation().setting(int32_t{7}, int32_t{70});
    map.begin_mutation().setting(int32_t{7}, int32_t{700});

    std::vector<std::pair<int32_t, int32_t>> updated;
    for (auto [key, value] : delta.updated_items()) {
        updated.emplace_back(key.as_atomic().as<int32_t>(), value.as_atomic().as<int32_t>());
    }
    CHECK(updated == std::vector<std::pair<int32_t, int32_t>>{{7, 700}});
}

TEST_CASE("Associative mutation scopes support fluent command-style chaining")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value set_value{*set_schema};
    auto set = set_value.set_view();
    auto one = hgraph::value_for(int32_t{1});
    auto two = hgraph::value_for(int32_t{2});
    set.begin_mutation().adding(one.view()).adding(two.view()).removing(one.view());
    CHECK_FALSE(set.contains(one.view()));
    CHECK(set.contains(two.view()));

    hgraph::Value map_value{*map_schema};
    auto map = map_value.map_view();
    auto key_a = hgraph::value_for(int32_t{10});
    auto val_a = hgraph::value_for(int32_t{100});
    auto key_b = hgraph::value_for(int32_t{20});
    auto val_b = hgraph::value_for(int32_t{200});
    map.begin_mutation().setting(key_a.view(), val_a.view()).setting(key_b.view(), val_b.view()).removing(key_a.view());
    CHECK_FALSE(map.contains(key_a.view()));
    CHECK(map.contains(key_b.view()));
    CHECK(map.at(key_b.view()).as_atomic().as<int32_t>() == 200);
}

TEST_CASE("Associative mutation scopes accept native C++ values directly")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<std::string>()).build();

    hgraph::Value set_value{*set_schema};
    auto set = set_value.set_view();
    set.begin_mutation().adding(int32_t{1}).adding(int32_t{2}).removing(int32_t{1});
    CHECK_FALSE(set.contains(hgraph::value_for(int32_t{1}).view()));
    CHECK(set.contains(hgraph::value_for(int32_t{2}).view()));

    hgraph::Value map_value{*map_schema};
    auto map = map_value.map_view();
    map.begin_mutation().setting(int32_t{7}, std::string{"seven"}).setting(int32_t{8}, std::string{"eight"}).removing(int32_t{7});
    CHECK_FALSE(map.contains(hgraph::value_for(int32_t{7}).view()));
    CHECK(map.at(hgraph::value_for(int32_t{8}).view()).as_atomic().as<std::string>() == "eight");
}

TEST_CASE("Plain associative values do not retain delta markers or removed payloads")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *set_schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();
    const auto *map_schema =
        registry.map(hgraph::value::scalar_type_meta<int32_t>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value set_value{*set_schema, hgraph::MutationTracking::Plain};
    auto set = set_value.set_view();
    set.begin_mutation().adding(int32_t{1}).adding(int32_t{2}).removing(int32_t{1});

    auto set_delta = set.delta();
    std::vector<int32_t> set_added;
    for (auto entry : set_delta.added()) {
        set_added.push_back(entry.as_atomic().as<int32_t>());
    }
    std::vector<int32_t> set_removed;
    for (auto entry : set_delta.removed()) {
        set_removed.push_back(entry.as_atomic().as<int32_t>());
    }
    CHECK(set_added.empty());
    CHECK(set_removed.empty());
    CHECK_FALSE(set.contains(hgraph::value_for(int32_t{1}).view()));

    hgraph::Value map_value{*map_schema, hgraph::MutationTracking::Plain};
    auto map = map_value.map_view();
    map.begin_mutation().setting(int32_t{7}, int32_t{70}).setting(int32_t{8}, int32_t{80}).removing(int32_t{7});

    auto map_delta = map.delta();
    std::vector<int32_t> added_keys;
    for (auto key : map_delta.added_keys()) {
        added_keys.push_back(key.as_atomic().as<int32_t>());
    }
    std::vector<int32_t> removed_keys;
    for (auto key : map_delta.removed_keys()) {
        removed_keys.push_back(key.as_atomic().as<int32_t>());
    }
    std::vector<int32_t> updated_keys;
    for (auto key : map_delta.updated_keys()) {
        updated_keys.push_back(key.as_atomic().as<int32_t>());
    }
    std::vector<std::pair<int32_t, int32_t>> added_items;
    for (auto [key, value] : map_delta.added_items()) {
        added_items.emplace_back(key.as_atomic().as<int32_t>(), value.as_atomic().as<int32_t>());
    }
    std::vector<std::pair<int32_t, int32_t>> removed_items;
    for (auto [key, value] : map_delta.removed_items()) {
        removed_items.emplace_back(key.as_atomic().as<int32_t>(), value.as_atomic().as<int32_t>());
    }
    std::vector<std::pair<int32_t, int32_t>> updated_items;
    for (auto [key, value] : map_delta.updated_items()) {
        updated_items.emplace_back(key.as_atomic().as<int32_t>(), value.as_atomic().as<int32_t>());
    }
    CHECK(added_keys.empty());
    CHECK(removed_keys.empty());
    CHECK(updated_keys.empty());
    CHECK(added_items.empty());
    CHECK(removed_items.empty());
    CHECK(updated_items.empty());
    CHECK_FALSE(map.contains(hgraph::value_for(int32_t{7}).view()));
}
