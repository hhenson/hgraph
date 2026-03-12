#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

TEST_CASE("Set values support add contains and remove")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
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

TEST_CASE("Set values retain removed payloads by slot until reuse")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
    auto set = value.set_view();

    auto one = hgraph::value_for(int32_t{1});
    auto two = hgraph::value_for(int32_t{2});

    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.add(one.view()));
        CHECK(mutation.add(two.view()));
    }

    size_t removed_slot = static_cast<size_t>(-1);
    for (size_t slot = 0; slot < set.slot_capacity(); ++slot) {
        if (set.slot_occupied(slot) && set.at_slot(slot).as_atomic().as<int32_t>() == 1) {
            removed_slot = slot;
            break;
        }
    }

    REQUIRE(removed_slot != static_cast<size_t>(-1));
    {
        auto mutation = set.begin_mutation();
        CHECK(mutation.remove(one.view()));
    }
    CHECK(set.slot_occupied(removed_slot));
    CHECK(set.at_slot(removed_slot).as_atomic().as<int32_t>() == 1);

    auto mutation_set = set.begin_mutation();
    CHECK_FALSE(set.slot_occupied(removed_slot));
}

TEST_CASE("Map values support lookup and require live values")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.map(hgraph::value::scalar_type_meta<std::string>(),
                                      hgraph::value::scalar_type_meta<int32_t>())
                             .build();

    hgraph::Value value{*schema};
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

TEST_CASE("Map values retain removed key and value payloads by slot until reuse")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema =
        registry.map(hgraph::value::scalar_type_meta<std::string>(), hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
    auto map = value.map_view();

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
    for (size_t slot = 0; slot < map.slot_capacity(); ++slot) {
        if (map.slot_occupied(slot) && map.key_at_slot(slot).as_atomic().as<std::string>() == "alpha") {
            removed_slot = slot;
            break;
        }
    }

    REQUIRE(removed_slot != static_cast<size_t>(-1));
    {
        auto mutation = map.begin_mutation();
        CHECK(mutation.remove(key.view()));
    }
    CHECK(map.slot_occupied(removed_slot));
    CHECK(map.key_at_slot(removed_slot).as_atomic().as<std::string>() == "alpha");
    CHECK(map.value_at_slot(removed_slot).as_atomic().as<int32_t>() == 42);

    auto mutation_map = map.begin_mutation();
    CHECK_FALSE(map.slot_occupied(removed_slot));
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

    hgraph::Value set_value{*set_schema};
    auto set = set_value.set_view();
    auto one = hgraph::value_for(int32_t{1});
    {
        auto outer = set.begin_mutation();
        CHECK(outer.add(one.view()));
        CHECK(outer.remove(one.view()));
        {
            auto inner = set.begin_mutation();
            CHECK(set.slot_occupied(0));
        }
        CHECK(set.slot_occupied(0));
    }
    {
        auto reopen = set.begin_mutation();
        CHECK_FALSE(set.slot_occupied(0));
        CHECK_FALSE(reopen.remove(one.view()));
    }

    hgraph::Value map_value{*map_schema};
    auto map = map_value.map_view();
    auto key = hgraph::value_for(int32_t{7});
    auto value = hgraph::value_for(int32_t{70});
    {
        auto outer = map.begin_mutation();
        outer.set(key.view(), value.view());
        CHECK(outer.remove(key.view()));
        {
            auto inner = map.begin_mutation();
            CHECK(map.slot_occupied(0));
        }
        CHECK(map.slot_occupied(0));
    }
    {
        auto reopen = map.begin_mutation();
        CHECK_FALSE(map.slot_occupied(0));
        CHECK_FALSE(reopen.remove(key.view()));
    }
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
