#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_data/storage.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

namespace
{
    using namespace hgraph;

    struct CheckpointObserver final : Notifiable
    {
        std::size_t notifications{0};
        void notify(DateTime) override { ++notifications; }
    };

    const TSValueTypeMetaData *checkpoint_int_schema()
    {
        auto &registry = TypeRegistry::instance();
        return registry.ts(registry.register_scalar<std::int32_t>("int32"));
    }

    void assign(const TSDataView &view, std::int32_t value, DateTime time)
    {
        Value payload{value};
        auto mutation = view.begin_mutation(time);
        (void)mutation.copy_value_from(payload.view());
    }
}

TEST_CASE("TS checkpoint: atomic import preserves timestamps and never notifies", "[checkpoint]")
{
    const auto *schema = checkpoint_int_schema();
    TSOutput source{schema};
    TSOutput target{schema};
    CheckpointObserver observer;
    target.subscribe(&observer);
    const auto t1 = MIN_ST + TimeDelta{2};
    assign(source.data_view(), 42, t1);
    const auto image = capture_ts_checkpoint(source.data_view());
    const auto owned_copy = image;
    assign(source.data_view(), 99, t1 + TimeDelta{1});

    restore_ts_checkpoint(target.data_view(), owned_copy);
    CHECK(target.data_view().value().checked_as<std::int32_t>() == 42);
    CHECK(target.data_view().last_modified_time() == t1);
    CHECK_FALSE(target.data_view().modified(t1 + TimeDelta{1}));
    CHECK(observer.notifications == 0);
    assign(target.data_view(), 43, t1 + TimeDelta{1});
    CHECK(observer.notifications == 1);
    CHECK_THROWS_AS(restore_ts_checkpoint(target.data_view(), image), std::invalid_argument);
    target.unsubscribe(&observer);
}

TEST_CASE("TS checkpoint: fixed structures retain partial validity and each child clock", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    const auto *list = registry.tsl(ts, 2);
    const auto *schema = registry.tsb("CheckpointPartialBundle", {{"early", ts}, {"late", list}, {"absent", ts}});
    TSOutput source{schema};
    TSOutput target{schema};
    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{1};
    assign(source.data_view().indexed_child_at(0), 10, t1);
    assign(source.data_view().indexed_child_at(1).indexed_child_at(1), 20, t2);
    CheckpointObserver root_observer;
    CheckpointObserver child_observer;
    target.subscribe(&root_observer);
    auto child = target.data_view().indexed_child_at(0);
    child.subscribe(&child_observer);

    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    CHECK(target.data_view().last_modified_time() == t2);
    CHECK(child.last_modified_time() == t1);
    CHECK(child.value().checked_as<std::int32_t>() == 10);
    auto recovered_list = target.data_view().indexed_child_at(1);
    CHECK(recovered_list.last_modified_time() == t2);
    CHECK_FALSE(recovered_list.indexed_child_at(0).has_current_value());
    CHECK(recovered_list.indexed_child_at(1).value().checked_as<std::int32_t>() == 20);
    CHECK_FALSE(target.data_view().indexed_child_at(2).has_current_value());
    CHECK_FALSE(target.data_view().all_valid());
    // Whole-value projection must honor imported child validity as well.
    CHECK(target.data_view().value().as_indexed_view().at(0).has_value());
    CHECK_FALSE(target.data_view().value().as_indexed_view().at(2).has_value());
    CHECK(root_observer.notifications == 0);
    CHECK(child_observer.notifications == 0);
    child.unsubscribe(&child_observer);
    target.unsubscribe(&root_observer);
}

TEST_CASE("TS checkpoint: signal import retains its last tick without producing a new one", "[checkpoint]")
{
    const auto *schema = TypeRegistry::instance().signal();
    TSOutput source{schema};
    TSOutput target{schema};
    source.data_view().begin_mutation(MIN_ST).mark_modified();
    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    CHECK(target.data_view().has_current_value());
    CHECK(target.data_view().last_modified_time() == MIN_ST);
    CHECK_FALSE(target.data_view().modified(MIN_ST + TimeDelta{1}));
}

TEST_CASE("TS checkpoint: atomic polymorphic payload preserves its concrete leaf", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = checkpoint_int_schema()->value_schema;
    const auto *base = registry.bundle("tests.checkpoint", "Event", {{"id", integer}}, {}, true);
    const auto *leaf = registry.bundle("tests.checkpoint", "FilledEvent", {{"id", integer}, {"amount", integer}}, {base});
    const auto *schema = registry.ts(base);
    const auto realization = TypeRealizationSnapshot::capture(registry);
    TypeRealizationScope realization_scope{realization.get()};
    TSOutput source{schema};
    TSOutput target{schema};
    BundleBuilder builder{ValuePlanFactory::instance().type_for(leaf)};
    builder.set("id", Value{std::int32_t{7}});
    builder.set("amount", Value{std::int32_t{50}});
    const Value value = builder.build();
    {
        auto mutation = source.data_view().begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }
    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    CHECK(target.data_view().value().concrete().schema() == leaf);
    CHECK(target.data_view().value().equals(source.data_view().value()));
}

TEST_CASE("TS checkpoint: validation is complete before any fixed child is imported", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tsl(checkpoint_int_schema(), 2);
    TSOutput source{schema};
    TSOutput target{schema};
    assign(source.data_view().indexed_child_at(0), 1, MIN_ST);
    assign(source.data_view().indexed_child_at(1), 2, MIN_ST);
    auto image = capture_ts_checkpoint(source.data_view());
    image.children[1].payload = Value{std::string{"bad schema"}};
    CHECK_THROWS_AS(restore_ts_checkpoint(target.data_view(), image), std::invalid_argument);
    CHECK_FALSE(target.data_view().has_current_value());
    CHECK_FALSE(target.data_view().indexed_child_at(0).has_current_value());
    CHECK_FALSE(target.data_view().indexed_child_at(1).has_current_value());
}

TEST_CASE("TS checkpoint: dictionaries restore membership slots partial values and future removal", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    const auto *child_schema = registry.tsb("CheckpointDictValue", {{"x", ts}, {"y", ts}});
    const auto *schema = registry.tsd(ts->value_schema, child_schema);
    TSOutput source{schema};
    TSOutput target{schema};
    auto source_view = source.data_view();
    auto source_dict = source_view.as_dict();
    const Value one{std::int32_t{1}}, two{std::int32_t{2}}, three{std::int32_t{3}}, four{std::int32_t{4}};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto mutation = source_dict.begin_mutation(t1);
        assign(mutation.at(one.view()).indexed_child_at(0), 11, t1);
        assign(mutation.at(two.view()).indexed_child_at(1), 22, t1);
        (void)mutation.at(three.view()); // A live key with an invalid child.
    }
    {
        auto mutation = source_dict.begin_mutation(t2);
        REQUIRE(mutation.erase(one.view()));
    }
    const auto image = capture_ts_checkpoint(source.data_view());
    CheckpointObserver root_observer, key_observer;
    target.subscribe(&root_observer);
    auto target_view = target.data_view();
    auto target_dict = target_view.as_dict();
    target_dict.key_set().subscribe(&key_observer);
    restore_ts_checkpoint(target.data_view(), image);
    CHECK(root_observer.notifications == 0);
    CHECK(key_observer.notifications == 0);
    CHECK(target_dict.size() == 2);
    CHECK(target_dict.find_slot(two.view()) == source_dict.find_slot(two.view()));
    CHECK(target_dict.find_slot(three.view()) == source_dict.find_slot(three.view()));
    CHECK(target_dict.key_set().last_modified_time() == source_dict.key_set().last_modified_time());
    CHECK_FALSE(target_dict.at(three.view()).has_current_value());
    CHECK_FALSE(target_dict.at(two.view()).indexed_child_at(0).has_current_value());
    CHECK(target_dict.at(two.view()).indexed_child_at(1).last_modified_time() == t1);
    CHECK(target_dict.at(two.view()).indexed_child_at(1).value().checked_as<std::int32_t>() == 22);
    CHECK_FALSE(target_dict.structural_delta_current(t2));

    const auto t3 = t2 + TimeDelta{1};
    {
        auto mutation = source_dict.begin_mutation(t3);
        assign(mutation.at(four.view()).indexed_child_at(0), 44, t3);
    }
    {
        auto mutation = target_dict.begin_mutation(t3);
        assign(mutation.at(four.view()).indexed_child_at(0), 44, t3);
        REQUIRE(mutation.erase(two.view()));
    }
    CHECK(target_dict.find_slot(four.view()) == source_dict.find_slot(four.view()));
    CHECK(target_dict.slot_removed(image.slots[0]));
    CHECK(root_observer.notifications == 1);
    CHECK(key_observer.notifications == 1);
    target_dict.key_set().unsubscribe(&key_observer);
    target.unsubscribe(&root_observer);
}

TEST_CASE("TS checkpoint: keyed preflight rejects a malformed later child before adding membership", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    const auto *schema = registry.tsd(ts->value_schema, ts);
    TSOutput source{schema};
    TSOutput target{schema};
    const Value one{std::int32_t{1}}, two{std::int32_t{2}};
    auto source_view = source.data_view();
    auto source_dict = source_view.as_dict();
    {
        auto mutation = source_dict.begin_mutation(MIN_ST);
        assign(mutation.at(one.view()), 11, MIN_ST);
        assign(mutation.at(two.view()), 22, MIN_ST);
    }
    auto image = capture_ts_checkpoint(source.data_view());
    image.children.back().payload = Value{std::string{"incompatible"}};
    CheckpointObserver observer;
    target.subscribe(&observer);
    CHECK_THROWS_AS(restore_ts_checkpoint(target.data_view(), image), std::invalid_argument);
    auto target_view = target.data_view();
    CHECK(target_view.as_dict().size() == 0);
    CHECK_FALSE(target.data_view().has_current_value());
    CHECK(observer.notifications == 0);
    target.unsubscribe(&observer);
}

TEST_CASE("TS checkpoint: slot capacity and publication inconsistencies fail before importing members", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    const auto *schema = registry.tsd(ts->value_schema, ts);
    TSOutput source{schema};
    TSOutput target{schema};
    auto source_view = source.data_view();
    auto source_dict = source_view.as_dict();
    const Value one{std::int32_t{1}}, two{std::int32_t{2}};
    {
        auto mutation = source_dict.begin_mutation(MIN_ST);
        assign(mutation.at(one.view()), 11, MIN_ST);
        assign(mutation.at(two.view()), 22, MIN_ST);
    }
    auto image = capture_ts_checkpoint(source.data_view());
    auto target_view = target.data_view();
    auto target_dict = target_view.as_dict();
    CheckpointObserver observer;
    target.subscribe(&observer);

    SECTION("an empty preallocated target cannot retain excess slots")
    {
        {
            auto mutation = target_dict.begin_mutation(MIN_ST);
            mutation.reserve(image.slot_capacity + 1);
        }
        REQUIRE_FALSE(target_view.has_current_value());
        REQUIRE(target_dict.slot_capacity() > image.slot_capacity);
        CHECK_THROWS_AS(restore_ts_checkpoint(target_view, image), std::invalid_argument);
        CHECK(target_dict.slot_capacity() == image.slot_capacity + 1);
    }
    SECTION("a valid later child cannot omit its publication marker")
    {
        REQUIRE(image.published.back());
        image.published.back() = false;
        CHECK_THROWS_AS(restore_ts_checkpoint(target_view, image), std::invalid_argument);
        CHECK(target_dict.slot_capacity() == 0);
    }
    CHECK(target_dict.size() == 0);
    CHECK_FALSE(target_view.has_current_value());
    CHECK(observer.notifications == 0);
    target.unsubscribe(&observer);
}

TEST_CASE("TS checkpoint: empty valid sets differ from invalid sets", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tss(checkpoint_int_schema()->value_schema);
    TSOutput invalid{schema};
    TSOutput empty{schema};
    TSOutput restored_invalid{schema};
    TSOutput restored_empty{schema};
    auto empty_view = empty.data_view();
    empty_view.as_set().begin_mutation(MIN_ST).touch();
    restore_ts_checkpoint(restored_invalid.data_view(), capture_ts_checkpoint(invalid.data_view()));
    restore_ts_checkpoint(restored_empty.data_view(), capture_ts_checkpoint(empty.data_view()));
    CHECK_FALSE(restored_invalid.data_view().has_current_value());
    CHECK(restored_empty.data_view().has_current_value());
    CHECK(restored_empty.data_view().last_modified_time() == MIN_ST);
}

TEST_CASE("TS checkpoint: set import preserves holes without publishing membership ticks", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tss(checkpoint_int_schema()->value_schema);
    TSOutput source{schema};
    TSOutput target{schema};
    auto source_view = source.data_view();
    auto source_set = source_view.as_set();
    const Value one{std::int32_t{1}}, two{std::int32_t{2}};
    {
        auto mutation = source_set.begin_mutation(MIN_ST);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.add(two.view()));
    }
    {
        auto mutation = source_set.begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.remove(one.view()));
    }
    CheckpointObserver observer;
    target.subscribe(&observer);
    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    auto target_view = target.data_view();
    auto target_set = target_view.as_set();
    CHECK(target_set.size() == 1);
    CHECK(target_set.find_slot(two.view()) == source_set.find_slot(two.view()));
    CHECK(target_set.last_modified_time() == source_set.last_modified_time());
    CHECK(observer.notifications == 0);
    CHECK(target_set.next_added_slot() == TS_DATA_NO_CHILD_ID);
    CHECK(target_set.next_removed_slot() == TS_DATA_NO_CHILD_ID);
    target.unsubscribe(&observer);
}

TEST_CASE("TS checkpoint: invalid containers retain their structural membership", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    const auto *schema = registry.tsd(ts->value_schema, ts);
    TSOutput source{schema};
    TSOutput target{schema};
    auto source_view = source.data_view();
    const Value key{std::int32_t{1}}, payload{std::int32_t{10}};
    {
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        mutation.set(key.view(), payload.view());
    }
    REQUIRE(source_view.begin_mutation(MIN_ST + TimeDelta{1}).invalidate());
    CHECK_FALSE(source_view.has_current_value());
    CHECK(source_view.as_dict().size() == 1);
    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    auto target_view = target.data_view();
    CHECK_FALSE(target_view.has_current_value());
    CHECK(target_view.as_dict().size() == 1);
    CHECK_FALSE(target_view.as_dict().at(key.view()).has_current_value());
}

TEST_CASE("TS checkpoint: unsupported state fails closed through nested structures", "[checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts = checkpoint_int_schema();
    for (const auto *schema : {registry.ref(ts)})
    {
        TSOutput output{schema};
        CHECK_FALSE(ts_checkpoint_eligible(output.data_view()));
        CHECK_THROWS_AS(capture_ts_checkpoint(output.data_view()), std::invalid_argument);
    }
    const auto *nested = registry.tsb("CheckpointUnsupportedNested", {{"reference", registry.ref(ts)}});
    TSOutput output{nested};
    CHECK_FALSE(ts_checkpoint_eligible(output.data_view()));
    CHECK_THROWS_AS(capture_ts_checkpoint(output.data_view()), std::invalid_argument);
}

TEST_CASE("TS checkpoint: dynamic lists normalize a removed tail and resume quietly", "[checkpoint]")
{
    const auto *schema = TypeRegistry::instance().tsl(checkpoint_int_schema());
    TSOutput source{schema};
    TSOutput target{schema};
    auto source_view = source.data_view();
    auto source_list = source_view.as_list();
    source_list.resize(3, MIN_ST);
    assign(source_view.indexed_child_at(0), 1, MIN_ST);
    assign(source_view.indexed_child_at(2), 3, MIN_ST);
    const auto cut = MIN_ST + TimeDelta{1};
    source_list.resize(2, cut);
    source_view.begin_mutation(cut).mark_modified();
    CheckpointObserver observer;
    target.subscribe(&observer);
    restore_ts_checkpoint(target.data_view(), capture_ts_checkpoint(source.data_view()));
    auto target_view = target.data_view();
    auto target_list = target_view.as_list();
    CHECK(target_list.size() == 2);
    CHECK(target_view.last_modified_time() == cut);
    CHECK(target_view.indexed_child_at(0).value().checked_as<std::int32_t>() == 1);
    CHECK_FALSE(target_view.indexed_child_at(1).has_current_value());
    CHECK(observer.notifications == 0);
    target_list.resize(3, cut + TimeDelta{1});
    CHECK_FALSE(target_view.indexed_child_at(2).has_current_value());
    assign(target_view.indexed_child_at(2), 4, cut + TimeDelta{1});
    CHECK(target_view.indexed_child_at(2).value().checked_as<std::int32_t>() == 4);
    CHECK(observer.notifications == 1);
    target.unsubscribe(&observer);
}
