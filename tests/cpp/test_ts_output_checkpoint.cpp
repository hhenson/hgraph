#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <iostream>
#include <unordered_map>

namespace
{
    using namespace hgraph;

    struct CheckpointCursor
    {
        const void *data;
        const TypeRecord *type;
        bool operator==(const CheckpointCursor &) const = default;
    };
    struct CheckpointCursorHash
    {
        std::size_t operator()(const CheckpointCursor &cursor) const noexcept
        {
            return std::hash<const void *>{}(cursor.data) ^ (std::hash<const void *>{}(cursor.type) << 1);
        }
    };
    CheckpointCursor cursor(const TSOutputHandle &handle)
    {
        return {handle.data_view().data(), handle.storage_type().record()};
    }

    struct AdapterObserver : Notifiable
    {
        std::size_t notifications{0};
        void notify(DateTime) override { ++notifications; }
    };

    void write(const TSOutputView &output, Value value, DateTime time)
    {
        auto mutation = output.begin_mutation(time);
        static_cast<void>(mutation.copy_value_from(value.view()));
    }

    // The coordinator imports raw references after every owning endpoint is
    // allocated, without publishing them as new source events.
    void import_reference(TSOutput &output, const TimeSeriesReference &reference, DateTime time)
    {
        auto data = output.data_view();
        const auto &ops = data.ops();
        const Value value{reference};
        static_cast<void>(ops.copy_value_from_impl(ops.context, data.mutable_data(), value.view(), time));
        ops.mutable_tracking_impl(ops.context, data.mutable_data())->last_modified_time = time;
    }
}

TEST_CASE("REF adapter checkpoint rejects malformed restoration cursors", "[checkpoint][reference]")
{
    const auto *scalar = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
    TSOutput output{scalar};
    const TSCheckpointImage clocks{};
    CHECK_THROWS_WITH(output.restore_checkpoint_alternative(output.view(), *scalar, clocks, MIN_ST),
                      Catch::Matchers::ContainsSubstring("schema adaptation"));
    CHECK_THROWS_WITH(output.restore_checkpoint_alternative(TSOutputView{}, *scalar, clocks, MIN_ST),
                      Catch::Matchers::ContainsSubstring("typed source"));
    TSOutput other{scalar};
    CHECK_THROWS_WITH(output.restore_checkpoint_alternative(other.view(), *scalar, clocks, MIN_ST),
                      Catch::Matchers::ContainsSubstring("typed source"));
}

TEST_CASE("REF adapter checkpoint retains alias identity and historical clocks without notifications", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *reference = registry.ref(scalar);
    const auto first = MIN_ST, retarget = MIN_ST + MIN_TD, resume = MIN_ST + 2 * MIN_TD;
    TSOutput value{scalar};
    TSOutput selected{reference};
    write(value.view(first), Value{Int{7}}, first);
    write(selected.view(retarget), Value{TimeSeriesReference::peered(value.view(retarget))}, retarget);
    const auto adapted = selected.binding_for(selected.view(retarget), *scalar);
    const auto descriptor = selected.checkpoint_alternative(adapted);
    REQUIRE(descriptor);
    CHECK(descriptor->source.same_as(selected.view().handle()));
    CHECK(descriptor->requested_schema == scalar);
    CHECK(descriptor->path.empty());
    const auto images = selected.capture_checkpoint_alternatives();
    REQUIRE(images.size() == 1);

    TSOutput new_value{scalar};
    restore_ts_checkpoint(new_value.data_view(), capture_ts_checkpoint(value.data_view()));
    TSOutput new_selected{reference};
    const auto new_adapter = new_selected.checkpoint_binding_for(new_selected.view(resume), *scalar);
    AdapterObserver observer;
    new_adapter.data_view().subscribe(&observer);
    import_reference(new_selected, TimeSeriesReference::peered(new_value.view(resume)), retarget);
    new_selected.restore_checkpoint_alternative(new_selected.view(resume), *scalar, images[0].clocks, resume);
    CHECK(observer.notifications == 0);
    CHECK(new_adapter.view(resume).last_modified_time() == adapted.view(resume).last_modified_time());
    CHECK_FALSE(new_adapter.view(resume).modified());
    CHECK(new_adapter.view(resume).value().checked_as<Int>() == 7);
    write(new_value.view(resume), Value{Int{9}}, resume);
    CHECK(observer.notifications == 1);
    CHECK(new_adapter.view(resume).value().checked_as<Int>() == 9);
    new_adapter.data_view().unsubscribe(&observer);
}

TEST_CASE("REF adapter checkpoint describes chained adapters and fixed interior cursors", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *bundle = registry.tsb("adapter_checkpoint_pair", {{"a", scalar}, {"b", scalar}});
    const auto *reference = registry.ref(bundle);
    TSOutput source{bundle};
    const auto first = source.binding_for(source.view(MIN_ST), *reference);
    const auto second = source.binding_for(first.view(MIN_ST), *bundle);
    const auto child = second.view(MIN_ST).indexed_child_at(1).handle();
    const auto descriptor = source.checkpoint_alternative(child);
    REQUIRE(descriptor);
    CHECK(descriptor->source.same_as(first));
    CHECK(descriptor->requested_schema == bundle);
    CHECK(descriptor->path == std::vector<std::size_t>{1});
    const auto parent = source.checkpoint_alternative(descriptor->source);
    REQUIRE(parent);
    CHECK(parent->source.same_as(source.view().handle()));
    CHECK(parent->requested_schema == reference);
    CHECK_FALSE(source.checkpoint_alternative(source.view(MIN_ST).indexed_child_at(1).handle()));
    std::unordered_map<CheckpointCursor, TSOutputAlternativeDescriptor, CheckpointCursorHash> indexed;
    source.visit_checkpoint_alternative_endpoints([&](const auto &handle, const auto &entry) {
        CHECK(indexed.emplace(cursor(handle), entry).second);
    });
    REQUIRE(indexed.contains(cursor(child)));
    CHECK(indexed.at(cursor(child)).source.same_as(descriptor->source));
    CHECK(indexed.at(cursor(child)).path == descriptor->path);
    CHECK(indexed.at(cursor(first)).source.same_as(source.view().handle()));

    const auto images = source.capture_checkpoint_alternatives();
    REQUIRE(images.size() == 2);
    TSOutput restored{bundle};
    const auto restored_first = restored.checkpoint_binding_for(restored.view(MIN_ST), *reference);
    const auto restored_second = restored.checkpoint_binding_for(restored_first.view(MIN_ST), *bundle);
    AdapterObserver observer;
    restored_second.data_view().subscribe(&observer);
    restored.restore_checkpoint_alternative(restored.view(MIN_ST), *reference, images[0].clocks, MIN_ST + MIN_TD);
    restored.restore_checkpoint_alternative(restored_first.view(MIN_ST), *bundle, images[1].clocks, MIN_ST + MIN_TD);
    CHECK(observer.notifications == 0);
    restored_second.data_view().unsubscribe(&observer);
}

TEST_CASE("REF adapter checkpoint index visits each scalar adapter once", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *reference = registry.ref(scalar);
    constexpr std::size_t count = 512;
    TSOutput source{registry.tsl(scalar, count)};
    std::unordered_map<CheckpointCursor, TSOutputHandle, CheckpointCursorHash> expected;
    for (std::size_t index = 0; index < count; ++index)
    {
        const auto child = source.view(MIN_ST).indexed_child_at(index);
        const auto adapter = source.binding_for(child, *reference);
        expected.emplace(cursor(adapter), child.handle());
    }
    std::size_t visits = 0;
    source.visit_checkpoint_alternative_endpoints([&](const auto &handle, const auto &entry) {
        const auto found = expected.find(cursor(handle));
        REQUIRE(found != expected.end());
        CHECK(entry.source.same_as(found->second));
        CHECK(entry.requested_schema == reference);
        CHECK(entry.path.empty());
        expected.erase(found);
        ++visits;
    });
    CHECK(visits == count);
    CHECK(expected.empty());
}

TEST_CASE("REF adapter checkpoint index scaling", "[.][checkpoint-scaling]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *reference = registry.ref(scalar);
    for (const std::size_t count : {1000, 2000, 4000, 8000})
    {
        TSOutput source{registry.tsl(scalar, count)};
        std::vector<TSOutputHandle> handles;
        for (std::size_t index = 0; index < count; ++index)
            handles.push_back(source.binding_for(source.view(MIN_ST).indexed_child_at(index), *reference));
        const auto start = std::chrono::steady_clock::now();
        std::unordered_map<CheckpointCursor, TSOutputAlternativeDescriptor, CheckpointCursorHash> indexed;
        source.visit_checkpoint_alternative_endpoints([&](const auto &handle, const auto &entry) {
            indexed.emplace(cursor(handle), entry);
        });
        std::size_t found = 0;
        for (const auto &handle : handles) { found += indexed.contains(cursor(handle)); }
        const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
        REQUIRE(found == count);
        std::cout << "checkpoint_adapter_index count=" << count << " ms=" << elapsed << '\n';
    }
}

TEST_CASE("REF adapter checkpoint refuses keyed interior proxy state", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *scalar = registry.ts(integer);
    const auto *source_schema = registry.tsd(integer, registry.ref(scalar));
    const auto *target_schema = registry.tsd(integer, scalar);
    TSOutput source{source_schema};
    const auto adapter = source.binding_for(source.view(MIN_ST), *target_schema);
    REQUIRE(source.checkpoint_alternative(adapter));
    CHECK_THROWS_WITH(source.capture_checkpoint_alternatives(),
        Catch::Matchers::ContainsSubstring("keyed interior REF adapter is unsupported"));
    CHECK(source.capture_checkpoint_alternatives([](const TSOutputHandle &) { return false; }).empty());
}

TEST_CASE("REF adapter checkpoint excludes cached adapters for removed source keys", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *scalar = registry.ts(integer);
    const auto *reference = registry.ref(scalar);
    TSOutput source{registry.tsd(integer, scalar)};
    const Value removed_key{Int{1}}, retained_key{Int{2}};
    {
        auto view = source.view(MIN_ST);
        auto mutation = view.as_dict().begin_mutation(MIN_ST);
        static_cast<void>(mutation.at(removed_key.view()));
        static_cast<void>(mutation.at(retained_key.view()));
    }
    auto view = source.view(MIN_ST);
    const auto removed = view.as_dict().at(removed_key.view()).handle();
    const auto retained = view.as_dict().at(retained_key.view()).handle();
    static_cast<void>(source.binding_for(removed.view(MIN_ST), *reference));
    static_cast<void>(source.binding_for(retained.view(MIN_ST), *reference));
    {
        auto mutation = view.as_dict().begin_mutation(MIN_ST + MIN_TD);
        REQUIRE(mutation.erase(removed_key.view()));
    }
    std::size_t inspected = 0;
    const auto images = source.capture_checkpoint_alternatives([&](const TSOutputHandle &handle) {
        ++inspected;
        return handle.same_as(retained);
    });
    CHECK(inspected == 2);
    REQUIRE(images.size() == 1);
    CHECK(images[0].binding.source.same_as(retained));
}

TEST_CASE("REF adapter checkpoint quietly restores a whole keyed target and its structural clock", "[checkpoint][reference]")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *dictionary = registry.tsd(integer, registry.ts(integer));
    const auto *reference = registry.ref(dictionary);
    const auto first = MIN_ST, retarget = MIN_ST + MIN_TD, resume = MIN_ST + 2 * MIN_TD;
    TSOutput values{dictionary};
    {
        auto output = values.view(first);
        auto mutation = output.as_dict().begin_mutation(first);
        auto child = mutation.at(Value{Int{1}}.view());
        static_cast<void>(child.begin_mutation(first).copy_value_from(Value{Int{7}}.view()));
    }
    TSOutput selected{reference};
    write(selected.view(retarget), Value{TimeSeriesReference::peered(values.view(retarget))}, retarget);
    const auto adapter = selected.binding_for(selected.view(retarget), *dictionary);
    const auto images = selected.capture_checkpoint_alternatives();
    REQUIRE(images.size() == 1);
    CHECK(images[0].clocks.key_set_last_modified_time == retarget);

    TSOutput restored_values{dictionary};
    restore_ts_checkpoint(restored_values.data_view(), capture_ts_checkpoint(values.data_view()));
    TSOutput restored_selected{reference};
    const auto restored = restored_selected.checkpoint_binding_for(restored_selected.view(resume), *dictionary);
    AdapterObserver observer;
    restored.data_view().subscribe(&observer);
    import_reference(restored_selected, TimeSeriesReference::peered(restored_values.view(resume)), retarget);
    restored_selected.restore_checkpoint_alternative(restored_selected.view(resume), *dictionary, images[0].clocks, resume);
    CHECK(observer.notifications == 0);
    CHECK(restored.view(resume).value().equals(adapter.view(resume).value()));
    CHECK(restored_selected.capture_checkpoint_alternatives()[0].clocks.key_set_last_modified_time == retarget);
    {
        auto output = restored_values.view(resume);
        auto mutation = output.as_dict().begin_mutation(resume);
        auto child = mutation.at(Value{Int{2}}.view());
        static_cast<void>(child.begin_mutation(resume).copy_value_from(Value{Int{9}}.view()));
    }
    CHECK(observer.notifications == 1);
    auto view = restored.view(resume);
    CHECK(view.as_dict().size() == 2);
    restored.data_view().unsubscribe(&observer);
}
