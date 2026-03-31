#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

TEST_CASE("Cyclic buffer values overwrite the oldest element when full")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 3).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto buffer = value.view().as_cyclic_buffer();

    {
        auto mutation = buffer.begin_mutation();
        mutation.push(hgraph::value_for(int32_t{1}).view());
        mutation.push(hgraph::value_for(int32_t{2}).view());
        mutation.push(hgraph::value_for(int32_t{3}).view());
        mutation.push(hgraph::value_for(int32_t{4}).view());
    }

    CHECK(buffer.size() == 3);
    CHECK(buffer.front().as_atomic().as<int32_t>() == 2);
    CHECK(buffer.back().as_atomic().as<int32_t>() == 4);
    CHECK(buffer.has_removed());
    CHECK(buffer.removed().as_atomic().as<int32_t>() == 1);

    buffer.begin_mutation().clear();
    CHECK(buffer.empty());
    CHECK(buffer.has_removed());
    CHECK(buffer.removed().as_atomic().as<int32_t>() == 4);
}

TEST_CASE("Bounded queue values evict from the front when full")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto queue = value.view().as_queue();

    {
        auto mutation = queue.begin_mutation();
        mutation.push(hgraph::value_for(int32_t{10}).view());
        mutation.push(hgraph::value_for(int32_t{20}).view());
        mutation.push(hgraph::value_for(int32_t{30}).view());
    }

    CHECK(queue.size() == 2);
    CHECK(queue.front().as_atomic().as<int32_t>() == 20);
    CHECK(queue.back().as_atomic().as<int32_t>() == 30);
    CHECK(queue.has_removed());
    CHECK(queue.removed().as_atomic().as<int32_t>() == 10);

    queue.begin_mutation().pop();
    CHECK(queue.size() == 1);
    CHECK(queue.front().as_atomic().as<int32_t>() == 30);
    CHECK(queue.has_removed());
    CHECK(queue.removed().as_atomic().as<int32_t>() == 20);
}

TEST_CASE("Queue mutation views support exact reserve", "[time_series][value][sequence]")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto queue = value.queue_view();

    queue.begin_mutation().reserving(12).pushing(int32_t{10}).pushing(int32_t{20}).pushing(int32_t{30});

    REQUIRE(queue.size() == 3);
    CHECK(queue.front().as_atomic().as<int32_t>() == 10);
    CHECK(queue.back().as_atomic().as<int32_t>() == 30);
}

TEST_CASE("Bounded queues reject impossible reserve requests", "[time_series][value][sequence]")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto queue = value.queue_view();

    CHECK_NOTHROW(queue.begin_mutation().reserve(2));
    CHECK_THROWS(queue.begin_mutation().reserve(3));
}

TEST_CASE("Buffer mutation views retain only the last removed payload in a scope")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *cyclic_schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 2).build();
    const auto *queue_schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value cyclic_value{*cyclic_schema, hgraph::MutationTracking::Delta};
    auto cyclic = cyclic_value.cyclic_buffer_view();
    cyclic.begin_mutation()
        .pushing(hgraph::value_for(int32_t{1}).view())
        .pushing(hgraph::value_for(int32_t{2}).view())
        .pushing(hgraph::value_for(int32_t{3}).view())
        .popping();
    CHECK(cyclic.has_removed());
    CHECK(cyclic.removed().as_atomic().as<int32_t>() == 2);

    hgraph::Value queue_value{*queue_schema, hgraph::MutationTracking::Delta};
    auto queue = queue_value.queue_view();
    queue.begin_mutation()
        .pushing(hgraph::value_for(int32_t{10}).view())
        .pushing(hgraph::value_for(int32_t{20}).view())
        .pushing(hgraph::value_for(int32_t{30}).view())
        .popping();
    CHECK(queue.has_removed());
    CHECK(queue.removed().as_atomic().as<int32_t>() == 20);
}

TEST_CASE("Buffer mutation views accept native C++ values directly")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *cyclic_schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 2).build();
    const auto *queue_schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value cyclic_value{*cyclic_schema};
    auto cyclic = cyclic_value.cyclic_buffer_view();
    cyclic.begin_mutation().pushing(int32_t{1}).pushing(int32_t{2}).setting(0, int32_t{3});
    CHECK(cyclic[0].as_atomic().as<int32_t>() == 3);
    CHECK(cyclic[1].as_atomic().as<int32_t>() == 2);

    hgraph::Value queue_value{*queue_schema};
    auto queue = queue_value.queue_view();
    queue.begin_mutation().pushing(int32_t{10}).pushing(int32_t{20}).pushing(int32_t{30});
    CHECK(queue.front().as_atomic().as<int32_t>() == 20);
    CHECK(queue.back().as_atomic().as<int32_t>() == 30);
}

TEST_CASE("Plain buffer values do not retain removed payloads")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *cyclic_schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 2).build();
    const auto *queue_schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value cyclic_value{*cyclic_schema, hgraph::MutationTracking::Plain};
    auto cyclic = cyclic_value.cyclic_buffer_view();
    cyclic.begin_mutation().pushing(int32_t{1}).pushing(int32_t{2}).pushing(int32_t{3}).popping();
    CHECK_FALSE(cyclic.has_removed());

    hgraph::Value queue_value{*queue_schema, hgraph::MutationTracking::Plain};
    auto queue = queue_value.queue_view();
    queue.begin_mutation().pushing(int32_t{10}).pushing(int32_t{20}).pushing(int32_t{30}).popping();
    CHECK_FALSE(queue.has_removed());
}

TEST_CASE("Buffer builders keep plain storage smaller than delta storage")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *cyclic_schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 2).build();
    const auto *queue_schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    CHECK(hgraph::ValueBuilderFactory::checked_builder_for(cyclic_schema, hgraph::MutationTracking::Plain).size() <
          hgraph::ValueBuilderFactory::checked_builder_for(cyclic_schema, hgraph::MutationTracking::Delta).size());
    CHECK(hgraph::ValueBuilderFactory::checked_builder_for(queue_schema, hgraph::MutationTracking::Plain).size() <
          hgraph::ValueBuilderFactory::checked_builder_for(queue_schema, hgraph::MutationTracking::Delta).size());
}
