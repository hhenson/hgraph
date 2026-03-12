#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

TEST_CASE("Cyclic buffer values overwrite the oldest element when full")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 3).build();

    hgraph::Value value{*schema};
    auto buffer = value.view().as_cyclic_buffer();

    buffer.push(hgraph::value_for(int32_t{1}).view());
    buffer.push(hgraph::value_for(int32_t{2}).view());
    buffer.push(hgraph::value_for(int32_t{3}).view());
    buffer.push(hgraph::value_for(int32_t{4}).view());

    CHECK(buffer.size() == 3);
    CHECK(buffer.front().as_atomic().as<int32_t>() == 2);
    CHECK(buffer.back().as_atomic().as<int32_t>() == 4);

    buffer.clear();
    CHECK(buffer.empty());
}

TEST_CASE("Bounded queue values evict from the front when full")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.queue(hgraph::value::scalar_type_meta<int32_t>()).max_capacity(2).build();

    hgraph::Value value{*schema};
    auto queue = value.view().as_queue();

    queue.push(hgraph::value_for(int32_t{10}).view());
    queue.push(hgraph::value_for(int32_t{20}).view());
    queue.push(hgraph::value_for(int32_t{30}).view());

    CHECK(queue.size() == 2);
    CHECK(queue.front().as_atomic().as<int32_t>() == 20);
    CHECK(queue.back().as_atomic().as<int32_t>() == 30);

    queue.pop();
    CHECK(queue.size() == 1);
    CHECK(queue.front().as_atomic().as<int32_t>() == 30);
}
