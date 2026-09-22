#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/utils/slot_observer.h>

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <functional>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace
{
    using namespace hgraph;

    struct Observer : Notifiable, SlotObserver
    {
        std::size_t calls{0};
        std::function<void()> callback{};
        std::size_t invalidations{0};
        const TSDataTracking *invalidated_source{nullptr};
        std::function<void()> invalidation_callback{};
        void source_invalidated(const TSDataTracking *source) noexcept override
        {
            ++invalidations;
            invalidated_source = source;
            if (invalidation_callback) { invalidation_callback(); }
        }
        void notify(DateTime) override
        {
            ++calls;
            if (callback) { callback(); }
        }
        void on_insert(std::size_t) override { notify(MIN_ST); }
        void on_capacity(std::size_t, std::size_t) override {}
        void on_remove(std::size_t) override {}
        void on_erase(std::size_t) override {}
        void on_clear() override {}
    };

    template <typename Registry>
    struct ObserverHarness
    {
        Registry registry{};
        void add(Observer *observer)
        {
            if constexpr (std::is_same_v<Registry, TSDataObserverSet>) { registry.subscribe(observer); }
            else { registry.add(observer); }
        }
        void remove(Observer *observer)
        {
            if constexpr (std::is_same_v<Registry, TSDataObserverSet>) { registry.unsubscribe(observer); }
            else { registry.remove(observer); }
        }
        void notify()
        {
            if constexpr (std::is_same_v<Registry, TSDataObserverSet>) { registry.notify(MIN_ST); }
            else { registry.notify_insert(0); }
        }
    };
}

TEMPLATE_TEST_CASE("observer lookup survives growth and arbitrary removal order", "[observers]",
                   TSDataObserverSet, SlotObserverList)
{
    const std::size_t count = GENERATE(2, 8, 9, 64, 257);
    const bool reverse = GENERATE(false, true);
    ObserverHarness<TestType> harness;
    std::vector<Observer> observers(count);
    std::vector<bool> live(count, true);
    for (auto &observer : observers) { harness.add(&observer); }
    REQUIRE(harness.registry.size() == count);
    for (std::size_t i = 0; i < count; ++i)
    {
        // 37 is coprime to every chosen size, so each order removes all entries.
        const auto position = reverse ? count - 1 - i : i * 37 % count;
        harness.remove(&observers[position]);
        live[position] = false;
        REQUIRE(harness.registry.size() == count - 1 - i);
        for (std::size_t j = 0; j < count; ++j)
        {
            REQUIRE(harness.registry.contains(&observers[j]) == live[j]);
        }
        if (i + 2 >= count) { REQUIRE(harness.registry.dynamic_storage_metrics().reserved_bytes == 0); }
    }
    REQUIRE(harness.registry.empty());
}

TEMPLATE_TEST_CASE("indexed observers defer re-additions and compact after nested callbacks", "[observers]",
                   TSDataObserverSet, SlotObserverList)
{
    ObserverHarness<TestType> harness;
    std::vector<Observer> observers(65);
    for (std::size_t i = 0; i < 64; ++i) { harness.add(&observers[i]); }
    const bool throw_after_nested = GENERATE(false, true);
    observers[0].callback = [&] {
        for (std::size_t i = 0; i < 64; i += 2) { harness.remove(&observers[i]); }
        harness.add(&observers[2]);
        harness.add(&observers[64]);
        REQUIRE(harness.registry.size() == 34);
        harness.notify();
        if (throw_after_nested) { throw std::runtime_error("observer failed"); }
    };
    if (throw_after_nested) { REQUIRE_THROWS_AS(harness.notify(), std::runtime_error); }
    else { harness.notify(); }
    REQUIRE(observers[0].calls == 1);
    REQUIRE(observers[2].calls == 1);
    REQUIRE(observers[64].calls == 1);
    for (std::size_t i = 1; i < 64; i += 2)
    {
        REQUIRE(observers[i].calls == (throw_after_nested ? 1u : 2u));
        harness.remove(&observers[i]);
    }
    for (std::size_t i = 4; i < 64; i += 2) { REQUIRE(observers[i].calls == 0); }
    REQUIRE(harness.registry.size() == 2);
    harness.remove(&observers[2]);
    REQUIRE(harness.registry.contains(&observers[64]));
    REQUIRE(harness.registry.dynamic_storage_metrics().reserved_bytes == 0);
    harness.remove(&observers[64]);
    REQUIRE(harness.registry.empty());
}

TEMPLATE_TEST_CASE("indexed observer clear detaches traversal before repopulation", "[observers]",
                   TSDataObserverSet, SlotObserverList)
{
    ObserverHarness<TestType> harness;
    std::vector<Observer> observers(128);
    for (std::size_t i = 0; i < 64; ++i) { harness.add(&observers[i]); }
    observers[0].callback = [&] {
        harness.registry.clear();
        REQUIRE(harness.registry.empty());
        for (std::size_t i = 64; i < 128; ++i) { harness.add(&observers[i]); }
    };
    harness.notify();
    REQUIRE(observers[0].calls == 1);
    for (std::size_t i = 1; i < 128; ++i) { REQUIRE(observers[i].calls == 0); }
    harness.notify();
    for (std::size_t i = 64; i < 128; ++i)
    {
        REQUIRE(observers[i].calls == 1);
        harness.remove(&observers[i]);
    }
    REQUIRE(harness.registry.empty());
    REQUIRE(harness.registry.dynamic_storage_metrics().reserved_bytes == 0);
}

TEMPLATE_TEST_CASE("observer index follows facade moves and copy contracts", "[observers]",
                   TSDataObserverSet, SlotObserverList)
{
    ObserverHarness<TestType> source;
    std::vector<Observer> observers(64);
    for (auto &observer : observers) { source.add(&observer); }
    const auto metrics = source.registry.dynamic_storage_metrics();
    REQUIRE(metrics.live_bytes > observers.size() * sizeof(void *));
    REQUIRE(metrics.reserved_bytes >= metrics.live_bytes);
    ObserverHarness<TestType> copy{source.registry};
    if constexpr (std::is_same_v<TestType, SlotObserverList>)
    {
        REQUIRE(copy.registry.size() == observers.size());
        copy.remove(&observers[31]);
        REQUIRE(source.registry.contains(&observers[31]));
    }
    else { REQUIRE(copy.registry.empty()); }

    ObserverHarness<TestType> moved{std::move(source.registry)};
    REQUIRE(source.registry.empty());
    REQUIRE(moved.registry.dynamic_storage_metrics().reserved_bytes == metrics.reserved_bytes);
    copy.registry = std::move(moved.registry);
    REQUIRE(moved.registry.empty());
    for (auto &observer : observers) { copy.remove(&observer); }
    REQUIRE(copy.registry.empty());
    REQUIRE(copy.registry.dynamic_storage_metrics().reserved_bytes == 0);
}

TEST_CASE("indexed TSData observer replacement preserves callback positions", "[observers]")
{
    TSDataObserverSet registry;
    std::vector<Observer> observers(129);
    for (std::size_t i = 0; i < 64; ++i) { registry.subscribe(&observers[i]); }
    for (std::size_t i = 0; i < 64; ++i)
    {
        registry.replace(&observers[i], &observers[i + 64]);
        REQUIRE_FALSE(registry.contains(&observers[i]));
        REQUIRE(registry.contains(&observers[i + 64]));
    }
    observers[64].callback = [&] {
        registry.replace(&observers[95], &observers[128]);
        registry.unsubscribe(&observers[64]);
        registry.notify(MIN_ST);
    };
    registry.notify(MIN_ST);
    REQUIRE(observers[64].calls == 1);
    REQUIRE(observers[95].calls == 0);
    REQUIRE(observers[128].calls == 2);
    REQUIRE_FALSE(registry.contains(&observers[95]));
    for (std::size_t i = 65; i < 128; ++i)
    {
        if (i != 95)
        {
            REQUIRE(observers[i].calls == 2);
            registry.unsubscribe(&observers[i]);
        }
    }
    registry.unsubscribe(&observers[128]);
    REQUIRE(registry.empty());
    REQUIRE(registry.dynamic_storage_metrics().reserved_bytes == 0);
}

TEST_CASE("indexed TSData invalidation detaches registrations during notification", "[observers]")
{
    TSDataObserverSet registry;
    TSDataTracking source;
    std::vector<Observer> observers(128);
    for (std::size_t i = 0; i < 64; ++i) { registry.subscribe(&observers[i]); }
    observers[0].invalidation_callback = [&] {
        CHECK(registry.empty());
        for (std::size_t i = 64; i < 128; ++i) { registry.subscribe(&observers[i]); }
    };
    const bool during_notification = GENERATE(false, true);
    if (during_notification)
    {
        observers[0].callback = [&] { registry.invalidate(&source); };
        registry.notify(MIN_ST);
    }
    else { registry.invalidate(&source); }
    for (std::size_t i = 0; i < 64; ++i)
    {
        REQUIRE(observers[i].invalidations == 1);
        REQUIRE(observers[i].invalidated_source == &source);
        REQUIRE_FALSE(registry.contains(&observers[i]));
        if (i != 0) { REQUIRE(observers[i].calls == 0); }
    }
    REQUIRE(registry.size() == 64);
    for (std::size_t i = 64; i < 128; ++i) { REQUIRE(observers[i].calls == 0); }
    registry.notify(MIN_ST);
    for (std::size_t i = 128; i > 64; --i)
    {
        REQUIRE(observers[i - 1].invalidations == 0);
        REQUIRE(observers[i - 1].calls == 1);
        registry.unsubscribe(&observers[i - 1]);
    }
    REQUIRE(registry.empty());
}
