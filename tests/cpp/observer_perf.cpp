#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/utils/slot_observer.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <limits>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>
#include <vector>

namespace
{
    constexpr auto unlimited = std::numeric_limits<std::size_t>::max();
    std::size_t allocations_before_failure = unlimited;

    struct Observer : hgraph::Notifiable, hgraph::SlotObserver
    {
        std::size_t calls{0};
        std::function<void()> callback{};
        void notify(hgraph::DateTime) override { ++calls; if (callback) { callback(); } }
        void on_insert(std::size_t) override { notify(hgraph::MIN_ST); }
        void on_capacity(std::size_t, std::size_t) override {}
        void on_remove(std::size_t) override {}
        void on_erase(std::size_t) override {}
        void on_clear() override {}
    };

    template <typename Registry> void add(Registry &registry, Observer *observer)
    {
        if constexpr (std::is_same_v<Registry, hgraph::TSDataObserverSet>) { registry.subscribe(observer); }
        else if constexpr (std::is_same_v<Registry, hgraph::SlotObserverList>) { registry.add(observer); }
        else { registry.insert(observer); }
    }
    template <typename Registry> void remove(Registry &registry, Observer *observer)
    {
        if constexpr (std::is_same_v<Registry, hgraph::TSDataObserverSet>) { registry.unsubscribe(observer); }
        else if constexpr (std::is_same_v<Registry, hgraph::SlotObserverList>) { registry.remove(observer); }
        else { registry.erase(observer); }
    }
    void require(bool condition)
    {
        if (!condition) { throw std::runtime_error("observer benchmark validation failed"); }
    }

    template <typename Registry> void check_failed_growth()
    {
        for (const std::size_t count : {8, 16, 32})
        {
            bool succeeded = false;
            for (std::size_t allowance = 0; allowance < 12; ++allowance)
            {
                Registry registry;
                std::vector<Observer> observers(count + 1);
                for (std::size_t i = 0; i < count; ++i) { add(registry, &observers[i]); }
                allocations_before_failure = allowance;
                try { add(registry, &observers[count]); succeeded = true; }
                catch (const std::bad_alloc &) {}
                allocations_before_failure = unlimited;
                require(registry.size() == count + static_cast<std::size_t>(succeeded));
                require(registry.contains(&observers[count]) == succeeded);
                for (std::size_t i = 0; i < count; ++i) { require(registry.contains(&observers[i])); }
                allocations_before_failure = 0;
                for (std::size_t i = 0; i < count; ++i) { remove(registry, &observers[i]); }
                if (succeeded) { remove(registry, &observers[count]); }
                allocations_before_failure = unlimited;
                require(registry.empty());
                if (succeeded) { break; }
            }
            require(succeeded);
        }
    }

    void check_no_allocation_during_replacement_and_compaction()
    {
        hgraph::TSDataObserverSet registry;
        std::vector<Observer> observers(128);
        for (std::size_t i = 0; i < 64; ++i) { registry.subscribe(&observers[i]); }
        observers[0].callback = [&] {
            for (std::size_t i = 0; i < 64; ++i) { registry.replace(&observers[i], &observers[i + 64]); }
            for (std::size_t i = 64; i < 128; i += 2) { registry.unsubscribe(&observers[i]); }
            registry.notify(hgraph::MIN_ST);
        };
        allocations_before_failure = 0;
        registry.notify(hgraph::MIN_ST);
        for (std::size_t i = 65; i < 128; i += 2) { registry.unsubscribe(&observers[i]); }
        allocations_before_failure = unlimited;
        require(registry.empty());
        for (std::size_t i = 65; i < 128; i += 2) { require(observers[i].calls == 2); }
    }

    template <typename Registry> void measure(const char *name, std::size_t count, bool reverse)
    {
        std::vector<Observer> observers(count);
        std::vector<double> registration, removal;
        for (int sample = 0; sample < 9; ++sample)
        {
            Registry registry;
            const auto start_add = std::chrono::steady_clock::now();
            for (auto &observer : observers) { add(registry, &observer); }
            const auto end_add = std::chrono::steady_clock::now();
            require(registry.size() == count);
            allocations_before_failure = 0;
            const auto start_remove = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i < count; ++i)
            {
                // Front order selects the current first vector entry after each swap/pop.
                const auto position = reverse ? count - i - 1 : (i == 0 ? 0 : count - i);
                remove(registry, &observers[position]);
            }
            const auto end_remove = std::chrono::steady_clock::now();
            allocations_before_failure = unlimited;
            require(registry.empty());
            if (sample != 0)
            {
                registration.push_back(std::chrono::duration<double, std::micro>(end_add - start_add).count());
                removal.push_back(std::chrono::duration<double, std::micro>(end_remove - start_remove).count());
            }
        }
        std::ranges::sort(registration);
        std::ranges::sort(removal);
        std::printf("%s,%s,%zu,%.3f,%.3f\n", name, reverse ? "reverse" : "front", count,
                    registration[registration.size() / 2], removal[removal.size() / 2]);
    }
}

void *operator new(std::size_t size)
{
    if (allocations_before_failure == 0) { throw std::bad_alloc{}; }
    if (allocations_before_failure != unlimited) { --allocations_before_failure; }
    if (auto *memory = std::malloc(size == 0 ? 1 : size)) { return memory; }
    throw std::bad_alloc{};
}
void *operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }

int main()
{
    check_failed_growth<hgraph::TSDataObserverSet>();
    check_failed_growth<hgraph::SlotObserverList>();
    check_no_allocation_during_replacement_and_compaction();
    std::puts("container,order,observers,registration_us,removal_us");
    for (const std::size_t count : {8, 64, 1000, 2000, 4000, 8000, 16000})
    {
        for (const bool reverse : {false, true})
        {
            measure<hgraph::TSDataObserverSet>("tsdata", count, reverse);
            measure<hgraph::SlotObserverList>("slot", count, reverse);
            // Historical representation: a useful lookup/removal reference,
            // without the current containers' callback mutation guarantees.
            measure<std::unordered_set<hgraph::Notifiable *>>("unordered_set", count, reverse);
        }
    }
}
