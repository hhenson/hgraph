#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/utils/small_dense_ptr_map.h>

#include <cstddef>
#include <memory>
#include <vector>

namespace
{
    struct Entry
    {
        explicit Entry(int value_) : value{value_} {}
        int value{0};
    };

    struct TrackedEntry
    {
        explicit TrackedEntry(int value_) : value{value_} { ++alive; }
        ~TrackedEntry() { --alive; }

        TrackedEntry(const TrackedEntry &) = delete;
        TrackedEntry &operator=(const TrackedEntry &) = delete;

        int value{0};
        static inline int alive{0};
    };
}

TEST_CASE("SmallDensePtrMap keeps small ownership maps allocation-efficient", "[small-dense-ptr-map]")
{
    using Map = hgraph::detail::SmallDensePtrMap<std::size_t, Entry>;
    STATIC_REQUIRE(sizeof(Map) == sizeof(void *));

    Map values;

    CHECK(values.empty());
    CHECK_FALSE(values.uses_dense_storage());
    CHECK(values.dynamic_storage_metrics().reserved_bytes == 0);

    auto &first = values.ensure(2, [] { return std::make_unique<Entry>(20); });
    CHECK(first.value == 20);
    CHECK(values.find(2) == &first);
    CHECK(&values.ensure(2, [] { return std::make_unique<Entry>(99); }) == &first);
    CHECK(values.size() == 1);
    CHECK_FALSE(values.uses_dense_storage());
    const auto single_metrics = values.dynamic_storage_metrics();
    CHECK(single_metrics.live_bytes > 0);
    CHECK(single_metrics.live_bytes == single_metrics.reserved_bytes);

    CHECK(values.erase(2));
    CHECK(values.empty());
    CHECK(values.dynamic_storage_metrics().reserved_bytes == 0);
}

TEST_CASE("SmallDensePtrMap promotes without relocating owned objects", "[small-dense-ptr-map]")
{
    hgraph::detail::SmallDensePtrMap<std::size_t, Entry> values;
    std::vector<Entry *> stable;
    std::vector<std::size_t> reserved;
    for (std::size_t key = 0; key < 8; ++key)
    {
        stable.push_back(&values.ensure(key, [key] { return std::make_unique<Entry>(static_cast<int>(key)); }));
        reserved.push_back(values.dynamic_storage_metrics().reserved_bytes);
    }

    CHECK_FALSE(values.uses_dense_storage());
    CHECK(reserved[1] > reserved[0]);
    CHECK(reserved[2] > reserved[1]);
    CHECK(reserved[3] == reserved[2]);
    CHECK(reserved[4] > reserved[3]);
    CHECK(reserved[5] == reserved[4]);
    CHECK(reserved[6] == reserved[4]);
    CHECK(reserved[7] == reserved[4]);
    auto &ninth = values.ensure(8, [] { return std::make_unique<Entry>(8); });
    CHECK(values.uses_dense_storage());
    CHECK(values.size() == 9);
    CHECK(values.find(8) == &ninth);
    for (std::size_t key = 0; key < stable.size(); ++key)
    {
        CHECK(values.find(key) == stable[key]);
    }

    CHECK(values.erase(3));
    CHECK_FALSE(values.erase(3));
    CHECK(values.find(3) == nullptr);

    std::size_t visited = 0;
    values.for_each([&](std::size_t, const Entry &) { ++visited; });
    CHECK(visited == 8);
    std::size_t predicates = 0;
    CHECK(values.any_of([&](std::size_t, const Entry &) {
        ++predicates;
        return true;
    }));
    CHECK(predicates == 1);
    CHECK(values.dynamic_storage_metrics().reserved_bytes >=
          values.dynamic_storage_metrics().live_bytes);
}

TEST_CASE("SmallDensePtrMap reuses promoted storage after erase", "[small-dense-ptr-map]")
{
    hgraph::detail::SmallDensePtrMap<std::size_t, Entry> values;
    for (std::size_t key = 0; key < 4; ++key)
    {
        static_cast<void>(values.ensure(key, [key] {
            return std::make_unique<Entry>(static_cast<int>(key));
        }));
    }
    const auto small_reserved = values.dynamic_storage_metrics().reserved_bytes;

    for (std::size_t key = 0; key < 4; ++key) { CHECK(values.erase(key)); }
    CHECK(values.empty());
    CHECK_FALSE(values.uses_dense_storage());
    CHECK(values.dynamic_storage_metrics().reserved_bytes == small_reserved);

    auto &replacement = values.ensure(10, [] { return std::make_unique<Entry>(10); });
    CHECK(values.find(10) == &replacement);
    CHECK(values.dynamic_storage_metrics().reserved_bytes == small_reserved);
}

TEST_CASE("SmallDensePtrMap move operations preserve pointees and release prior ownership",
          "[small-dense-ptr-map]")
{
    CHECK(TrackedEntry::alive == 0);
    {
        using Map = hgraph::detail::SmallDensePtrMap<std::size_t, TrackedEntry>;
        Map source;
        std::vector<TrackedEntry *> stable;
        for (std::size_t key = 0; key < 9; ++key)
        {
            stable.push_back(&source.ensure(key, [key] {
                return std::make_unique<TrackedEntry>(static_cast<int>(key));
            }));
        }
        CHECK(TrackedEntry::alive == 9);

        Map moved{std::move(source)};
        CHECK(source.empty());
        CHECK(source.dynamic_storage_metrics().reserved_bytes == 0);
        CHECK(moved.uses_dense_storage());
        for (std::size_t key = 0; key < stable.size(); ++key)
        {
            CHECK(moved.find(key) == stable[key]);
        }

        Map destination;
        static_cast<void>(destination.ensure(100, [] {
            return std::make_unique<TrackedEntry>(100);
        }));
        CHECK(TrackedEntry::alive == 10);
        destination = std::move(moved);
        CHECK(TrackedEntry::alive == 9);
        CHECK(moved.empty());
        for (std::size_t key = 0; key < stable.size(); ++key)
        {
            CHECK(destination.find(key) == stable[key]);
        }
    }
    CHECK(TrackedEntry::alive == 0);
}

TEST_CASE("SmallDensePtrMap iteration preserves keys and exposes pointees", "[small-dense-ptr-map]")
{
    hgraph::detail::SmallDensePtrMap<std::size_t, Entry> values;
    for (std::size_t key = 0; key < 3; ++key)
    {
        static_cast<void>(values.ensure(key, [key] {
            return std::make_unique<Entry>(static_cast<int>(key));
        }));
    }

    values.for_each([](std::size_t key, Entry &entry) {
        entry.value += static_cast<int>(key + 10);
    });

    const auto &const_values = values;
    int total = 0;
    const_values.for_each([&](std::size_t, const Entry &entry) { total += entry.value; });
    CHECK(total == 36);

    std::size_t predicates = 0;
    CHECK(const_values.any_of([&](std::size_t key, const Entry &entry) {
        ++predicates;
        return key == 1 && entry.value == 12;
    }));
    CHECK(predicates <= values.size());
}
