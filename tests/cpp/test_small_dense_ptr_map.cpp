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
}

TEST_CASE("SmallDensePtrMap keeps small ownership maps allocation-efficient", "[small-dense-ptr-map]")
{
    hgraph::detail::SmallDensePtrMap<std::size_t, Entry> values;

    CHECK(values.empty());
    CHECK_FALSE(values.uses_dense_storage());
    CHECK(values.dynamic_storage_metrics().reserved_bytes == 0);

    auto &first = values.ensure(2, [] { return std::make_unique<Entry>(20); });
    CHECK(first.value == 20);
    CHECK(values.find(2) == &first);
    CHECK(&values.ensure(2, [] { return std::make_unique<Entry>(99); }) == &first);
    CHECK(values.size() == 1);
    CHECK_FALSE(values.uses_dense_storage());
    CHECK(values.dynamic_storage_metrics().live_bytes > 0);
}

TEST_CASE("SmallDensePtrMap promotes without relocating owned objects", "[small-dense-ptr-map]")
{
    hgraph::detail::SmallDensePtrMap<std::size_t, Entry> values;
    std::vector<Entry *> stable;
    for (std::size_t key = 0; key < 8; ++key)
    {
        stable.push_back(&values.ensure(key, [key] { return std::make_unique<Entry>(static_cast<int>(key)); }));
    }

    CHECK_FALSE(values.uses_dense_storage());
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
