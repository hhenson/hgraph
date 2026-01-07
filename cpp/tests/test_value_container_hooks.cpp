#include <hgraph/types/value/container_hooks.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <utility>
#include <vector>

namespace hgraph::value::test {

struct HookRecorder {
    std::vector<size_t> inserts;
    std::vector<std::pair<size_t, size_t>> swaps;
    std::vector<size_t> erases;

    static void on_insert(void* ctx, size_t index) {
        static_cast<HookRecorder*>(ctx)->inserts.push_back(index);
    }

    static void on_swap(void* ctx, size_t a, size_t b) {
        static_cast<HookRecorder*>(ctx)->swaps.emplace_back(a, b);
    }

    static void on_erase(void* ctx, size_t index) {
        static_cast<HookRecorder*>(ctx)->erases.push_back(index);
    }

    [[nodiscard]] ContainerHooks hooks() {
        return ContainerHooks{this, &HookRecorder::on_insert, &HookRecorder::on_swap, &HookRecorder::on_erase};
    }
};

}  // namespace hgraph::value::test

TEST_CASE("SetView hook surface provides indices and swap-with-last notifications", "[value][set][hooks]") {
    using namespace hgraph::value;
    using hgraph::value::test::HookRecorder;

    auto& reg = TypeRegistry::instance();
    const TypeMeta* i64 = scalar_type_meta<int64_t>();
    const TypeMeta* set_schema = reg.set(i64).build();

    PlainValue set_value(set_schema);
    SetView s = set_value.view().as_set();

    HookRecorder rec;
    const auto hooks = rec.hooks();

    PlainValue v1(int64_t{1});
    PlainValue v2(int64_t{2});

    auto idx1 = s.insert_with_index(v1.const_view(), hooks);
    REQUIRE(idx1.has_value());
    REQUIRE(*idx1 == 0);
    REQUIRE(rec.inserts == std::vector<size_t>{0});

    auto idx2 = s.insert_with_index(v2.const_view(), hooks);
    REQUIRE(idx2.has_value());
    REQUIRE(*idx2 == 1);
    REQUIRE(rec.inserts == std::vector<size_t>{0, 1});

    // Erase non-last element triggers swap-with-last then erase of last slot
    bool erased = s.erase_with_hooks(v1.const_view(), hooks);
    REQUIRE(erased);

    REQUIRE(rec.swaps == std::vector<std::pair<size_t, size_t>>{{0, 1}});
    REQUIRE(rec.erases == std::vector<size_t>{1});

    REQUIRE(s.size() == 1);
    REQUIRE_FALSE(s.contains(v1.const_view()));
    REQUIRE(s.contains(v2.const_view()));

    // Remaining element should now be in slot 0
    auto idx2_after = s.find_index(v2.const_view());
    REQUIRE(idx2_after.has_value());
    REQUIRE(*idx2_after == 0);
}

TEST_CASE("MapView hook surface provides indices and swap-with-last notifications", "[value][map][hooks]") {
    using namespace hgraph::value;
    using hgraph::value::test::HookRecorder;

    auto& reg = TypeRegistry::instance();
    const TypeMeta* i64 = scalar_type_meta<int64_t>();
    const TypeMeta* map_schema = reg.map(i64, i64).build();

    PlainValue map_value(map_schema);
    MapView m = map_value.view().as_map();

    HookRecorder rec;
    const auto hooks = rec.hooks();

    PlainValue k1(int64_t{1});
    PlainValue v1(int64_t{10});
    PlainValue k2(int64_t{2});
    PlainValue v2(int64_t{20});

    auto r1 = m.set_with_index(k1.const_view(), v1.const_view(), hooks);
    REQUIRE(r1.inserted);
    REQUIRE(r1.index == 0);
    REQUIRE(rec.inserts == std::vector<size_t>{0});

    // Updating existing key should not trigger insert hook
    PlainValue v1b(int64_t{11});
    auto r1b = m.set_with_index(k1.const_view(), v1b.const_view(), hooks);
    REQUIRE_FALSE(r1b.inserted);
    REQUIRE(r1b.index == 0);
    REQUIRE(rec.inserts == std::vector<size_t>{0});
    REQUIRE(m.at(k1.const_view()).as<int64_t>() == 11);

    auto r2 = m.set_with_index(k2.const_view(), v2.const_view(), hooks);
    REQUIRE(r2.inserted);
    REQUIRE(r2.index == 1);
    REQUIRE(rec.inserts == std::vector<size_t>{0, 1});

    // Erase non-last key triggers swap-with-last then erase of last slot
    bool erased = m.erase_with_hooks(k1.const_view(), hooks);
    REQUIRE(erased);
    REQUIRE(rec.swaps == std::vector<std::pair<size_t, size_t>>{{0, 1}});
    REQUIRE(rec.erases == std::vector<size_t>{1});

    REQUIRE(m.size() == 1);
    REQUIRE_FALSE(m.contains(k1.const_view()));
    REQUIRE(m.contains(k2.const_view()));
    REQUIRE(m.at(k2.const_view()).as<int64_t>() == 20);

    auto idx2_after = m.find_index(k2.const_view());
    REQUIRE(idx2_after.has_value());
    REQUIRE(*idx2_after == 0);
}
