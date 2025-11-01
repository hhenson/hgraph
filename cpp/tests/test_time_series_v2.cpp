#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "hgraph/types/v2/time_series.h"
#include <string>
#include <vector>
#include <typeinfo>

using namespace hgraph;

namespace {
    struct Small {
        int a{0};
        static inline int new_calls = 0;
        static inline int delete_calls = 0;
        static void reset_counts() { new_calls = delete_calls = 0; }
        static void* operator new(std::size_t sz) {
            ++new_calls;
            return ::operator new(sz);
        }
        static void operator delete(void* p) noexcept {
            ++delete_calls;
            ::operator delete(p);
        }
        // Provide placement new/delete overloads so class-specific operator new doesn't hide global placement new
        static void* operator new(std::size_t, void* p) noexcept { return p; }
        static void operator delete(void*, void*) noexcept {}
    };

    struct Big {
        // Make it bigger than SBO to force heap allocation
        char buf[static_cast<std::size_t>(HGRAPH_TS_VALUE_SBO) + 32];
        int x{0};
        static inline int new_calls = 0;
        static inline int delete_calls = 0;
        static void reset_counts() { new_calls = delete_calls = 0; }
        static void* operator new(std::size_t sz) {
            ++new_calls;
            return ::operator new(sz);
        }
        static void operator delete(void* p) noexcept {
            ++delete_calls;
            ::operator delete(p);
        }
        // Provide placement new/delete as well to avoid hiding global placement new
        static void* operator new(std::size_t, void* p) noexcept { return p; }
        static void operator delete(void*, void*) noexcept {}
    };
}

TEST_CASE("SBO size matches nb::object", "[time_series][any]") {
    // Ensures we compiled with the requested SBO policy
    STATIC_REQUIRE(HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object));
}

TEST_CASE("TsEventAny: none and invalidate have no payload", "[time_series][event]") {
    engine_time_t t{};
    auto e1 = TsEventAny::none(t);
    REQUIRE(e1.kind == TsEventKind::None);
    REQUIRE_FALSE(e1.value.has_value());

    auto e2 = TsEventAny::invalidate(t);
    REQUIRE(e2.kind == TsEventKind::Invalidate);
    REQUIRE_FALSE(e2.value.has_value());
}

TEST_CASE("TsEventAny: modify with double and string", "[time_series][event]") {
    engine_time_t t{};

    auto e1 = TsEventAny::modify(t, 3.14);
    REQUIRE(e1.kind == TsEventKind::Modify);
    auto pd = e1.value.get_if<double>();
    REQUIRE(pd != nullptr);
    REQUIRE(*pd == Catch::Approx(3.14));

    std::string s = "hello";
    auto e2 = TsEventAny::modify(t, s);
    auto ps = e2.value.get_if<std::string>();
    REQUIRE(ps != nullptr);
    REQUIRE(*ps == "hello");
}

TEST_CASE("TsValueAny: none and of", "[time_series][value]") {
    auto v0 = TsValueAny::none();
    REQUIRE_FALSE(v0.has_value);

    auto v1 = TsValueAny::of(42);
    REQUIRE(v1.has_value);
    auto pi = v1.value.get_if<int>();
    REQUIRE(pi != nullptr);
    REQUIRE(*pi == 42);
}

TEST_CASE("AnyValue copy/move semantics", "[time_series][any]") {
    AnyValue<> a;
    a.emplace<std::string>("abc");
    REQUIRE(a.has_value());
    REQUIRE(a.get_if<std::string>());

    // Copy
    AnyValue<> b = a;
    REQUIRE(b.has_value());
    REQUIRE(b.get_if<std::string>());
    REQUIRE(*b.get_if<std::string>() == "abc");

    // Move
    AnyValue<> c = std::move(a);
    REQUIRE(c.has_value());
    REQUIRE(c.get_if<std::string>());
    REQUIRE(*c.get_if<std::string>() == "abc");
}

TEST_CASE("AnyValue storage path: inline vs heap via operator new counters", "[time_series][any]") {
    // Small should use inline storage (placement-new), not calling class operator new
    Small::reset_counts();
    {
        AnyValue<> v;
        v.emplace<Small>();
        REQUIRE(v.has_value());
        REQUIRE(v.get_if<Small>() != nullptr);
    }
    REQUIRE(Small::new_calls == 0);
    REQUIRE(Small::delete_calls == 0);

    // Big must exceed SBO and allocate on heap (class operator new/delete should be used)
    Big::reset_counts();
    {
        AnyValue<> v;
        v.emplace<Big>();
        REQUIRE(v.has_value());
        REQUIRE(v.get_if<Big>() != nullptr);
        // Force a copy to allocate another instance
        AnyValue<> w = v;
        (void) w;
    }
    REQUIRE(Big::new_calls >= 1);
    REQUIRE(Big::delete_calls == Big::new_calls);
}

TEST_CASE("erase_event helpers convert typed events", "[time_series][event]") {
    TsModifyEvent<int> m; m.event_time = engine_time_t{}; m.value = 7;
    TsInvalidateEvent inv; inv.event_time = engine_time_t{};
    TsNoneEvent none; none.event_time = engine_time_t{};

    auto em = erase_event(m);
    REQUIRE(em.kind == TsEventKind::Modify);
    REQUIRE(*em.value.get_if<int>() == 7);

    auto ei = erase_event(inv);
    REQUIRE(ei.kind == TsEventKind::Invalidate);
    REQUIRE_FALSE(ei.value.has_value());

    auto en = erase_event(none);
    REQUIRE(en.kind == TsEventKind::None);
    REQUIRE_FALSE(en.value.has_value());
}
