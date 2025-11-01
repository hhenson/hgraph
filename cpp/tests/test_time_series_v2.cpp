#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "hgraph/types/v2/time_series.h"
#include <string>
#include <vector>
#include <typeinfo>

using namespace hgraph;

// Bring hgraph::to_string into scope explicitly for ADL clarity
using hgraph::to_string;

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



TEST_CASE("TypeId equality and hashing", "[time_series][typeid][hash]") {
    TypeId id_i1{ &typeid(int64_t) };
    TypeId id_i2{ &typeid(int64_t) };
    TypeId id_d{ &typeid(double) };

    REQUIRE(id_i1 == id_i2);
    REQUIRE_FALSE(id_i1 == id_d);

    std::size_t h1 = std::hash<TypeId>{}(id_i1);
    std::size_t h2 = std::hash<TypeId>{}(id_i2);
    REQUIRE(h1 == h2);
}

TEST_CASE("AnyValue hash_code: empty and primitives", "[time_series][any][hash]") {
    // Empty
    AnyValue<> empty;
    REQUIRE_FALSE(empty.has_value());
    REQUIRE(empty.hash_code() == 0);

    // int64_t
    AnyValue<> vi;
    vi.emplace<int64_t>(42);
    REQUIRE(vi.has_value());
    REQUIRE(vi.type().info == &typeid(int64_t));
    REQUIRE(vi.hash_code() == std::hash<int64_t>{}(42));

    // Copy preserves hash
    AnyValue<> vi_copy = vi;
    REQUIRE(vi_copy.hash_code() == vi.hash_code());
    REQUIRE(vi_copy.type().info == vi.type().info);

    // Move preserves hash
    AnyValue<> vi_move = std::move(vi_copy);
    REQUIRE(vi_move.hash_code() == std::hash<int64_t>{}(42));
    REQUIRE(vi_move.type().info == &typeid(int64_t));

    // double
    AnyValue<> vd;
    vd.emplace<double>(3.14);
    REQUIRE(vd.has_value());
    REQUIRE(vd.type().info == &typeid(double));
    REQUIRE(vd.hash_code() == std::hash<double>{}(3.14));
}

TEST_CASE("AnyValue hash_code: std::string and stability across copies", "[time_series][any][hash]") {
    AnyValue<> vs1;
    vs1.emplace<std::string>("hello");
    REQUIRE(vs1.has_value());
    REQUIRE(vs1.type().info == &typeid(std::string));

    const auto h_expected = std::hash<std::string>{}("hello");
    REQUIRE(vs1.hash_code() == h_expected);

    AnyValue<> vs2 = vs1;           // copy
    AnyValue<> vs3 = std::move(vs2); // move

    REQUIRE(vs3.hash_code() == h_expected);
    REQUIRE(vs3.type().info == &typeid(std::string));

    // Independent instance with same payload should hash the same
    AnyValue<> vs4;
    vs4.emplace<std::string>("hello");
    REQUIRE(vs4.hash_code() == h_expected);
}

#include <catch2/matchers/catch_matchers_string.hpp>

TEST_CASE("to_string for AnyValue<>", "[time_series][any][string]") {
    // Empty
    AnyValue<> v0;
    REQUIRE(to_string(v0) == std::string("<empty>"));

    // int64_t
    AnyValue<> vi; vi.emplace<int64_t>(42);
    REQUIRE(to_string(vi) == std::string("42"));

    // double (std::to_string may include many decimals); just check prefix "3.14"
    AnyValue<> vd; vd.emplace<double>(3.14);
    auto ds = to_string(vd);
    REQUIRE_THAT(ds, Catch::Matchers::StartsWith("3.14"));

    // std::string
    AnyValue<> vs; vs.emplace<std::string>("hello");
    REQUIRE(to_string(vs) == std::string("hello"));
}

TEST_CASE("to_string for TsEventAny", "[time_series][event][string]") {
    engine_time_t t{};

    auto e_none = TsEventAny::none(t);
    auto s_none = to_string(e_none);
    REQUIRE(s_none.find("TsEventAny{") != std::string::npos);
    REQUIRE(s_none.find("kind=None") != std::string::npos);

    auto e_inv = TsEventAny::invalidate(t);
    auto s_inv = to_string(e_inv);
    REQUIRE(s_inv.find("kind=Invalidate") != std::string::npos);

    auto e_mod = TsEventAny::modify(t, 3.14);
    auto s_mod = to_string(e_mod);
    REQUIRE(s_mod.find("kind=Modify") != std::string::npos);
    REQUIRE(s_mod.find("value=") != std::string::npos);
}

TEST_CASE("to_string for TsValueAny", "[time_series][value][string]") {
    auto v_none = TsValueAny::none();
    auto s_none = to_string(v_none);
    REQUIRE(s_none.find("TsValueAny{") != std::string::npos);
    REQUIRE(s_none.find("none") != std::string::npos);

    auto v_str = TsValueAny::of(std::string("hello"));
    auto s_val = to_string(v_str);
    REQUIRE(s_val.find("value=hello") != std::string::npos);
}
