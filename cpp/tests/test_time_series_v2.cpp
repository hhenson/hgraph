#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "hgraph/types/v2/ts_event.h"
#include <string>
#include <vector>
#include <map>
#include <typeinfo>

using namespace hgraph;

// Bring hgraph::to_string into scope explicitly for ADL clarity
using hgraph::to_string;

namespace
{
    struct Small
    {
        int a{0};
        static inline int new_calls = 0;
        static inline int delete_calls = 0;
        static void reset_counts() { new_calls = delete_calls = 0; }

        static void* operator new(std::size_t sz)
        {
            ++new_calls;
            return ::operator new(sz);
        }

        static void operator delete(void* p) noexcept
        {
            ++delete_calls;
            ::operator delete(p);
        }

        // Provide placement new/delete overloads so class-specific operator new doesn't hide global placement new
        static void* operator new(std::size_t, void* p) noexcept { return p; }

        static void operator delete(void*, void*) noexcept
        {
        }
    };

    struct Big
    {
        // Make it bigger than SBO to force heap allocation
        char buf[static_cast<std::size_t>(HGRAPH_TS_VALUE_SBO) + 32];
        int x{0};
        static inline int new_calls = 0;
        static inline int delete_calls = 0;
        static void reset_counts() { new_calls = delete_calls = 0; }

        static void* operator new(std::size_t sz)
        {
            ++new_calls;
            return ::operator new(sz);
        }

        static void operator delete(void* p) noexcept
        {
            ++delete_calls;
            ::operator delete(p);
        }

        // Provide placement new/delete as well to avoid hiding global placement new
        static void* operator new(std::size_t, void* p) noexcept { return p; }

        static void operator delete(void*, void*) noexcept
        {
        }
    };
}

TEST_CASE (

"SBO size matches nb::object"
,
"[time_series][any]"
)
{
    // Ensures we compiled with the requested SBO policy
    STATIC_REQUIRE(HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object));
}

TEST_CASE (

"TsEventAny: none and invalidate have no payload"
,
"[time_series][event]"
)
{
    engine_time_t t{};
    auto e1 = TsEventAny::none(t);
    REQUIRE(e1.kind == TsEventKind::None);
    REQUIRE_FALSE(e1.value.has_value());

    auto e2 = TsEventAny::invalidate(t);
    REQUIRE(e2.kind == TsEventKind::Invalidate);
    REQUIRE_FALSE(e2.value.has_value());
}

TEST_CASE (

"TsEventAny: modify with double and string"
,
"[time_series][event]"
)
{
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

TEST_CASE (

"TsValueAny: none and of"
,
"[time_series][value]"
)
{
    auto v0 = TsValueAny::none();
    REQUIRE_FALSE(v0.has_value);

    auto v1 = TsValueAny::of(42);
    REQUIRE(v1.has_value);
    auto pi = v1.value.get_if<int>();
    REQUIRE(pi != nullptr);
    REQUIRE(*pi == 42);
}

TEST_CASE (

"AnyValue copy/move semantics"
,
"[time_series][any]"
)
{
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

TEST_CASE (

"AnyValue storage path: inline vs heap via operator new counters"
,
"[time_series][any]"
)
{
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
        (void)w;
    }
    REQUIRE(Big::new_calls >= 1);
    REQUIRE(Big::delete_calls == Big::new_calls);
}

TEST_CASE (

"AnyValue storage_size: empty container"
,
"[time_series][any][storage]"
)
{
    AnyValue<> empty;
    REQUIRE_FALSE(empty.has_value());
    REQUIRE(empty.storage_size() == 0);
    REQUIRE_FALSE(empty.is_inline());
    REQUIRE_FALSE(empty.is_heap_allocated());
}

TEST_CASE (

"AnyValue storage_size: inline (SBO) types"
,
"[time_series][any][storage]"
)
{
    // Small primitives should use inline storage
    AnyValue<> v_int;
    v_int.emplace<int>(42);
    REQUIRE(v_int.is_inline());
    REQUIRE_FALSE(v_int.is_heap_allocated());
    REQUIRE(v_int.storage_size() == HGRAPH_TS_VALUE_SBO);

    AnyValue<> v_double;
    v_double.emplace<double>(3.14);
    REQUIRE(v_double.is_inline());
    REQUIRE_FALSE(v_double.is_heap_allocated());
    REQUIRE(v_double.storage_size() == HGRAPH_TS_VALUE_SBO);

    // Small struct should use inline storage
    AnyValue<> v_small;
    v_small.emplace<Small>();
    REQUIRE(v_small.is_inline());
    REQUIRE_FALSE(v_small.is_heap_allocated());
    REQUIRE(v_small.storage_size() == HGRAPH_TS_VALUE_SBO);
}

TEST_CASE (

"AnyValue storage_size: heap-allocated types"
,
"[time_series][any][storage]"
)
{
    // Big struct exceeds SBO and must be heap-allocated
    AnyValue<> v_big;
    v_big.emplace<Big>();
    REQUIRE_FALSE(v_big.is_inline());
    REQUIRE(v_big.is_heap_allocated());
    REQUIRE(v_big.storage_size() == sizeof(void*));

    // Long string should be heap-allocated (if it exceeds SBO)
    AnyValue<> v_string;
    v_string.emplace<std::string>("This is a reasonably long string that might exceed SBO");
    // Note: std::string itself may fit in SBO (it's just 24-32 bytes typically)
    // but we test that storage_size returns correct value based on using_heap_ flag
    if (v_string.is_heap_allocated())
    {
        REQUIRE(v_string.storage_size() == sizeof(void*));
    }
    else
    {
        REQUIRE(v_string.storage_size() == HGRAPH_TS_VALUE_SBO);
    }
}

TEST_CASE (

"AnyValue storage_size: references"
,
"[time_series][any][storage][ref]"
)
{
    int x = 42;
    AnyValue<> v_ref;
    v_ref.emplace_ref(x);

    // References use heap flag (even though they're just storing a pointer)
    REQUIRE(v_ref.is_reference());
    REQUIRE(v_ref.is_heap_allocated());
    REQUIRE_FALSE(v_ref.is_inline());
    REQUIRE(v_ref.storage_size() == sizeof(void*));
}

TEST_CASE (

"AnyValue storage_size: after copy and move"
,
"[time_series][any][storage]"
)
{
    // Inline value
    AnyValue<> v1;
    v1.emplace<int>(42);
    REQUIRE(v1.is_inline());

    AnyValue<> v2 = v1; // Copy
    REQUIRE(v2.is_inline());
    REQUIRE(v2.storage_size() == v1.storage_size());

    AnyValue<> v3 = std::move(v1); // Move
    REQUIRE(v3.is_inline());
    REQUIRE(v3.storage_size() == HGRAPH_TS_VALUE_SBO);

    // Heap-allocated value
    AnyValue<> v4;
    v4.emplace<Big>();
    REQUIRE(v4.is_heap_allocated());

    AnyValue<> v5 = v4; // Copy (allocates new heap object)
    REQUIRE(v5.is_heap_allocated());
    REQUIRE(v5.storage_size() == sizeof(void*));

    AnyValue<> v6 = std::move(v4); // Move (transfers heap pointer)
    REQUIRE(v6.is_heap_allocated());
    REQUIRE(v6.storage_size() == sizeof(void*));
}

TEST_CASE (

"AnyValue storage_size: after reset"
,
"[time_series][any][storage]"
)
{
    AnyValue<> v;
    v.emplace<int>(42);
    REQUIRE(v.storage_size() == HGRAPH_TS_VALUE_SBO);

    v.reset();
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.storage_size() == 0);
    REQUIRE_FALSE(v.is_inline());
    REQUIRE_FALSE(v.is_heap_allocated());
}

TEST_CASE (

"AnyValue storage_size: reference materialization"
,
"[time_series][any][storage][ref]"
)
{
    std::string s = "hello";
    AnyValue<> v_ref;
    v_ref.emplace_ref(s);
    REQUIRE(v_ref.is_reference());
    REQUIRE(v_ref.is_heap_allocated());
    REQUIRE(v_ref.storage_size() == sizeof(void*));

    // Copy materializes the reference into an owned value
    AnyValue<> v_owned = v_ref;
    REQUIRE_FALSE(v_owned.is_reference());
    // String is small enough for SBO typically
    if (v_owned.is_inline())
    {
        REQUIRE(v_owned.storage_size() == HGRAPH_TS_VALUE_SBO);
    }
    else
    {
        REQUIRE(v_owned.storage_size() == sizeof(void*));
    }

    // ensure_owned converts in place
    AnyValue<> v_ref2;
    v_ref2.emplace_ref(s);
    v_ref2.ensure_owned();
    REQUIRE_FALSE(v_ref2.is_reference());
}


TEST_CASE (

"TypeId equality and hashing"
,
"[time_series][typeid][hash]"
)
{
    TypeId id_i1{&typeid(int64_t)};
    TypeId id_i2{&typeid(int64_t)};
    TypeId id_d{&typeid(double)};

    REQUIRE(id_i1 == id_i2);
    REQUIRE_FALSE(id_i1 == id_d);

    std::size_t h1 = std::hash<TypeId>{}(id_i1);
    std::size_t h2 = std::hash<TypeId>{}(id_i2);
    REQUIRE(h1 == h2);
}

TEST_CASE (

"AnyValue hash_code: empty and primitives"
,
"[time_series][any][hash]"
)
{
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

TEST_CASE (

"AnyValue hash_code: std::string and stability across copies"
,
"[time_series][any][hash]"
)
{
    AnyValue<> vs1;
    vs1.emplace<std::string>("hello");
    REQUIRE(vs1.has_value());
    REQUIRE(vs1.type().info == &typeid(std::string));

    const auto h_expected = std::hash<std::string>{}("hello");
    REQUIRE(vs1.hash_code() == h_expected);

    AnyValue<> vs2 = vs1; // copy
    AnyValue<> vs3 = std::move(vs2); // move

    REQUIRE(vs3.hash_code() == h_expected);
    REQUIRE(vs3.type().info == &typeid(std::string));

    // Independent instance with same payload should hash the same
    AnyValue<> vs4;
    vs4.emplace<std::string>("hello");
    REQUIRE(vs4.hash_code() == h_expected);
}

#include <catch2/matchers/catch_matchers_string.hpp>

TEST_CASE (

"to_string for AnyValue<>"
,
"[time_series][any][string]"
)
{
    // Empty
    AnyValue<> v0;
    REQUIRE(to_string(v0) == std::string("<empty>"));

    // int64_t
    AnyValue<> vi;
    vi.emplace<int64_t>(42);
    REQUIRE(to_string(vi) == std::string("42"));

    // double (std::to_string may include many decimals); just check prefix "3.14"
    AnyValue<> vd;
    vd.emplace<double>(3.14);
    auto ds = to_string(vd);
    REQUIRE_THAT(ds, Catch::Matchers::StartsWith("3.14"));

    // std::string
    AnyValue<> vs;
    vs.emplace<std::string>("hello");
    REQUIRE(to_string(vs) == std::string("hello"));
}

TEST_CASE (

"to_string for TsEventAny"
,
"[time_series][event][string]"
)
{
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

TEST_CASE (

"to_string for TsValueAny"
,
"[time_series][value][string]"
)
{
    auto v_none = TsValueAny::none();
    auto s_none = to_string(v_none);
    REQUIRE(s_none.find("TsValueAny{") != std::string::npos);
    REQUIRE(s_none.find("none") != std::string::npos);

    auto v_str = TsValueAny::of(std::string("hello"));
    auto s_val = to_string(v_str);
    REQUIRE(s_val.find("value=hello") != std::string::npos);
}


// ---------------- Collection event tests ----------------
TEST_CASE (

"TsCollectionEventAny: none/invalidate/modify structure"
,
"[time_series][collection][event]"
)
{
    engine_time_t t{};

    // None
    auto e_none = TsCollectionEventAny::none(t);
    REQUIRE(e_none.kind == TsEventKind::None);
    REQUIRE(e_none.items.empty());

    // Invalidate
    auto e_inv = TsCollectionEventAny::invalidate(t);
    REQUIRE(e_inv.kind == TsEventKind::Invalidate);
    REQUIRE(e_inv.items.empty());

    // Modify and add items
    auto e_mod = TsCollectionEventAny::modify(t);
    REQUIRE(e_mod.kind == TsEventKind::Modify);
    REQUIRE(e_mod.items.empty());

    // add modify: key=int64_t(1), value=double(3.5)
    AnyKey k1;
    k1.emplace<int64_t>(1);
    AnyValue<> v1;
    v1.emplace<double>(3.5);
    e_mod.add_modify(std::move(k1), std::move(v1));

    // add reset: key=int64_t(2)
    AnyKey k2;
    k2.emplace<int64_t>(2);
    e_mod.add_reset(std::move(k2));

    // remove: key=std::string("gone")
    AnyKey r1;
    r1.emplace<std::string>("gone");
    e_mod.remove(std::move(r1));

    REQUIRE(e_mod.items.size() == 3);

    // Inspect first add/modify
    const auto& it0 = e_mod.items[0];
    REQUIRE(it0.kind == ColItemKind::Modify);
    auto pkey0 = it0.key.get_if<int64_t>();
    REQUIRE(pkey0 != nullptr);
    REQUIRE(*pkey0 == 1);
    auto pval0 = it0.value.get_if<double>();
    REQUIRE(pval0 != nullptr);
    REQUIRE(*pval0 == Catch::Approx(3.5));

    // Inspect second (reset)
    const auto& it1 = e_mod.items[1];
    REQUIRE(it1.kind == ColItemKind::Reset);
    auto pkey1 = it1.key.get_if<int64_t>();
    REQUIRE(pkey1 != nullptr);
    REQUIRE(*pkey1 == 2);
    REQUIRE_FALSE(it1.value.has_value());

    // Inspect third (remove)
    const auto& it2 = e_mod.items[2];
    REQUIRE(it2.kind == ColItemKind::Remove);
    auto prk = it2.key.get_if<std::string>();
    REQUIRE(prk != nullptr);
    REQUIRE(*prk == std::string("gone"));
}

#include <catch2/matchers/catch_matchers_string.hpp>

// ---- AnyValue equality tests ----
TEST_CASE (

"AnyValue equality: empty and basic types"
,
"[any][eq]"
)
{
    AnyValue<> e1, e2;
    REQUIRE(e1 == e2); // both empty

    AnyValue<> v1;
    v1.emplace<int64_t>(42);
    AnyValue<> v2;
    v2.emplace<int64_t>(42);
    AnyValue<> v3;
    v3.emplace<int64_t>(43);

    REQUIRE(v1 == v2);
    REQUIRE(v1 != v3);

    AnyValue<> d1;
    d1.emplace<double>(3.14);
    AnyValue<> d2;
    d2.emplace<double>(3.14);
    AnyValue<> d3;
    d3.emplace<double>(2.71);
    REQUIRE(d1 == d2);
    REQUIRE(d1 != d3);

    AnyValue<> s1;
    s1.emplace<std::string>("abc");
    AnyValue<> s2;
    s2.emplace<std::string>("abc");
    AnyValue<> s3;
    s3.emplace<std::string>("xyz");
    REQUIRE(s1 == s2);
    REQUIRE(s1 != s3);

    // different types
    REQUIRE(v1 != d1);
    REQUIRE(v1 != s1);
}

TEST_CASE (

"AnyValue equality: engine_time_t"
,
"[any][eq]"
)
{
    engine_time_t t{};
    AnyValue<> a;
    a.emplace<engine_time_t>(t);
    AnyValue<> b;
    b.emplace<engine_time_t>(t);
    REQUIRE(a == b);
}

// ---- AnyValue reference semantics tests ----
TEST_CASE (

"AnyValue reference: get_if returns referent, copy materializes"
,
"[any][ref]"
)
{
    std::string s = "abc";
    AnyValue<> v;
    v.emplace_ref(s);

    auto ps = v.get_if<std::string>();
    REQUIRE(ps != nullptr);
    REQUIRE(*ps == "abc");

    s = "xyz";
    REQUIRE(*v.get_if<std::string>() == "xyz");

    AnyValue<> v2 = v; // copy materializes
    REQUIRE_FALSE(v2.is_reference());
    REQUIRE(v2.get_if<std::string>());
    REQUIRE(*v2.get_if<std::string>() == "xyz");

    s = "after";
    REQUIRE(*v.get_if<std::string>() == "after");
    REQUIRE(*v2.get_if<std::string>() == "xyz");
}

TEST_CASE (

"AnyValue reference: move also materializes destination"
,
"[any][ref]"
)
{
    std::string s = "hello";
    AnyValue<> v;
    v.emplace_ref(s);

    AnyValue<> w = std::move(v);
    REQUIRE_FALSE(w.is_reference());
    REQUIRE(*w.get_if<std::string>() == "hello");

    // Source remains a reference
    REQUIRE(v.is_reference());
    REQUIRE(*v.get_if<std::string>() == "hello");
}

TEST_CASE (

"AnyValue reference: hash stable across materialization"
,
"[any][ref][hash]"
)
{
    int64_t x = 42;
    AnyValue<> r;
    r.emplace_ref(x);
    const auto h_ref = r.hash_code();
    REQUIRE(h_ref == std::hash<int64_t>{}(42));

    AnyValue<> owned = r; // materialize via copy
    REQUIRE(owned.hash_code() == h_ref);
}

TEST_CASE (

"AnyValue ensure_owned() converts a reference in place"
,
"[any][ref]"
)
{
    std::string s = "snap";
    AnyValue<> v;
    v.emplace_ref(s);
    v.ensure_owned();
    REQUIRE_FALSE(v.is_reference());

    s = "different";
    REQUIRE(*v.get_if<std::string>() == "snap");
}

TEST_CASE (

"to_string for TsCollectionEventAny"
,
"[time_series][collection][string]"
)
{
    engine_time_t t{};
    auto e = TsCollectionEventAny::modify(t);

    AnyKey k1;
    k1.emplace<int64_t>(7);
    AnyValue<> v1;
    v1.emplace<std::string>("hello");
    e.add_modify(std::move(k1), std::move(v1));

    AnyKey k2;
    k2.emplace<int64_t>(8);
    e.add_reset(std::move(k2));

    AnyKey r;
    r.emplace<int64_t>(9);
    e.remove(std::move(r));

    auto s = to_string(e);
    REQUIRE(s.find("TsCollectionEventAny{") != std::string::npos);
    REQUIRE(s.find("kind=Modify") != std::string::npos);
    REQUIRE(s.find("items=") != std::string::npos);
    REQUIRE(s.find("key=7") != std::string::npos);
    REQUIRE(s.find("value=hello") != std::string::npos);
    REQUIRE(s.find("Reset") != std::string::npos);
    REQUIRE(s.find("Remove") != std::string::npos);
}


// ---- Recover event tests ----
TEST_CASE (

"TsEventAny: recover without payload"
,
"[time_series][event][recover]"
)
{
    engine_time_t t{};
    auto e = TsEventAny::recover(t);
    REQUIRE(e.kind == TsEventKind::Recover);
    REQUIRE_FALSE(e.value.has_value());
    auto s = to_string(e);
    REQUIRE(s.find("kind=Recover") != std::string::npos);
    REQUIRE(s.find("value=") == std::string::npos);
}

TEST_CASE (

"TsEventAny: recover with payload"
,
"[time_series][event][recover]"
)
{
    engine_time_t t{};
    auto e = TsEventAny::recover(t, static_cast<int64_t>(42));
    REQUIRE(e.kind == TsEventKind::Recover);
    auto p = e.value.get_if<int64_t>();
    REQUIRE(p != nullptr);
    REQUIRE(*p == 42);
    auto s = to_string(e);
    REQUIRE(s.find("kind=Recover") != std::string::npos);
    REQUIRE(s.find("value=42") != std::string::npos);
}

TEST_CASE (

"TsCollectionEventAny: recover header only"
,
"[time_series][collection][recover]"
)
{
    engine_time_t t{};
    auto e = TsCollectionEventAny::recover(t);
    REQUIRE(e.kind == TsEventKind::Recover);
    REQUIRE(e.items.empty());
    auto s = to_string(e);
    REQUIRE(s.find("kind=Recover") != std::string::npos);
    // items list printed only when kind==Modify
    REQUIRE(s.find("items=") == std::string::npos);
}


// ---- AnyValue optional less-than tests ----
TEST_CASE (

"AnyValue < : comparable primitives"
,
"[any][lt]"
)
{
    AnyValue<> a;
    a.emplace<int64_t>(1);
    AnyValue<> b;
    b.emplace<int64_t>(2);
    AnyValue<> c;
    c.emplace<int64_t>(2);

    REQUIRE(a < b);
    REQUIRE_FALSE(b < a);
    REQUIRE_FALSE(b < c);

    AnyValue<> d;
    d.emplace<double>(3.14);
    AnyValue<> e;
    e.emplace<double>(6.28);
    REQUIRE(d < e);
    REQUIRE_FALSE(e < d);

    AnyValue<> s1;
    s1.emplace<std::string>("abc");
    AnyValue<> s2;
    s2.emplace<std::string>("abd");
    REQUIRE(s1 < s2);
    REQUIRE_FALSE(s2 < s1);
}

TEST_CASE (

"AnyValue < : reference vs owned"
,
"[any][lt][ref]"
)
{
    std::string referent = "b";
    AnyValue<> r;
    r.emplace_ref(referent);

    AnyValue<> o;
    o.emplace<std::string>("c");
    REQUIRE(r < o);
    REQUIRE_FALSE(o < r);

    // mutate referent; comparison should reflect new value through reference
    referent = "d";
    REQUIRE_FALSE(r < o); // "d" < "c" is false
    REQUIRE(o < r); // "c" < "d" is true
}

TEST_CASE (

"AnyValue < : type mismatch throws"
,
"[any][lt][throws]"
)
{
    AnyValue<> i;
    i.emplace<int64_t>(1);
    AnyValue<> d;
    d.emplace<double>(2.0);
    REQUIRE_THROWS_AS((void)(i < d), std::runtime_error);
    REQUIRE_THROWS_AS((void)(d < i), std::runtime_error);
}

TEST_CASE (

"AnyValue < : unsupported type throws"
,
"[any][lt][throws]"
)
{
    struct NoLess
    {
        int x;
    };
    AnyValue<> a;
    a.emplace<NoLess>(NoLess{1});
    AnyValue<> b;
    b.emplace<NoLess>(NoLess{2});
    REQUIRE_THROWS_AS((void)(a < b), std::runtime_error);
}

TEST_CASE (

"AnyValue < : empty comparisons"
,
"[any][lt][empty]"
)
{
    AnyValue<> e1, e2;
    REQUIRE_FALSE(e1 < e2); // both empty => false

    AnyValue<> v;
    v.emplace<int64_t>(1);
    REQUIRE_THROWS_AS((void)(e1 < v), std::runtime_error);
    REQUIRE_THROWS_AS((void)(v < e1), std::runtime_error);
}

// ---- AnyValue visitor pattern tests ----
TEST_CASE (

"AnyValue visit_as: type-safe visitation"
,
"[any][visitor]"
)
{
    AnyValue<> v;
    v.emplace<int64_t>(42);

    // Visit with matching type
    int64_t result = 0;
    bool visited = v.visit_as<int64_t>([&result](int64_t val)
    {
        result = val * 2;
    });
    REQUIRE(visited);
    REQUIRE(result == 84);

    // Visit with non-matching type
    visited = v.visit_as<double>([](double)
    {
        REQUIRE(false); // Should not be called
    });
    REQUIRE_FALSE(visited);

    // Empty AnyValue
    AnyValue<> empty;
    visited = empty.visit_as<int64_t>([](int64_t)
    {
        REQUIRE(false); // Should not be called
    });
    REQUIRE_FALSE(visited);
}

TEST_CASE (

"AnyValue visit_as: mutable visitation"
,
"[any][visitor]"
)
{
    AnyValue<> v;
    v.emplace<int64_t>(42);

    // Modify value through visitor
    bool visited = v.visit_as<int64_t>([](int64_t& val)
    {
        val = 100;
    });
    REQUIRE(visited);
    REQUIRE(*v.get_if<int64_t>() == 100);
}

TEST_CASE (

"AnyValue visit_as: with std::string"
,
"[any][visitor]"
)
{
    AnyValue<> v;
    v.emplace<std::string>("hello");

    std::string result;
    bool visited = v.visit_as<std::string>([&result](const std::string& s)
    {
        result = s + " world";
    });
    REQUIRE(visited);
    REQUIRE(result == "hello world");

    // Modify the string
    visited = v.visit_as<std::string>([](std::string& s)
    {
        s = "goodbye";
    });
    REQUIRE(visited);
    REQUIRE(*v.get_if<std::string>() == "goodbye");
}

TEST_CASE (

"AnyValue visit_as: with references"
,
"[any][visitor][ref]"
)
{
    int x = 42;
    AnyValue<> v;
    v.emplace_ref(x);

    // Visit reference (const)
    int result = 0;
    bool visited = v.visit_as<int>([&result](int val)
    {
        result = val;
    });
    REQUIRE(visited);
    REQUIRE(result == 42);

    // Modify through reference - this modifies the referent
    x = 100;
    visited = v.visit_as<int>([&result](int val)
    {
        result = val;
    });
    REQUIRE(visited);
    REQUIRE(result == 100);
}

TEST_CASE (

"AnyValue visit_untyped: introspection"
,
"[any][visitor]"
)
{
    AnyValue<> v_int;
    v_int.emplace<int64_t>(42);

    // Visit with type info
    bool visited = false;
    const void* ptr = nullptr;
    const std::type_info* tinfo = nullptr;

    v_int.visit_untyped([&](const void* p, const std::type_info& ti)
    {
        visited = true;
        ptr = p;
        tinfo = &ti;
    });

    REQUIRE(visited);
    REQUIRE(ptr != nullptr);
    REQUIRE(tinfo == &typeid(int64_t));
    REQUIRE(*static_cast<const int64_t*>(ptr) == 42);
}

TEST_CASE (

"AnyValue visit_untyped: with std::string"
,
"[any][visitor]"
)
{
    AnyValue<> v;
    v.emplace<std::string>("test");

    bool visited = false;
    v.visit_untyped([&visited](const void* p, const std::type_info& ti)
    {
        visited = true;
        REQUIRE(ti == typeid(std::string));
        const auto* s = static_cast<const std::string*>(p);
        REQUIRE(*s == "test");
    });
    REQUIRE(visited);
}

TEST_CASE (

"AnyValue visit_untyped: empty does nothing"
,
"[any][visitor]"
)
{
    AnyValue<> empty;

    bool visited = false;
    empty.visit_untyped([&visited](const void*, const std::type_info&)
    {
        visited = true;
    });
    REQUIRE_FALSE(visited);
}

TEST_CASE (

"AnyValue visitor: combined pattern"
,
"[any][visitor]"
)
{
    // Demonstrates using both visit types together
    AnyValue<> v;
    v.emplace<double>(3.14);

    // Try multiple types with visit_as
    bool found = false;
    found = v.visit_as<int64_t>([](int64_t)
    {
        REQUIRE(false); // Not this type
    });
    REQUIRE_FALSE(found);

    found = v.visit_as<std::string>([](const std::string&)
    {
        REQUIRE(false); // Not this type
    });
    REQUIRE_FALSE(found);

    found = v.visit_as<double>([](double val)
    {
        REQUIRE(val == Catch::Approx(3.14));
    });
    REQUIRE(found);

    // Or use visit_untyped for dynamic dispatch
    v.visit_untyped([](const void* p, const std::type_info& ti)
    {
        if (ti == typeid(double))
        {
            double val = *static_cast<const double*>(p);
            REQUIRE(val == Catch::Approx(3.14));
        }
        else if (ti == typeid(int64_t))
        {
            REQUIRE(false); // Not this type
        }
    });
}

// =============================================================================
// TsEventAny Tests
// =============================================================================

// Helper to create AnyValue/AnyKey
template <typename T>
static AnyValue<> make_any(T&& value)
{
    AnyValue<> av;
    av.emplace<std::decay_t<T>>(std::forward<T>(value));
    return av;
}

TEST_CASE (

"TsEventAny validation"
,
"[ts_event][validation]"
)
{
    using namespace std::chrono;
    auto t = engine_time_t{microseconds{1000}};

    SECTION("Valid events")
    {
        auto none_event = TsEventAny::none(t);
        REQUIRE(none_event.is_valid());

        auto invalidate_event = TsEventAny::invalidate(t);
        REQUIRE(invalidate_event.is_valid());

        auto modify_event = TsEventAny::modify(t, 42);
        REQUIRE(modify_event.is_valid());

        auto recover_no_value = TsEventAny::recover(t);
        REQUIRE(recover_no_value.is_valid());

        auto recover_with_value = TsEventAny::recover(t, 3.14);
        REQUIRE(recover_with_value.is_valid());
    }

    SECTION("Invalid events - manually constructed with wrong value presence")
    {
        // None should have no value
        TsEventAny invalid_none{t, TsEventKind::None, make_any(42)};
        REQUIRE_FALSE(invalid_none.is_valid());

        // Invalidate should have no value
        TsEventAny invalid_invalidate{t, TsEventKind::Invalidate, make_any(42)};
        REQUIRE_FALSE(invalid_invalidate.is_valid());

        // Modify must have value
        TsEventAny invalid_modify{t, TsEventKind::Modify, {}};
        REQUIRE_FALSE(invalid_modify.is_valid());
    }
}

TEST_CASE (

"TsEventAny equality operators"
,
"[ts_event][equality]"
)
{
    using namespace std::chrono;
    auto t1 = engine_time_t{microseconds{1000}};
    auto t2 = engine_time_t{microseconds{2000}};

    SECTION("Equal modify events")
    {
        auto e1 = TsEventAny::modify(t1, 42);
        auto e2 = TsEventAny::modify(t1, 42);

        REQUIRE(e1 == e2);
        REQUIRE_FALSE(e1 != e2);
    }

    SECTION("Modify events with different values")
    {
        auto e1 = TsEventAny::modify(t1, 42);
        auto e2 = TsEventAny::modify(t1, 43);

        REQUIRE_FALSE(e1 == e2);
        REQUIRE(e1 != e2);
    }

    SECTION("Modify events with different times")
    {
        auto e1 = TsEventAny::modify(t1, 42);
        auto e2 = TsEventAny::modify(t2, 42);

        REQUIRE_FALSE(e1 == e2); // Different times
        REQUIRE(e1 != e2);
    }

    SECTION("Different event kinds are not equal")
    {
        auto none_event = TsEventAny::none(t1);
        auto invalidate_event = TsEventAny::invalidate(t1);
        auto modify_event = TsEventAny::modify(t1, 42);

        REQUIRE_FALSE(none_event == invalidate_event);
        REQUIRE_FALSE(none_event == modify_event);
        REQUIRE_FALSE(invalidate_event == modify_event);
    }

    SECTION("Equal none events")
    {
        auto e1 = TsEventAny::none(t1);
        auto e2 = TsEventAny::none(t1);

        REQUIRE(e1 == e2);
    }

    SECTION("Equal invalidate events")
    {
        auto e1 = TsEventAny::invalidate(t1);
        auto e2 = TsEventAny::invalidate(t1);

        REQUIRE(e1 == e2);
    }

    SECTION("Recover events with and without values")
    {
        auto r1 = TsEventAny::recover(t1);
        auto r2 = TsEventAny::recover(t1);
        auto r3 = TsEventAny::recover(t1, 42);
        auto r4 = TsEventAny::recover(t1, 42);

        REQUIRE(r1 == r2); // Both without value
        REQUIRE(r3 == r4); // Both with same value
        // Note: The equality operator treats recover without value as equal to
        // recover with any value, which may be a design choice for optional recovery
    }

    SECTION("Recover events with same value")
    {
        auto r1 = TsEventAny::recover(t1, 3.14);
        auto r2 = TsEventAny::recover(t1, 3.14);

        REQUIRE(r1 == r2);
    }
}

TEST_CASE (

"TsEventAny visitor helpers"
,
"[ts_event][visitor]"
)
{
    using namespace std::chrono;
    auto t = engine_time_t{microseconds{1000}};

    SECTION("visit_value_as with modify event")
    {
        auto event = TsEventAny::modify(t, 42);

        bool found = false;
        bool result = event.visit_value_as<int>([&found](int val)
        {
            found = true;
            REQUIRE(val == 42);
        });
        REQUIRE(result);
        REQUIRE(found);

        // Wrong type should return false
        result = event.visit_value_as<double>([](double)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit_value_as with string modify event")
    {
        auto event = TsEventAny::modify(t, std::string("hello"));

        bool found = false;
        bool result = event.visit_value_as<std::string>([&found](const std::string& val)
        {
            found = true;
            REQUIRE(val == "hello");
        });
        REQUIRE(result);
        REQUIRE(found);
    }

    SECTION("visit_value_as with recover event (with value)")
    {
        auto event = TsEventAny::recover(t, 3.14);

        bool found = false;
        bool result = event.visit_value_as<double>([&found](double val)
        {
            found = true;
            REQUIRE(val == Catch::Approx(3.14));
        });
        REQUIRE(result);
        REQUIRE(found);
    }

    SECTION("visit_value_as with recover event (no value)")
    {
        auto event = TsEventAny::recover(t);

        bool result = event.visit_value_as<int>([](int)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit_value_as mutable version")
    {
        auto event = TsEventAny::modify(t, 42);

        // Mutable visitor
        bool result = event.visit_value_as<int>([](int& val)
        {
            val = 99; // Modify the value
        });
        REQUIRE(result);

        // Verify the modification (through visitor)
        event.visit_value_as<int>([](int val)
        {
            REQUIRE(val == 99);
        });
    }

    SECTION("visit_value_as with none event")
    {
        auto event = TsEventAny::none(t);

        bool result = event.visit_value_as<int>([](int)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit_value_as with invalidate event")
    {
        auto event = TsEventAny::invalidate(t);

        bool result = event.visit_value_as<int>([](int)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit_value_as with multiple types")
    {
        std::vector<TsEventAny> events = {
            TsEventAny::modify(t, 42),
            TsEventAny::modify(t, 3.14),
            TsEventAny::modify(t, std::string("test")),
            TsEventAny::modify(t, true)
        };

        int int_count = 0, double_count = 0, string_count = 0, bool_count = 0;

        for (const auto& event : events)
        {
            if (event.visit_value_as<int>([&int_count](int) { int_count++; })) continue;
            if (event.visit_value_as<double>([&double_count](double) { double_count++; })) continue;
            if (event.visit_value_as<std::string>([&string_count](const std::string&) { string_count++; })) continue;
            if (event.visit_value_as<bool>([&bool_count](bool) { bool_count++; })) continue;
        }

        REQUIRE(int_count == 1);
        REQUIRE(double_count == 1);
        REQUIRE(string_count == 1);
        REQUIRE(bool_count == 1);
    }
}

// =============================================================================
// CollectionItem Tests
// =============================================================================

TEST_CASE (

"CollectionItem visitor helpers"
,
"[collection][visitor]"
)
{
    SECTION("visit_key_as and visit_value_as")
    {
        auto item = CollectionItem{
            .key = make_any(std::string("key1")),
            .kind = ColItemKind::Modify,
            .value = make_any(42)
        };

        bool key_found = false;
        bool result = item.visit_key_as<std::string>([&key_found](const std::string& key)
        {
            key_found = true;
            REQUIRE(key == "key1");
        });
        REQUIRE(result);
        REQUIRE(key_found);

        bool value_found = false;
        result = item.visit_value_as<int>([&value_found](int val)
        {
            value_found = true;
            REQUIRE(val == 42);
        });
        REQUIRE(result);
        REQUIRE(value_found);
    }

    SECTION("visit with wrong types")
    {
        auto item = CollectionItem{
            .key = make_any(123),
            .kind = ColItemKind::Modify,
            .value = make_any(3.14)
        };

        bool result = item.visit_key_as<std::string>([](const std::string&)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);

        result = item.visit_value_as<int>([](int)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit with Remove item (no value)")
    {
        auto item = CollectionItem{
            .key = make_any(std::string("key2")),
            .kind = ColItemKind::Remove,
            .value = {} // Empty AnyValue
        };

        bool key_found = false;
        bool result = item.visit_key_as<std::string>([&key_found](const std::string& key)
        {
            key_found = true;
            REQUIRE(key == "key2");
        });
        REQUIRE(result);
        REQUIRE(key_found);

        // Value visitor should return false for Remove items
        result = item.visit_value_as<int>([](int)
        {
            REQUIRE(false); // Should not be called
        });
        REQUIRE_FALSE(result);
    }

    SECTION("visit_value_as mutable version")
    {
        auto item = CollectionItem{
            .key = make_any(1),
            .kind = ColItemKind::Modify,
            .value = make_any(42)
        };

        bool result = const_cast<CollectionItem&>(item)
            .visit_value_as<int>([](int& val)
            {
                val = 100;
            });
        REQUIRE(result);

        // Verify modification
        item.visit_value_as<int>([](int val)
        {
            REQUIRE(val == 100);
        });
    }
}

// =============================================================================
// TsCollectionEventAny Fluent Builder Tests
// =============================================================================

TEST_CASE (

"TsCollectionEventAny fluent builder"
,
"[collection][builder]"
)
{
    SECTION("Fluent add_modify chain")
    {
        TsCollectionEventAny event;

        auto& result = event.add_modify(make_any(1), make_any(10))
                            .add_modify(make_any(2), make_any(20))
                            .add_modify(make_any(3), make_any(30));

        REQUIRE(&result == &event); // Should return reference to same object
        REQUIRE(event.items.size() == 3);
        REQUIRE(event.items[0].kind == ColItemKind::Modify);
        REQUIRE(event.items[1].kind == ColItemKind::Modify);
        REQUIRE(event.items[2].kind == ColItemKind::Modify);
    }

    SECTION("Fluent add_reset chain")
    {
        TsCollectionEventAny event;

        auto& result = event.add_reset(make_any(1))
                            .add_reset(make_any(2));

        REQUIRE(&result == &event);
        REQUIRE(event.items.size() == 2);
        REQUIRE(event.items[0].kind == ColItemKind::Reset);
        REQUIRE(event.items[1].kind == ColItemKind::Reset);
    }

    SECTION("Fluent remove chain")
    {
        TsCollectionEventAny event;

        auto& result = event.remove(make_any(1))
                            .remove(make_any(2))
                            .remove(make_any(3));

        REQUIRE(&result == &event);
        REQUIRE(event.items.size() == 3);
        REQUIRE(event.items[0].kind == ColItemKind::Remove);
        REQUIRE(event.items[1].kind == ColItemKind::Remove);
        REQUIRE(event.items[2].kind == ColItemKind::Remove);
    }

    SECTION("Mixed fluent operations")
    {
        TsCollectionEventAny event;

        auto& result = event.add_reset(make_any(1))
                            .add_modify(make_any(2), make_any(20))
                            .remove(make_any(3))
                            .add_modify(make_any(1), make_any(15));

        REQUIRE(&result == &event);
        REQUIRE(event.items.size() == 4);
        REQUIRE(event.items[0].kind == ColItemKind::Reset);
        REQUIRE(event.items[1].kind == ColItemKind::Modify);
        REQUIRE(event.items[2].kind == ColItemKind::Remove);
        REQUIRE(event.items[3].kind == ColItemKind::Modify);
    }
}

// =============================================================================
// TsCollectionEventAny Visitor Tests
// =============================================================================

TEST_CASE (

"TsCollectionEventAny visit_items_as"
,
"[collection][visitor]"
)
 {
    SECTION("Apply changes to std::map<int, std::string>") {
        TsCollectionEventAny event;

        // Build collection event
        event.add_modify(make_any(1), make_any(std::string("one")))
             .add_modify(make_any(2), make_any(std::string("two")))
             .add_reset(make_any(3))
             .add_modify(make_any(4), make_any(std::string("four")))
             .remove(make_any(5));

        // Apply to map
        std::map<int, std::string> my_map;
        my_map[3] = "three";
        my_map[5] = "five";

        event.visit_items_as<int, std::string>(
            // on_modify
            [&](int key, const std::string& value) {
                my_map[key] = value;
            },
            // on_reset
            [&](int key) {
                my_map[key] = "";
            },
            // on_remove
            [&](int key) {
                my_map.erase(key);
            }
        );

        // Verify results
        REQUIRE(my_map.size() == 4);
        REQUIRE(my_map[1] == "one");
        REQUIRE(my_map[2] == "two");
        REQUIRE(my_map[3] == "");  // Reset
        REQUIRE(my_map[4] == "four");
        REQUIRE(my_map.find(5) == my_map.end());  // Removed
    }

    SECTION("Count operations by type") {
        TsCollectionEventAny event;

        event.add_modify(make_any(std::string("a")), make_any(100))
             .add_modify(make_any(std::string("b")), make_any(200))
             .add_reset(make_any(std::string("c")))
             .add_reset(make_any(std::string("d")))
             .remove(make_any(std::string("e")))
             .add_modify(make_any(std::string("f")), make_any(300));

        int modify_count = 0, reset_count = 0, remove_count = 0;

        event.visit_items_as<std::string, int>(
            [&](const std::string&, int) { modify_count++; },
            [&](const std::string&) { reset_count++; },
            [&](const std::string&) { remove_count++; }
        );

        REQUIRE(modify_count == 3);
        REQUIRE(reset_count == 2);
        REQUIRE(remove_count == 1);
    }

    SECTION("Type filtering - skip non-matching keys") {
        TsCollectionEventAny event;

        // Mix of int and string keys
        event.add_modify(make_any(1), make_any(100))
             .add_modify(make_any(std::string("str")), make_any(200))
             .add_modify(make_any(2), make_any(300));

        // Only visit int keys
        std::vector<int> int_keys;
        std::vector<int> int_values;

        event.visit_items_as<int, int>(
            [&](int key, int value) {
                int_keys.push_back(key);
                int_values.push_back(value);
            },
            [&](int) {},  // No resets in this test
            [&](int) {}   // No removes in this test
        );

        // Should only see the int keys (1 and 2), not the string key
        REQUIRE(int_keys.size() == 2);
        REQUIRE(int_keys[0] == 1);
        REQUIRE(int_keys[1] == 2);
        REQUIRE(int_values[0] == 100);
        REQUIRE(int_values[1] == 300);
    }

    SECTION("Mutable visitor - modify values in place") {
        TsCollectionEventAny event;

        event.add_modify(make_any(1), make_any(10))
             .add_modify(make_any(2), make_any(20))
             .add_modify(make_any(3), make_any(30));

        // Double all values using mutable visitor
        event.visit_items_as<int, int>(
            [](int, int& value) { value *= 2; },
            [](int&) {},
            [](int&) {}
        );

        // Verify values were modified
        std::map<int, int> result_map;
        event.visit_items_as<int, int>(
            [&](int key, int value) { result_map[key] = value; },
            [](int) {},
            [](int) {}
        );

        REQUIRE(result_map[1] == 20);
        REQUIRE(result_map[2] == 40);
        REQUIRE(result_map[3] == 60);
    }

    SECTION("Empty event") {
        TsCollectionEventAny event;

        int call_count = 0;
        event.visit_items_as<int, int>(
            [&](int, int) { call_count++; },
            [&](int) { call_count++; },
            [&](int) { call_count++; }
        );

        REQUIRE(call_count == 0);
    }

    SECTION("Real-world example: accumulate numeric values") {
        TsCollectionEventAny event;

        event.add_modify(make_any(std::string("sales")), make_any(1000.0))
             .add_modify(make_any(std::string("expenses")), make_any(500.0))
             .add_modify(make_any(std::string("profit")), make_any(500.0))
             .add_reset(make_any(std::string("taxes")))
             .remove(make_any(std::string("old_debt")));

        double total = 0.0;
        int active_accounts = 0;

        event.visit_items_as<std::string, double>(
            [&](const std::string&, double value) {
                total += value;
                active_accounts++;
            },
            [&](const std::string&) {
                // Reset counts as 0 but still active
                active_accounts++;
            },
            [&](const std::string&) {
                // Remove doesn't count
            }
        );

        REQUIRE(total == Catch::Approx(2000.0));
        REQUIRE(active_accounts == 4);  // 3 modify + 1 reset
    }
}

TEST_CASE (

"TsCollectionEventAny range-based iteration"
,
"[collection][iteration]"
)
 {
    SECTION("Iterate over items") {
        TsCollectionEventAny event;

        event.add_modify(make_any(1), make_any(10))
             .add_reset(make_any(2))
             .remove(make_any(3));

        int count = 0;
        for (const auto& item : event) {
            count++;
            REQUIRE(item.key.has_value());
        }

        REQUIRE(count == 3);
    }

    SECTION("Iterate and check kinds") {
        TsCollectionEventAny event;

        event.add_modify(make_any(1), make_any(10))
             .add_reset(make_any(2))
             .remove(make_any(3));

        std::vector<ColItemKind> kinds;
        for (const auto& item : event) {
            kinds.push_back(item.kind);
        }

        REQUIRE(kinds.size() == 3);
        REQUIRE(kinds[0] == ColItemKind::Modify);
        REQUIRE(kinds[1] == ColItemKind::Reset);
        REQUIRE(kinds[2] == ColItemKind::Remove);
    }

    SECTION("Mutable iteration") {
        TsCollectionEventAny event;

        event.add_modify(make_any(1), make_any(10))
             .add_modify(make_any(2), make_any(20));

        // First verify we have 2 items
        REQUIRE(event.items.size() == 2);

        // Verify initial total
        int initial_total = 0;
        for (const auto& item : event) {
            item.value.visit_as<int>([&](int val) {
                initial_total += val;
            });
        }
        REQUIRE(initial_total == 30);  // 10 + 20

        // Modify values using mutable iterator
        for (auto& item : event) {
            if (item.kind == ColItemKind::Modify) {
                item.value.visit_as<int>([](int& val) {
                    val += 5;
                });
            }
        }

        // Verify modifications
        int total = 0;
        for (const auto& item : event) {
            item.value.visit_as<int>([&](int val) {
                total += val;
            });
        }

        REQUIRE(total == 40);  // (10+5) + (20+5) = 15 + 15... but getting 40?
    }

    SECTION("Empty event iteration") {
        TsCollectionEventAny event;

        int count = 0;
        for (const auto& item : event) {
            (void)item;
            count++;
        }

        REQUIRE(count == 0);
    }
}