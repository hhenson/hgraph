#include <catch2/catch_test_macros.hpp>

#include <hgraph/util/tagged_ptr.h>

namespace
{
    struct alignas(8) AltA
    {
        int value{0};
    };

    struct alignas(8) AltB
    {
        int value{0};
    };

    struct alignas(8) AltC
    {
        int value{0};
    };

    struct alignas(8) AltD
    {
        int value{0};
    };

    struct alignas(8) AltE
    {
        int value{0};
    };

    struct alignas(8) AltF
    {
        int value{0};
    };
}  // namespace

TEST_CASE("discriminated_ptr stores and clears typed alternatives", "[util][discriminated_ptr]")
{
    using Ptr = hgraph::discriminated_ptr<AltA, AltB, AltC, AltD, AltE, AltF>;

    static_assert(Ptr::alternative_count == 6);
    static_assert(Ptr::tag_bits == 3);
    static_assert(Ptr::alignment == 8);

    AltA a{};
    AltC c{};
    AltF f{};

    Ptr ptr{};
    CHECK_FALSE(ptr);
    CHECK(ptr.empty());
    CHECK(ptr.index() == Ptr::npos);

    ptr = &c;
    CHECK(ptr);
    CHECK_FALSE(ptr.empty());
    CHECK(ptr.is<AltC>());
    CHECK(ptr.index() == 2);
    CHECK(ptr.get<AltA>() == nullptr);
    CHECK(ptr.get<AltC>() == &c);
    CHECK(ptr.raw_bits() != 0);

    ptr.set(&f);
    CHECK(ptr.is<AltF>());
    CHECK(ptr.index() == 5);
    CHECK(ptr.get<AltF>() == &f);

    ptr = &a;
    CHECK(ptr.is<AltA>());
    CHECK(ptr.index() == 0);
    CHECK(ptr.get<AltA>() == &a);

    ptr = nullptr;
    CHECK_FALSE(ptr);
    CHECK(ptr.empty());
    CHECK(ptr.index() == Ptr::npos);
}

TEST_CASE("discriminated_ptr supports narrow parent-style pointer families", "[util][discriminated_ptr]")
{
    using Ptr = hgraph::discriminated_ptr<AltA, AltB>;

    static_assert(Ptr::alternative_count == 2);
    static_assert(Ptr::tag_bits == 1);

    AltB b{};

    Ptr ptr{&b};
    CHECK(ptr);
    CHECK(ptr.is<AltB>());
    CHECK_FALSE(ptr.is<AltA>());
    CHECK(ptr.get<AltB>() == &b);
    CHECK(ptr.index() == 1);
}
