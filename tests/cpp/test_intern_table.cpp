#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/utils/intern_table.h>

#include <atomic>
#include <cctype>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

namespace
{
    struct Counter
    {
        static inline std::atomic<int> default_constructed{0};
        static inline std::atomic<int> value_constructed{0};
        static inline std::atomic<int> copy_constructed{0};
        static inline std::atomic<int> move_constructed{0};
        static inline std::atomic<int> destroyed{0};

        int value{0};

        Counter() noexcept { ++default_constructed; }
        explicit Counter(int v) noexcept : value(v) { ++value_constructed; }
        Counter(const Counter &other) noexcept : value(other.value) { ++copy_constructed; }
        Counter(Counter &&other) noexcept : value(other.value) { ++move_constructed; }
        ~Counter() noexcept { ++destroyed; }

        static void reset() noexcept
        {
            default_constructed = 0;
            value_constructed   = 0;
            copy_constructed    = 0;
            move_constructed    = 0;
            destroyed           = 0;
        }
    };

    struct CaseInsensitiveHash
    {
        size_t operator()(const std::string &s) const noexcept
        {
            std::string lower(s);
            for (char &c : lower)
            {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            return std::hash<std::string>{}(lower);
        }
    };

    struct CaseInsensitiveEqual
    {
        bool operator()(const std::string &lhs, const std::string &rhs) const noexcept
        {
            if (lhs.size() != rhs.size()) { return false; }
            for (size_t i = 0; i < lhs.size(); ++i)
            {
                if (std::tolower(static_cast<unsigned char>(lhs[i])) !=
                    std::tolower(static_cast<unsigned char>(rhs[i])))
                {
                    return false;
                }
            }
            return true;
        }
    };
}  // namespace

TEST_CASE("InternTable returns the same reference for the same key")
{
    hgraph::InternTable<int, std::string> table;
    const auto                           &a = table.emplace(7, "seven");
    const auto                           &b = table.emplace(7, "ignored-on-second-call");
    REQUIRE(&a == &b);
    REQUIRE(a == "seven");
}

TEST_CASE("InternTable returns different references for different keys")
{
    hgraph::InternTable<int, std::string> table;
    const auto                           &a = table.emplace(1, "one");
    const auto                           &b = table.emplace(2, "two");
    REQUIRE(&a != &b);
    REQUIRE(a == "one");
    REQUIRE(b == "two");
}

TEST_CASE("InternTable factory only runs on first intern")
{
    hgraph::InternTable<int, int> table;

    int  factory_calls = 0;
    auto factory       = [&]() {
        ++factory_calls;
        return 42;
    };

    const auto &a = table.intern(1, factory);
    const auto &b = table.intern(1, factory);
    const auto &c = table.intern(1, factory);
    REQUIRE(&a == &b);
    REQUIRE(&a == &c);
    REQUIRE(a == 42);
    REQUIRE(factory_calls == 1);
}

TEST_CASE("InternTable::find returns null for missing keys and the value for present keys")
{
    hgraph::InternTable<int, std::string> table;
    REQUIRE(table.find(99) == nullptr);

    const auto &emplaced = table.emplace(7, "seven");

    const auto *found = table.find(7);
    REQUIRE(found != nullptr);
    REQUIRE(&emplaced == found);
    REQUIRE(*found == "seven");
}

TEST_CASE("InternTable::emplace forwards arguments to Value's constructor")
{
    Counter::reset();
    hgraph::InternTable<int, Counter> table;

    const auto &c = table.emplace(1, 99);
    REQUIRE(c.value == 99);
    REQUIRE(Counter::value_constructed.load() == 1);
}

TEST_CASE("InternTable references are stable across many inserts")
{
    hgraph::InternTable<int, int> table;
    std::vector<const int *>      ptrs;
    ptrs.reserve(1000);

    for (int i = 0; i < 1000; ++i) { ptrs.push_back(&table.emplace(i, i)); }

    for (int i = 0; i < 1000; ++i)
    {
        const int *check = &table.intern(i, []() { return -1; });
        REQUIRE(check == ptrs[static_cast<size_t>(i)]);
        REQUIRE(*check == i);
    }
}

TEST_CASE("InternTable concurrent intern of the same key returns one canonical instance")
{
    hgraph::InternTable<int, int> table;
    constexpr int                 thread_count = 16;
    std::vector<std::thread>      threads;
    threads.reserve(thread_count);
    std::vector<const int *> results(static_cast<size_t>(thread_count), nullptr);

    for (int i = 0; i < thread_count; ++i)
    {
        threads.emplace_back([&, i]() {
            results[static_cast<size_t>(i)] = &table.intern(42, []() { return 42; });
        });
    }
    for (auto &t : threads) { t.join(); }

    const int *first = results.front();
    REQUIRE(first != nullptr);
    REQUIRE(*first == 42);
    for (const int *r : results) { REQUIRE(r == first); }
}

TEST_CASE("InternTable concurrent intern of distinct keys stores all values")
{
    hgraph::InternTable<int, int> table;
    constexpr int                 thread_count = 32;
    std::vector<std::thread>      threads;
    threads.reserve(thread_count);

    std::vector<const int *> results(static_cast<size_t>(thread_count), nullptr);

    for (int i = 0; i < thread_count; ++i)
    {
        threads.emplace_back([&, i]() {
            results[static_cast<size_t>(i)] = &table.emplace(i, i * 10);
        });
    }
    for (auto &t : threads) { t.join(); }

    for (int i = 0; i < thread_count; ++i)
    {
        const int *p = table.find(i);
        REQUIRE(p != nullptr);
        REQUIRE(p == results[static_cast<size_t>(i)]);
        REQUIRE(*p == i * 10);
    }
}

TEST_CASE("InternTable supports custom hash and equality functors")
{
    hgraph::InternTable<std::string, int, CaseInsensitiveHash, CaseInsensitiveEqual> table;

    const int &a = table.emplace("Hello", 1);
    const int &b = table.emplace("HELLO", 999);  // structurally equal under custom equality
    REQUIRE(&a == &b);
    REQUIRE(a == 1);
}

TEST_CASE("InternTable destroys interned values when the table goes out of scope")
{
    Counter::reset();
    {
        hgraph::InternTable<int, Counter> table;
        const auto                       &c1 = table.emplace(1, 10);
        const auto                       &c2 = table.emplace(2, 20);
        const auto                       &c3 = table.emplace(3, 30);

        REQUIRE(c1.value == 10);
        REQUIRE(c2.value == 20);
        REQUIRE(c3.value == 30);
        REQUIRE(Counter::value_constructed.load() == 3);
    }
    REQUIRE(Counter::destroyed.load() >= 3);
}
