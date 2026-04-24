#include <catch2/catch_test_macros.hpp>

#include <hgraph/util/scope.h>

#include <stdexcept>
#include <utility>

namespace
{
    struct Counter
    {
        int value{0};
    };

    struct PrimaryError
    {};

    struct CleanupError
    {};

    struct FirstError
    {};

    struct SecondError
    {};
}

TEST_CASE("scope exit runs its callback when leaving scope", "[scope]")
{
    Counter counter{};

    {
        auto guard = hgraph::make_scope_exit([&]() noexcept { ++counter.value; });
        static_cast<void>(guard);
        REQUIRE(counter.value == 0);
    }

    REQUIRE(counter.value == 1);
}

TEST_CASE("scope exit release suppresses the callback", "[scope]")
{
    Counter counter{};

    {
        auto guard = hgraph::make_scope_exit([&]() noexcept { ++counter.value; });
        guard.release();
    }

    REQUIRE(counter.value == 0);
}

TEST_CASE("scope exit move transfers cleanup responsibility", "[scope]")
{
    Counter counter{};

    {
        auto original = hgraph::make_scope_exit([&]() noexcept { ++counter.value; });
        {
            auto moved = std::move(original);
            static_cast<void>(moved);
        }

        REQUIRE(counter.value == 1);
    }

    REQUIRE(counter.value == 1);
}

TEST_CASE("unwind cleanup guard does not run cleanup on normal scope exit", "[scope]")
{
    Counter counter{};

    {
        hgraph::UnwindCleanupGuard guard{[&]() noexcept { ++counter.value; }};
        static_cast<void>(guard);
    }

    REQUIRE(counter.value == 0);
}

TEST_CASE("unwind cleanup guard runs cleanup during exception unwinding", "[scope]")
{
    Counter counter{};

    try {
        hgraph::UnwindCleanupGuard guard{[&]() noexcept { ++counter.value; }};
        static_cast<void>(guard);
        throw PrimaryError{};
    } catch (const PrimaryError &) {
    }

    REQUIRE(counter.value == 1);
}

TEST_CASE("unwind cleanup guard suppresses cleanup failures during unwinding", "[scope]")
{
    Counter counter{};

    try {
        hgraph::UnwindCleanupGuard guard{[&]() {
            ++counter.value;
            throw CleanupError{};
        }};
        static_cast<void>(guard);
        throw PrimaryError{};
    } catch (const PrimaryError &) {
    }

    REQUIRE(counter.value == 1);
}

TEST_CASE("unwind cleanup guard complete runs cleanup immediately", "[scope]")
{
    Counter counter{};

    hgraph::UnwindCleanupGuard guard{[&]() noexcept { ++counter.value; }};
    guard.complete();

    REQUIRE(counter.value == 1);
}

TEST_CASE("unwind cleanup guard complete propagates cleanup failures", "[scope]")
{
    hgraph::UnwindCleanupGuard guard{[] { throw CleanupError{}; }};

    REQUIRE_THROWS_AS(guard.complete(), CleanupError);
}

TEST_CASE("unwind cleanup guard release suppresses cleanup", "[scope]")
{
    Counter counter{};

    try {
        hgraph::UnwindCleanupGuard guard{[&]() noexcept { ++counter.value; }};
        guard.release();
        throw PrimaryError{};
    } catch (const PrimaryError &) {
    }

    REQUIRE(counter.value == 0);
}

TEST_CASE("first exception recorder captures the first failure and continues", "[scope]")
{
    hgraph::FirstExceptionRecorder recorder;
    Counter counter{};

    recorder.capture([&] {
        ++counter.value;
        throw FirstError{};
    });
    recorder.capture([&] {
        ++counter.value;
        throw SecondError{};
    });
    recorder.capture([&] { ++counter.value; });

    REQUIRE(counter.value == 3);
    REQUIRE_THROWS_AS(recorder.rethrow_if_any(), FirstError);
}

TEST_CASE("first exception recorder does nothing when no exception was captured", "[scope]")
{
    hgraph::FirstExceptionRecorder recorder;
    Counter counter{};

    recorder.capture([&] { ++counter.value; });
    recorder.capture([&] { ++counter.value; });

    REQUIRE(counter.value == 2);
    REQUIRE_NOTHROW(recorder.rethrow_if_any());
}
