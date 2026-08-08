#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>

TEST_CASE("fallback_on_exception returns the callable result on success")
{
    const auto result = hgraph::fallback_on_exception(7, [] { return 42; });
    REQUIRE(result == 42);
}

TEST_CASE("fallback_on_exception returns the fallback on failure")
{
    const auto result = hgraph::fallback_on_exception(7, []() -> int {
        throw std::runtime_error("boom");
    });

    REQUIRE(result == 7);
}

TEST_CASE("fallback_on_exception reports std exception messages")
{
    std::string message;
    const auto  result = hgraph::fallback_on_exception(7,
                                                       []() -> int {
                                                           throw std::runtime_error("boom");
                                                       },
                                                       [&](const char *error) {
                                                           message = error;
                                                       });

    REQUIRE(result == 7);
    REQUIRE(message == "boom");
}

TEST_CASE("fallback_on_exception reports unknown exceptions")
{
    std::string message;
    const auto  result = hgraph::fallback_on_exception(7,
                                                       []() -> int {
                                                           throw 1;
                                                       },
                                                       [&](const char *error) {
                                                           message = error;
                                                       });

    REQUIRE(result == 7);
    REQUIRE(message == "unknown error");
}

TEST_CASE("typed annotate_on_exception exposes and replaces the selected exception")
{
    std::string annotated_message;
    REQUIRE_THROWS_AS(
        hgraph::annotate_on_exception<std::out_of_range>(
            []() -> int { throw std::out_of_range("bad index"); },
            [&](const std::out_of_range &error) {
                annotated_message = std::string{"path: "} + error.what();
                throw std::invalid_argument(annotated_message);
            }),
        std::invalid_argument);
    REQUIRE(annotated_message == "path: bad index");
}

TEST_CASE("typed annotate_on_exception leaves other exception types unchanged")
{
    bool annotated = false;
    REQUIRE_THROWS_AS(
        hgraph::annotate_on_exception<std::out_of_range>(
            []() -> int { throw std::runtime_error("boom"); },
            [&](const std::out_of_range &) { annotated = true; }),
        std::runtime_error);
    REQUIRE_FALSE(annotated);
}

TEST_CASE("scope_exit propagates cleanup exceptions by default")
{
    REQUIRE_THROWS_AS(
        [] {
            auto cleanup = hgraph::make_scope_exit([] { throw std::runtime_error("cleanup failed"); });
        }(),
        std::runtime_error);
}

TEST_CASE("scope_exit can suppress cleanup exceptions")
{
    REQUIRE_NOTHROW([] {
        auto cleanup = hgraph::make_scope_exit<true>([] { throw std::runtime_error("cleanup failed"); });
    }());
}
