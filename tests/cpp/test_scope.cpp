#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

#include <exception>
#include <optional>
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

namespace
{
    struct CodedError : std::runtime_error
    {
        explicit CodedError(int code_) : std::runtime_error("coded"), code(code_) {}
        int code;
    };

    /** A nested_exception made outside any handler: it holds nothing. */
    struct HollowNestedError : std::runtime_error, std::nested_exception
    {
        HollowNestedError() : std::runtime_error("hollow") {}
    };

    [[nodiscard]] std::optional<int> code_beneath(const std::exception &failure)
    {
        std::optional<int> code;
        const bool         found = hgraph::find_nested_exception<CodedError>(
            failure, [&](const CodedError &coded) noexcept { code = coded.code; });
        REQUIRE(found == code.has_value());
        return code;
    }
}  // namespace

TEST_CASE("find_nested_exception finds the failure itself and one nested beneath it")
{
    CHECK(code_beneath(CodedError{3}) == 3);

    std::optional<int> code;
    try
    {
        try
        {
            try
            {
                throw CodedError{5};
            }
            catch (const std::exception &)
            {
                std::throw_with_nested(std::runtime_error("canonical"));
            }
        }
        catch (const std::exception &)
        {
            std::throw_with_nested(std::runtime_error("annotated"));
        }
    }
    catch (const std::exception &annotated)
    {
        code = code_beneath(annotated);
    }
    CHECK(code == 5);
}

TEST_CASE("find_nested_exception ends without a match where there is nothing to follow")
{
    CHECK_FALSE(code_beneath(std::runtime_error("plain")).has_value());

    // std::rethrow_if_nested would terminate here.
    REQUIRE(HollowNestedError{}.nested_ptr() == nullptr);
    CHECK_FALSE(code_beneath(HollowNestedError{}).has_value());

    bool reached = false;
    try
    {
        try
        {
            throw 42;
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("over a failure that is no std::exception"));
        }
    }
    catch (const std::exception &outer)
    {
        reached = true;
        CHECK_FALSE(code_beneath(outer).has_value());
    }
    CHECK(reached);
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
