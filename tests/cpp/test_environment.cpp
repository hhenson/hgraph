#include <hgraph/util/environment.h>
#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <optional>
#include <string>

TEST_CASE("environment variables are returned as owned snapshots")
{
    constexpr const char *name = "HGRAPH_CPP_ENVIRONMENT_SNAPSHOT_TEST";
    const auto original = hgraph::environment_variable(name);
    auto restore = hgraph::make_scope_exit([&] {
#if defined(_MSC_VER)
        static_cast<void>(::_putenv_s(name, original ? original->c_str() : ""));
#else
        if (original)
        {
            static_cast<void>(::setenv(name, original->c_str(), 1));
        }
        else
        {
            static_cast<void>(::unsetenv(name));
        }
#endif
    });

#if defined(_MSC_VER)
    REQUIRE(::_putenv_s(name, "first") == 0);
#else
    REQUIRE(::setenv(name, "first", 1) == 0);
#endif
    const auto snapshot = hgraph::environment_variable(name);
    REQUIRE(snapshot == std::optional<std::string>{"first"});

#if defined(_MSC_VER)
    REQUIRE(::_putenv_s(name, "second") == 0);
#else
    REQUIRE(::setenv(name, "second", 1) == 0);
#endif
    CHECK(*snapshot == "first");
    CHECK(hgraph::environment_variable(name) ==
          std::optional<std::string>{"second"});
}
