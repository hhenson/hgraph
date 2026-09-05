#include "driver/cpp_formatter.h"
#include "driver/native_module.h"

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>

namespace
{
    void throw_standard_exception(hgraph::OperatorProviderHandle *)
    {
        throw std::runtime_error{"registration error"};
    }

    void throw_unknown_exception(hgraph::OperatorProviderHandle *)
    {
        throw 42;
    }
}  // namespace

TEST_CASE("native module activation reports standard exceptions through the "
          "scope helper")
{
    hgl::driver::NativeModule module{"retained", {}, false, throw_standard_exception};
    std::string error;

    CHECK_FALSE(module.activate(error));
    CHECK(error == "native module registration failed: registration error; "
                   "artifacts retained in 'retained'");
    CHECK_FALSE(module.active());
}

TEST_CASE("native module activation reports unknown exceptions through the "
          "scope helper")
{
    hgl::driver::NativeModule module{"retained", {}, false, throw_unknown_exception};
    std::string error;

    CHECK_FALSE(module.activate(error));
    CHECK(error == "native module registration failed: unknown error; artifacts "
                   "retained in 'retained'");
    CHECK_FALSE(module.active());
}

TEST_CASE("generated C++ is passed through clang-format")
{
    hgl::codegen::EmittedModule module;
    module.header = "#pragma once\nnamespace example{using value=int;}\n";
    module.source = "#include \"module.h\"\nnamespace example{int twice(int "
                    "x){return x*2;}}\n";
    std::string error;

    const bool formatted = hgl::driver::format_cpp(module, error);
    INFO(error);
    REQUIRE(formatted);
    CHECK(module.header == R"(#pragma once
namespace example
{
    using value = int;
}
)");
    CHECK(module.source == R"(#include "module.h"
namespace example
{
    int twice(int x)
    {
        return x * 2;
    }
}  // namespace example
)");
}
