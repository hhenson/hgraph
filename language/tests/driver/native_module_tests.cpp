#include "driver/cpp_formatter.h"
#include "driver/native_module.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    struct FakeModuleState
    {
        bool active{false};
        int  init_calls{0};
        int  deinit_calls{0};
    };

    void set_error(hgl_native_module_error_v1 *error, std::string_view message) {
        if (error == nullptr || error->struct_size < sizeof(*error)) { return; }
        const std::size_t length = std::min(message.size(), sizeof(error->message) - 1);
        std::copy_n(message.begin(), length, error->message);
        error->message[length] = '\0';
    }

    std::int32_t is_active(const void *context) { return static_cast<const FakeModuleState *>(context)->active ? 1 : 0; }

    std::int32_t init_success(void *context, hgl_native_module_error_v1 *) {
        auto &state = *static_cast<FakeModuleState *>(context);
        ++state.init_calls;
        state.active = true;
        return HGL_NATIVE_MODULE_OK;
    }

    std::int32_t deinit_success(void *context, hgl_native_module_error_v1 *) {
        auto &state = *static_cast<FakeModuleState *>(context);
        ++state.deinit_calls;
        state.active = false;
        return HGL_NATIVE_MODULE_OK;
    }

    std::int32_t init_failure(void *, hgl_native_module_error_v1 *error) {
        set_error(error, "registration rejected");
        return HGL_NATIVE_MODULE_ERROR;
    }

    std::int32_t throw_standard_exception(void *, hgl_native_module_error_v1 *) { throw std::runtime_error{"registration error"}; }

    std::int32_t throw_unknown_exception(void *, hgl_native_module_error_v1 *) { throw 42; }

    hgl_native_module_v1 module_abi(FakeModuleState &state, hgl_native_module_hook_v1 init = init_success,
                                    hgl_native_module_hook_v1 deinit = deinit_success) {
        return {HGL_NATIVE_MODULE_ABI_V1, sizeof(hgl_native_module_v1), "example.module", "", &state, init, deinit, is_active};
    }
}  // namespace

TEST_CASE("native module activation reports standard exceptions through the "
          "scope helper") {
    FakeModuleState           state;
    hgl_native_module_v1      abi = module_abi(state, throw_standard_exception);
    hgl::driver::NativeModule module{"retained", {}, false, &abi};
    std::string               error;

    CHECK_FALSE(module.activate(error));
    CHECK(error == "native module initialization failed: registration error; "
                   "artifacts retained in 'retained'");
    CHECK_FALSE(module.active());
}

TEST_CASE("native module activation reports unknown exceptions through the "
          "scope helper") {
    FakeModuleState           state;
    hgl_native_module_v1      abi = module_abi(state, throw_unknown_exception);
    hgl::driver::NativeModule module{"retained", {}, false, &abi};
    std::string               error;

    CHECK_FALSE(module.activate(error));
    CHECK(error == "native module initialization failed: unknown error; artifacts "
                   "retained in 'retained'");
    CHECK_FALSE(module.active());
}

TEST_CASE("native module activation reports ABI errors") {
    FakeModuleState           state;
    hgl_native_module_v1      abi = module_abi(state, init_failure);
    hgl::driver::NativeModule module{"retained", {}, false, &abi};
    std::string               error;

    CHECK_FALSE(module.activate(error));
    CHECK(error == "native module initialization failed: registration rejected; "
                   "artifacts retained in 'retained'");
    CHECK_FALSE(module.active());
}

TEST_CASE("native module lifecycle is idempotent and module owned") {
    FakeModuleState           state;
    hgl_native_module_v1      abi = module_abi(state);
    hgl::driver::NativeModule module{"retained", {}, false, &abi};
    std::string               error;

    REQUIRE(module.activate(error));
    REQUIRE(module.activate(error));
    CHECK(state.init_calls == 1);
    CHECK(module.active());

    REQUIRE(module.deactivate(error));
    REQUIRE(module.deactivate(error));
    CHECK(state.deinit_calls == 1);
    CHECK_FALSE(module.active());
}

TEST_CASE("generated C++ is passed through clang-format") {
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
    int twice(int x) { return x * 2; }
}  // namespace example
)");
}
