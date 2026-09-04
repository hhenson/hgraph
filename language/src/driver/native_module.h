#ifndef HGL_DRIVER_NATIVE_MODULE_H
#define HGL_DRIVER_NATIVE_MODULE_H

#include "codegen/cpp_emitter.h"

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace hgl::driver
{
    /// Result of compiling and loading one generated HGL module. Successful
    /// images remain resident for the process lifetime because their overload
    /// registrations contain code pointers into the image.
    struct NativeModule
    {
        std::filesystem::path artifact_directory{};
        std::string           cache_key{};
        bool                  cache_hit{false};
    };

    /// Compile the emitted C++ as a loadable image, load it into this hgl
    /// process, and invoke its registration entry point. The current compiler
    /// is used with the same definitions and include paths as hgl itself. A
    /// content-addressed cache reuses complete images for equivalent source,
    /// compiler, SDK, target, and executable inputs. Failed build artifacts
    /// are retained and named in `error`.
    [[nodiscard]] std::optional<NativeModule> compile_and_load_native_module(
        const codegen::EmittedModule &module, std::string_view source_stem, std::string &error);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_NATIVE_MODULE_H
