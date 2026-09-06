#ifndef HGL_DRIVER_NATIVE_MODULE_H
#define HGL_DRIVER_NATIVE_MODULE_H

#include "codegen/cpp_emitter.h"

#include <hgl/native_module_abi.h>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace hgl::driver
{
    /// One loaded generated HGL image and its module-owned lifecycle table.
    /// The image remains resident for process lifetime; this object invokes
    /// logical activation/deactivation transactionally without accepting
    /// hgraph C++ ownership objects across the dynamic boundary.
    class NativeModule
    {
      public:
        NativeModule() noexcept = default;
        NativeModule(std::filesystem::path artifact_directory, std::string cache_key, bool cache_hit,
                     const hgl_native_module_v1 *module_abi) noexcept;
        NativeModule(const NativeModule &)            = delete;
        NativeModule &operator=(const NativeModule &) = delete;
        NativeModule(NativeModule &&other) noexcept;
        NativeModule &operator=(NativeModule &&other);
        ~NativeModule();

        [[nodiscard]] bool active() const noexcept;
        [[nodiscard]] bool activate(std::string &error);
        [[nodiscard]] bool deactivate(std::string &error);

        std::filesystem::path artifact_directory{};
        std::string           cache_key{};
        bool                  cache_hit{false};

      private:
        const hgl_native_module_v1 *module_abi_{nullptr};

        friend std::optional<NativeModule> compile_and_load_native_module(const codegen::EmittedModule &, std::string_view,
                                                                          std::string &);
        friend bool                        compile_and_replace_native_module(const codegen::EmittedModule &, std::string_view,
                                                                             std::optional<NativeModule> &, std::string &);
    };

    /// Compile the emitted C++ as a loadable image, load it into this hgl
    /// process, and invoke its registration entry point. The current compiler
    /// is used with the same definitions and include paths as hgl itself. A
    /// content-addressed cache reuses complete images for equivalent source,
    /// compiler binary, SDK, target, and executable inputs. Caching is skipped
    /// when those executable identities or a per-user cache root are not
    /// available. Failed build artifacts are retained and named in `error`.
    [[nodiscard]] std::optional<NativeModule> compile_and_load_native_module(const codegen::EmittedModule &module,
                                                                             std::string_view source_stem, std::string &error);

    /// Compile and load a candidate image before touching ``active``. At a
    /// quiescent boundary, remove the old provider, activate the candidate,
    /// and restore the old image if candidate initialization fails.
    [[nodiscard]] bool compile_and_replace_native_module(const codegen::EmittedModule &module, std::string_view source_stem,
                                                         std::optional<NativeModule> &active, std::string &error);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_NATIVE_MODULE_H
