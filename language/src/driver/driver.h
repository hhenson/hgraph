#ifndef HGL_DRIVER_DRIVER_H
#define HGL_DRIVER_DRIVER_H

#include <span>
#include <string>
#include <string_view>

namespace hgl::driver
{
    /// Run the `hgl` command line (`check`, `test`, `run`, `emit-cpp`, `repl`). Returns
    /// the process exit code (0 ok, 1 diagnostics, 2 usage). `tool_version` is what
    /// `--version` and generated-code banners report; `hgl` passes the hgraph
    /// release version (RFC 0032).
    /// Optional provider compiled from the HGL replay/record source. Invoked
    /// only when eval needs those operators; custom hosts may omit it and use
    /// scripted preparation where that facility is supported.
    using EvalLibraryProvider = void (*)();
    int run(std::span<const std::string_view> arguments, std::string_view tool_version,
            EvalLibraryProvider eval_provider = nullptr);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_DRIVER_H
