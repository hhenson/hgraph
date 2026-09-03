#ifndef HGL_DRIVER_DRIVER_H
#define HGL_DRIVER_DRIVER_H

#include <span>
#include <string>
#include <string_view>

namespace hgl::driver
{
    /// Run the `hgl` command line (`check`, `test`, `run`, `emit-cpp`, `repl`). Returns
    /// the process exit code (0 ok, 1 diagnostics, 2 usage).
    int run(std::span<const std::string_view> arguments, std::string_view language_version);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_DRIVER_H
