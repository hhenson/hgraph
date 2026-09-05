#ifndef HGL_DRIVER_PROCESS_H
#define HGL_DRIVER_PROCESS_H

#include <span>
#include <string>

namespace hgl::driver
{
    struct ProcessResult
    {
        int status{-1};
        std::string output{};
    };

    /// Run one process without a command shell and capture its standard output
    /// and standard error together. Arguments are passed exactly as supplied.
    [[nodiscard]] ProcessResult run_process(std::span<const std::string> arguments);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_PROCESS_H
