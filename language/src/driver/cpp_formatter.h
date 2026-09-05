#ifndef HGL_DRIVER_CPP_FORMATTER_H
#define HGL_DRIVER_CPP_FORMATTER_H

#include "codegen/cpp_emitter.h"

#include <string>

namespace hgl::driver
{
    /// Run clang-format over both generated C++ files. Formatting is a
    /// required compiler stage: failure leaves the module untouched.
    [[nodiscard]] bool format_cpp(codegen::EmittedModule &module, std::string &error);
}  // namespace hgl::driver

#endif  // HGL_DRIVER_CPP_FORMATTER_H
