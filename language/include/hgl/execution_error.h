#ifndef HGL_EXECUTION_ERROR_H
#define HGL_EXECUTION_ERROR_H

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

// Scripted kernels are built with hidden visibility. Their exception RTTI
// must coalesce with the host on platforms such as Darwin, even when hgraph
// itself is linked statically. Windows currently uses CMake-built kernels.
#if defined(__GNUC__) && !defined(_WIN32)
#define HGL_EXECUTION_ERROR_VISIBLE __attribute__((visibility("default")))
#else
#define HGL_EXECUTION_ERROR_VISIBLE
#endif

namespace hgl {
    // Runtime identity, independent of the human-readable diagnostic text.
    class HGL_EXECUTION_ERROR_VISIBLE ExecutionError : public std::runtime_error {
      public:
        ExecutionError(std::string code, std::string message)
            : std::runtime_error{std::move(message)}, code_{std::move(code)} {}
        [[nodiscard]] std::string_view code() const noexcept { return code_; }
      private:
        std::string code_;
    };

    [[nodiscard]] inline std::string execution_error_code(const std::exception &error) {
        if (const auto *coded = dynamic_cast<const ExecutionError *>(&error)) { return std::string{coded->code()}; }
        const auto *nested = dynamic_cast<const std::nested_exception *>(&error);
        if (nested != nullptr && nested->nested_ptr()) {
            try { std::rethrow_exception(nested->nested_ptr()); }
            catch (const std::exception &cause) { return execution_error_code(cause); }
            catch (...) {}
        }
        return {};
    }
}
#undef HGL_EXECUTION_ERROR_VISIBLE
#endif
