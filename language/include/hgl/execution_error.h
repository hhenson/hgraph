#ifndef HGL_EXECUTION_ERROR_H
#define HGL_EXECUTION_ERROR_H

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace hgl {
    // Runtime identity, independent of the human-readable diagnostic text.
    class ExecutionError : public std::runtime_error {
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
#endif
