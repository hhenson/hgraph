#ifndef HGRAPH_RUNTIME_CANONICAL_ERROR_H
#define HGRAPH_RUNTIME_CANONICAL_ERROR_H

#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>
#include <optional>
#include <expected>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>

#if defined(__GNUC__) && !defined(_WIN32)
#define HGRAPH_CANONICAL_ERROR_VISIBLE __attribute__((visibility("default")))
#else
#define HGRAPH_CANONICAL_ERROR_VISIBLE
#endif

namespace hgraph {
    struct HGRAPH_CANONICAL_ERROR_VISIBLE ErrorContext {
        std::string node;
        std::string phase;
        DateTime evaluation_time{};
        bool operator==(const ErrorContext &) const = default;
        friend std::ostream &operator<<(std::ostream &out, const ErrorContext &value) { return out << value.node << " (" << value.phase << ")"; }
    };

    struct HGRAPH_CANONICAL_ERROR_VISIBLE CanonicalError {
        std::optional<std::string> code;
        std::string message;
        std::optional<ErrorContext> context;
        bool operator==(const CanonicalError &other) const { return code == other.code && message == other.message; }
        friend std::ostream &operator<<(std::ostream &out, const CanonicalError &value) { return out << value.message; }
    };

    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<CanonicalError>() noexcept;
    extern template HGRAPH_EXPORT const ValueOps &ops_for<CanonicalError>() noexcept;
    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<ErrorContext>() noexcept;
    extern template HGRAPH_EXPORT const ValueOps &ops_for<ErrorContext>() noexcept;

    template <typename T = void> using NodeResult = std::expected<T, CanonicalError>;

    enum class FailurePhase : std::uint8_t { Evaluate, Start, Stop };

    // The host boundary transports operational failure separately from Result data.
    class HGRAPH_CANONICAL_ERROR_VISIBLE ExecutionFailure : public std::runtime_error {
      public:
        explicit ExecutionFailure(CanonicalError error, FailurePhase phase = FailurePhase::Evaluate) : std::runtime_error{error.message}, error_{std::move(error)}, phase_{phase} {}
        [[nodiscard]] const CanonicalError &error() const noexcept { return error_; }
        [[nodiscard]] FailurePhase active_phase() const noexcept { return phase_; }
      private:
        CanonicalError error_;
        FailurePhase phase_;
    };

    [[nodiscard]] HGRAPH_EXPORT FailurePhase execution_failure_phase(const std::exception &failure) noexcept;
    class NodeView;
    [[nodiscard]] HGRAPH_EXPORT CanonicalError canonicalize_failure(const NodeView &node, DateTime time,
                                                                  std::string phase, const std::exception &failure);
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *canonical_error_value_meta();
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *error_context_value_meta();
    template <> struct scalar_descriptor<CanonicalError> {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta() { return canonical_error_value_meta(); }
    };
    template <> struct scalar_descriptor<ErrorContext> {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta() { return error_context_value_meta(); }
    };

    namespace static_schema_detail {
        template <> struct scalar_name<CanonicalError> { static constexpr std::string_view value{"hgl.error"}; };
        template <> struct scalar_name<ErrorContext> { static constexpr std::string_view value{"hgl.error.context"}; };
    }
}
#undef HGRAPH_CANONICAL_ERROR_VISIBLE
#endif
