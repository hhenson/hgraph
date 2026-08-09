#ifndef HGRAPH_RUNTIME_EVALUATION_CLOCK_H
#define HGRAPH_RUNTIME_EVALUATION_CLOCK_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/clock_type_ref.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <string_view>

namespace hgraph
{
    /** Schema descriptor for a type-erased evaluation clock provider. */
    struct HGRAPH_EXPORT EvaluationClockTypeMetaData
    {
        SchemaHeader header{};
        const char *display_name{nullptr};

        [[nodiscard]] std::string_view name() const noexcept
        {
            return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
        }
    };

    /** Type-erased read-only clock operations exposed to user-authored nodes. */
    struct HGRAPH_EXPORT EvaluationClockOps
    {
        const void *context{nullptr};

        DateTime (*evaluation_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*now_impl)(const void *context, const void *memory) noexcept = nullptr;
        TimeDelta (*cycle_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*next_cycle_evaluation_time_impl)(const void *context,
                                                                  const void *memory) noexcept = nullptr;
    };

    namespace detail
    {
        [[nodiscard]] HGRAPH_EXPORT const EvaluationClockTypeMetaData &evaluation_clock_schema() noexcept;
    }  // namespace detail

    /**
     * Borrowed view over the active evaluation clock.
     *
     * The view owns no state. Runtime storage supplies a borrowed typed pointer
     * whose type record carries the operation table; static nodes receive this
     * as a transparent injectable.
     */
    class HGRAPH_EXPORT EvaluationClockView
    {
      public:
        EvaluationClockView() noexcept = default;

        explicit EvaluationClockView(ClockPtr pointer) noexcept
            : pointer_(pointer)
        {
        }

        [[nodiscard]] bool valid() const noexcept { return pointer_.has_value(); }
        [[nodiscard]] ClockTypeRef type() const noexcept { return ClockTypeRef{pointer_.record()}; }
        [[nodiscard]] ClockPtr pointer() const noexcept { return pointer_; }
        [[nodiscard]] const EvaluationClockTypeMetaData *schema() const noexcept { return type().schema(); }

        [[nodiscard]] DateTime evaluation_time() const noexcept
        {
            if (!valid()) return MIN_DT;
            const auto &table = ops();
            return table.evaluation_time_impl(table.context, pointer_.data());
        }

        [[nodiscard]] DateTime now() const noexcept
        {
            if (!valid()) return MIN_DT;
            const auto &table = ops();
            return table.now_impl(table.context, pointer_.data());
        }

        [[nodiscard]] TimeDelta cycle_time() const noexcept
        {
            if (!valid()) return TimeDelta{0};
            const auto &table = ops();
            return table.cycle_time_impl(table.context, pointer_.data());
        }

        [[nodiscard]] DateTime next_cycle_evaluation_time() const noexcept
        {
            if (!valid()) return MIN_DT;
            const auto &table = ops();
            return table.next_cycle_evaluation_time_impl(table.context, pointer_.data());
        }

      private:
        [[nodiscard]] const EvaluationClockOps &ops() const noexcept
        {
            return type().ops_ref();
        }

        ClockPtr pointer_{};
    };

    static_assert(offsetof(EvaluationClockTypeMetaData, header) == 0);

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EVALUATION_CLOCK_H
