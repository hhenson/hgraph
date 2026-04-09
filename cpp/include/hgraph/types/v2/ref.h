#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <compare>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace hgraph::v2
{
    struct HGRAPH_EXPORT TimeSeriesReference
    {
        enum class Kind : uint8_t { EMPTY = 0, PEERED = 1, NON_PEERED = 2 };

        TimeSeriesReference() noexcept = default;
        explicit TimeSeriesReference(LinkedTSContext target) noexcept;
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);

        [[nodiscard]] Kind kind() const noexcept { return m_kind; }
        [[nodiscard]] bool is_empty() const noexcept { return m_kind == Kind::EMPTY; }
        [[nodiscard]] bool is_peered() const noexcept { return m_kind == Kind::PEERED; }
        [[nodiscard]] bool is_non_peered() const noexcept { return m_kind == Kind::NON_PEERED; }
        [[nodiscard]] bool is_valid() const;
        [[nodiscard]] engine_time_t observed_time() const noexcept { return m_observed_time; }

        [[nodiscard]] const LinkedTSContext &target() const;
        [[nodiscard]] TSOutputView target_view(engine_time_t evaluation_time = MIN_DT) const;
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference &operator[](size_t ndx) const;

        [[nodiscard]] bool operator==(const TimeSeriesReference &other) const noexcept;
        [[nodiscard]] std::string to_string() const;

        static TimeSeriesReference make();
        static TimeSeriesReference make(const TSOutputView &output);
        static TimeSeriesReference make(const TSInputView &input);
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);

        static const TimeSeriesReference &empty();

      private:
        Kind m_kind{Kind::EMPTY};
        engine_time_t m_observed_time{MIN_DT};
        std::variant<std::monostate, LinkedTSContext, std::vector<TimeSeriesReference>> m_storage;
    };

    [[nodiscard]] HGRAPH_EXPORT size_t atomic_hash(const TimeSeriesReference &value);
    [[nodiscard]] HGRAPH_EXPORT TimeSeriesReference atomic_default_value(std::type_identity<TimeSeriesReference>);
    [[nodiscard]] HGRAPH_EXPORT std::partial_ordering atomic_compare(const TimeSeriesReference &lhs,
                                                                     const TimeSeriesReference &rhs);
    [[nodiscard]] HGRAPH_EXPORT std::string to_string(const TimeSeriesReference &value);
}  // namespace hgraph::v2
