//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/view_data.h>

#include <optional>

namespace hgraph
{
    class TSInputView;

    struct HGRAPH_EXPORT TimeSeriesReference
    {
        enum class Kind : uint8_t { EMPTY = 0, BOUND = 1, UNBOUND = 2 };

        // Copy/Move semantics
        TimeSeriesReference(const TimeSeriesReference &other);
        TimeSeriesReference(TimeSeriesReference &&other) noexcept;
        TimeSeriesReference &operator=(const TimeSeriesReference &other);
        TimeSeriesReference &operator=(TimeSeriesReference &&other) noexcept;
        ~TimeSeriesReference();

        // Query methods
        [[nodiscard]] Kind kind() const noexcept { return _kind; }
        [[nodiscard]] bool is_empty() const noexcept { return _kind == Kind::EMPTY; }
        [[nodiscard]] bool is_bound() const noexcept { return _kind == Kind::BOUND; }
        [[nodiscard]] bool is_unbound() const noexcept { return _kind == Kind::UNBOUND; }
        [[nodiscard]] bool has_output() const;
        [[nodiscard]] bool is_valid() const;

        // Accessors (throw if wrong kind)
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference              &operator[](size_t ndx) const;
        [[nodiscard]] const ViewData*                         bound_view() const noexcept;

        // Operations
        void                      bind_input(TSInputView &ts_input) const;
        bool                      operator==(const TimeSeriesReference &other) const;
        [[nodiscard]] std::string to_string() const;

        // Factory methods - use these to construct instances
        static TimeSeriesReference make();
        static TimeSeriesReference make(const ViewData& bound_view);
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);

      private:
        // Private constructors - must use make() factory methods
        TimeSeriesReference() noexcept;                                        // Empty
        explicit TimeSeriesReference(ViewData bound_view);                     // Bound (TS view)
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);  // Unbound

        Kind _kind;

        // Storage for the three variants
        std::optional<ViewData> _bound_view;  // Active when BOUND
        std::vector<TimeSeriesReference> _unbound_items;  // Active when UNBOUND

        // Helper methods for variant management
        void destroy() noexcept;
    };

}  // namespace hgraph

#endif  // REF_H
