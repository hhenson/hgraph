#ifndef HGRAPH_CPP_ROOT_TS_INPUT_WINDOW_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_WINDOW_VIEW_H

#include <hgraph/types/time_series/ts_input/typed_view.h>

namespace hgraph
{
    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    class HGRAPH_EXPORT TSWInputView : public TSInputTypedView<TSWInputView>
    {
      public:
        explicit TSWInputView(TSInputView view);

        [[nodiscard]] TSWDataView data_view() const;
        [[nodiscard]] bool duration_based() const noexcept;
        [[nodiscard]] bool size_based() const noexcept;
        [[nodiscard]] bool time_based() const noexcept;
        [[nodiscard]] std::size_t period() const;
        [[nodiscard]] std::size_t min_period() const;
        [[nodiscard]] TimeDelta time_range() const;
        [[nodiscard]] TimeDelta min_time_range() const;
        [[nodiscard]] std::size_t capacity() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] bool has_removed_value() const;
        [[nodiscard]] ValueView removed_value() const;
        [[nodiscard]] DateTime first_modified_time() const;
        [[nodiscard]] DateTime time_at(std::size_t index) const;
        [[nodiscard]] ValueView time_value_at(std::size_t index) const;
        [[nodiscard]] ValueView at(std::size_t index) const;
        [[nodiscard]] ValueView operator[](std::size_t index) const;
        [[nodiscard]] ValueView front() const;
        [[nodiscard]] ValueView back() const;
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> time_values() const;
        [[nodiscard]] Range<DateTime> value_times() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;

      private:
        TSWInputView(TSInputView view, detail::TrustedTSEndpointKind)
            : TSInputTypedView<TSWInputView>(std::move(view))
        {
        }

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_WINDOW_VIEW_H
