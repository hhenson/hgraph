#ifndef HGRAPH_CPP_ROOT_TS_INPUT_SET_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_SET_VIEW_H

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

    class HGRAPH_EXPORT TSSInputView : public TSInputTypedView<TSSInputView>
    {
      public:
        explicit TSSInputView(TSInputView view);

        [[nodiscard]] TSSDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] ValueView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> added() const;
        [[nodiscard]] Range<ValueView> removed() const;
        [[nodiscard]] Range<ValueView> added_values() const;
        [[nodiscard]] Range<ValueView> removed_values() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;

      private:
        TSSInputView(TSInputView view, detail::TrustedTSEndpointKind)
            : TSInputTypedView<TSSInputView>(std::move(view))
        {
        }

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_SET_VIEW_H
