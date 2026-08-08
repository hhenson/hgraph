#include <hgraph/types/time_series/ts_input/set_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSSInputView::TSSInputView(TSInputView view)
        : TSInputTypedView<TSSInputView>(std::move(view))
    {
        detail::validate_input_view_kind(schema(), TSTypeKind::TSS, "TSSInputView");
    }

    TSSDataView TSSInputView::data_view() const
    {
        TSDataView view = view_.input_data_view();
        return view.as_set();
    }

    std::size_t TSSInputView::size() const { return data_view().size(); }
    bool TSSInputView::empty() const { return data_view().empty(); }
    std::size_t TSSInputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSSInputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSSInputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSSInputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSSInputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    ValueView TSSInputView::at_slot(std::size_t slot) const { return data_view().at_slot(slot); }
    bool TSSInputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSSInputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    Range<ValueView> TSSInputView::values() const { return data_view().values(); }
    Range<ValueView> TSSInputView::added() const
    {
        if (!modified()) { return detail::empty_input_range<ValueView>(); }
        if (view_.inherited_sampled_transition()) { return values(); }
        return data_view().added();
    }
    Range<ValueView> TSSInputView::removed() const
    {
        if (!modified()) { return detail::empty_input_range<ValueView>(); }
        if (view_.inherited_sampled_transition()) { return detail::empty_input_range<ValueView>(); }
        return data_view().removed();
    }
    Range<ValueView> TSSInputView::added_values() const
    {
        if (!modified()) { return detail::empty_input_range<ValueView>(); }
        if (view_.inherited_sampled_transition()) { return values(); }
        return data_view().added_values();
    }
    Range<ValueView> TSSInputView::removed_values() const
    {
        if (!modified()) { return detail::empty_input_range<ValueView>(); }
        if (view_.inherited_sampled_transition()) { return detail::empty_input_range<ValueView>(); }
        return data_view().removed_values();
    }
    Range<ValueView>::iterator TSSInputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSSInputView::end() const { return data_view().end(); }

}  // namespace hgraph
