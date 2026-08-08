#include <hgraph/types/time_series/ts_output/set_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    TSSOutputView::TSSOutputView(TSOutputView view)
        : TSOutputTypedView<TSSOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSS, "TSSOutputView");
    }

    TSSDataView TSSOutputView::data_view() const
    {
        return view_.data_view().as_set();
    }

    std::size_t TSSOutputView::size() const { return data_view().size(); }
    bool TSSOutputView::empty() const { return data_view().empty(); }
    std::size_t TSSOutputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSSOutputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSSOutputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSSOutputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSSOutputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    ValueView TSSOutputView::at_slot(std::size_t slot) const { return data_view().at_slot(slot); }
    bool TSSOutputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSSOutputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    Range<ValueView> TSSOutputView::values() const { return data_view().values(); }
    Range<ValueView> TSSOutputView::added() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().added();
    }
    Range<ValueView> TSSOutputView::removed() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().removed();
    }
    Range<ValueView> TSSOutputView::added_values() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().added_values();
    }
    Range<ValueView> TSSOutputView::removed_values() const
    {
        if (!modified()) { return detail::empty_output_range<ValueView>(); }
        return data_view().removed_values();
    }
    Range<ValueView>::iterator TSSOutputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSSOutputView::end() const { return data_view().end(); }
    TSSDataMutationView TSSOutputView::begin_mutation(DateTime evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }

}  // namespace hgraph
