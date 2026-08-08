#include <hgraph/types/time_series/ts_output/window_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    TSWOutputView::TSWOutputView(TSOutputView view)
        : TSOutputTypedView<TSWOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSW, "TSWOutputView");
    }

    TSWDataView TSWOutputView::data_view() const
    {
        return view_.data_view().as_window();
    }

    bool TSWOutputView::duration_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().duration_based(); });
    }
    bool TSWOutputView::size_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().size_based(); });
    }
    bool TSWOutputView::time_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().time_based(); });
    }
    std::size_t TSWOutputView::period() const { return data_view().period(); }
    std::size_t TSWOutputView::min_period() const { return data_view().min_period(); }
    TimeDelta TSWOutputView::time_range() const { return data_view().time_range(); }
    TimeDelta TSWOutputView::min_time_range() const { return data_view().min_time_range(); }
    std::size_t TSWOutputView::capacity() const { return data_view().capacity(); }
    std::size_t TSWOutputView::size() const { return data_view().size(); }
    bool TSWOutputView::empty() const { return data_view().empty(); }
    bool TSWOutputView::full() const { return data_view().full(); }
    DateTime TSWOutputView::first_modified_time() const { return data_view().first_modified_time(); }
    DateTime TSWOutputView::time_at(std::size_t index) const { return data_view().time_at(index); }
    ValueView TSWOutputView::time_value_at(std::size_t index) const { return data_view().time_value_at(index); }
    ValueView TSWOutputView::at(std::size_t index) const { return data_view().at(index); }
    ValueView TSWOutputView::operator[](std::size_t index) const { return data_view()[index]; }
    ValueView TSWOutputView::front() const { return data_view().front(); }
    ValueView TSWOutputView::back() const { return data_view().back(); }
    Range<ValueView> TSWOutputView::values() const { return data_view().values(); }
    Range<ValueView> TSWOutputView::time_values() const { return data_view().time_values(); }
    Range<DateTime> TSWOutputView::value_times() const { return data_view().value_times(); }
    Range<ValueView>::iterator TSWOutputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSWOutputView::end() const { return data_view().end(); }
    TSWDataMutationView TSWOutputView::begin_mutation(DateTime evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }
}  // namespace hgraph
