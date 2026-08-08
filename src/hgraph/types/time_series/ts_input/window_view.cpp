#include <hgraph/types/time_series/ts_input/window_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSWInputView::TSWInputView(TSInputView view)
        : TSInputTypedView<TSWInputView>(std::move(view))
    {
        detail::validate_input_view_kind(schema(), TSTypeKind::TSW, "TSWInputView");
    }

    TSWDataView TSWInputView::data_view() const
    {
        return view_.checked_target_data_view("TSWInputView::data_view").as_window();
    }

    bool TSWInputView::duration_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().duration_based(); });
    }

    bool TSWInputView::size_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().size_based(); });
    }

    bool TSWInputView::time_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().time_based(); });
    }

    std::size_t TSWInputView::period() const { return data_view().period(); }
    std::size_t TSWInputView::min_period() const { return data_view().min_period(); }
    TimeDelta TSWInputView::time_range() const { return data_view().time_range(); }
    TimeDelta TSWInputView::min_time_range() const { return data_view().min_time_range(); }
    std::size_t TSWInputView::capacity() const { return data_view().capacity(); }
    std::size_t TSWInputView::size() const { return data_view().size(); }
    bool TSWInputView::empty() const { return data_view().empty(); }
    bool TSWInputView::full() const { return data_view().full(); }
    bool TSWInputView::has_removed_value() const
    {
        return data_view().has_removed_value(view_.evaluation_time());
    }
    ValueView TSWInputView::removed_value() const
    {
        return data_view().removed_value(view_.evaluation_time());
    }
    DateTime TSWInputView::first_modified_time() const { return data_view().first_modified_time(); }
    DateTime TSWInputView::time_at(std::size_t index) const { return data_view().time_at(index); }
    ValueView TSWInputView::time_value_at(std::size_t index) const { return data_view().time_value_at(index); }
    ValueView TSWInputView::at(std::size_t index) const { return data_view().at(index); }
    ValueView TSWInputView::operator[](std::size_t index) const { return data_view()[index]; }
    ValueView TSWInputView::front() const { return data_view().front(); }
    ValueView TSWInputView::back() const { return data_view().back(); }
    Range<ValueView> TSWInputView::values() const { return data_view().values(); }
    Range<ValueView> TSWInputView::time_values() const { return data_view().time_values(); }
    Range<DateTime> TSWInputView::value_times() const { return data_view().value_times(); }
    Range<ValueView>::iterator TSWInputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSWInputView::end() const { return data_view().end(); }
}  // namespace hgraph
