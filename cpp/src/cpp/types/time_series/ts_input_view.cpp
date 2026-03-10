#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/value/indexed_view.h>

namespace hgraph
{
    TSInputView::TSInputView(value::ValueView active_state, TimeSeriesStatePtr state,
                             Notifiable *scheduling_notifier) noexcept :
        m_active_state(active_state), m_state(state), m_scheduling_notifier(scheduling_notifier)
    {}

    TSInputCollectionView::TSInputCollectionView(value::ValueView active_state, TimeSeriesStatePtr state,
                                                 Notifiable *scheduling_notifier) noexcept :
        TSInputView(active_state, state, scheduling_notifier)
    {}

    void TSInputView::make_active() noexcept
    {
        if (bool *flag = m_active_state.try_as<bool>(); flag != nullptr) { *flag = true; }
    }

    void TSInputView::make_passive() noexcept
    {
        if (bool *flag = m_active_state.try_as<bool>(); flag != nullptr) { *flag = false; }
    }

    bool TSInputView::active() const noexcept
    {
        if (const bool *flag = m_active_state.try_as<bool>(); flag != nullptr) { return *flag; }
        return false;
    }

    void TSInputCollectionView::make_active() noexcept
    {
        if (bool *flag = m_active_state.as_tuple().at(0).try_as<bool>(); flag != nullptr) { *flag = true; }
    }

    void TSInputCollectionView::make_passive() noexcept
    {
        if (bool *flag = m_active_state.as_tuple().at(0).try_as<bool>(); flag != nullptr) { *flag = false; }
    }

    bool TSInputCollectionView::active() const noexcept
    {
        if (const bool *flag = value::View{m_active_state}.as_tuple().at(0).try_as<bool>(); flag != nullptr) { return *flag; }
        return false;
    }
}  // namespace hgraph
