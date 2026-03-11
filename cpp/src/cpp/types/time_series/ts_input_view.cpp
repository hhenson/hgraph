#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>

namespace hgraph
{
    TSInputView::TSInputView(View active_state, TimeSeriesStatePtr state,
                             Notifiable &scheduling_notifier) noexcept :
        m_active_state(active_state), m_state(state), m_scheduling_notifier(scheduling_notifier)
    {}

    TSInputCollectionView::TSInputCollectionView(View active_state, TimeSeriesStatePtr state,
                                                 Notifiable &scheduling_notifier) noexcept :
        TSInputView(active_state, state, scheduling_notifier)
    {}

    void TSInputView::subscribe_scheduling_notifier() noexcept
    {
        std::visit(
            [this](auto *ptr) {
                if (ptr != nullptr) { ptr->subscribe(&m_scheduling_notifier); }
            },
            m_state);
    }

    void TSInputView::unsubscribe_scheduling_notifier() noexcept
    {
        std::visit(
            [this](auto *ptr) {
                if (ptr != nullptr) { ptr->unsubscribe(&m_scheduling_notifier); }
            },
            m_state);
    }

    void TSInputView::make_active()
    {
        if (bool *flag = m_active_state.as_atomic().try_as<bool>(); flag != nullptr) {
            if (!*flag) {
                *flag = true;
                subscribe_scheduling_notifier();
            }
        }
    }

    void TSInputView::make_passive()
    {
        if (bool *flag = m_active_state.as_atomic().try_as<bool>(); flag != nullptr) {
            if (*flag) {
                *flag = false;
                unsubscribe_scheduling_notifier();
            }
        }
    }

    bool TSInputView::active() const
    {
        if (const bool *flag = m_active_state.as_atomic().try_as<bool>(); flag != nullptr) { return *flag; }
        return false;
    }

    void TSInputCollectionView::make_active()
    {
        // TODO: Restore collection-head activation once the new time-series
        // value view exposes tuple navigation and mutable child access.
        //
        // Intended logic:
        // if (bool *flag = m_active_state.as_tuple().at(0).try_as<bool>(); flag != nullptr) {
        //     if (!*flag) {
        //         *flag = true;
        //         subscribe_scheduling_notifier();
        //     }
        // }
        throw std::logic_error("TSInputCollectionView::make_active requires tuple navigation on the new View");
    }

    void TSInputCollectionView::make_passive()
    {
        // TODO: Restore collection-head activation once the new time-series
        // value view exposes tuple navigation and mutable child access.
        //
        // Intended logic:
        // if (bool *flag = m_active_state.as_tuple().at(0).try_as<bool>(); flag != nullptr) {
        //     if (*flag) {
        //         *flag = false;
        //         unsubscribe_scheduling_notifier();
        //     }
        // }
        throw std::logic_error("TSInputCollectionView::make_passive requires tuple navigation on the new View");
    }

    bool TSInputCollectionView::active() const
    {
        // TODO: Restore collection-head activation once the new time-series
        // value view exposes tuple navigation for const views.
        //
        // Intended logic:
        // if (const bool *flag = m_active_state.as_tuple().at(0).try_as<bool>(); flag != nullptr) {
        //     return *flag;
        // }
        throw std::logic_error("TSInputCollectionView::active requires tuple navigation on the new View");
    }
}  // namespace hgraph
