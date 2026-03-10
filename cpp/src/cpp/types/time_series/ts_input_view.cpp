#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/value/indexed_view.h>

namespace hgraph
{
    TSInputView::TSInputView(value::ValueView active_state) noexcept : m_active_state(active_state) {}

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
