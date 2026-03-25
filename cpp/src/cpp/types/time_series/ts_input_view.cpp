#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <stdexcept>

namespace hgraph
{
    TSInputView::TSInputView(TSViewContext context,
                             TSViewContext parent,
                             engine_time_t evaluation_time) noexcept :
        TSView<TSInputView>(context, parent, evaluation_time), m_state(context.ts_state)
    {}

    TSInputView::TSInputView(View active_state, BaseState *state, Notifiable *scheduling_notifier) noexcept :
        m_active_state(active_state), m_state(state), m_scheduling_notifier(scheduling_notifier)
    {}

    TSInputCollectionView::TSInputCollectionView(View active_state, BaseState *state,
                                                 Notifiable *scheduling_notifier) noexcept :
        TSInputView(active_state, state, scheduling_notifier)
    {}

    void TSInputView::bind_output(const TSOutputView &output)
    {
        BaseState *state = ts_state();
        const TSViewContext parent_context = this->parent_context();
        if (state == nullptr || parent_context.schema == nullptr) {
            throw std::logic_error("TSInputView::bind_output requires a child view reached through TSL/TSB navigation");
        }

        const size_t slot = state->index;
        const TSMeta *child_schema = nullptr;
        switch (parent_context.schema->kind) {
            case TSKind::TSB:
                if (slot >= parent_context.schema->field_count()) {
                    throw std::out_of_range("TSInputView::bind_output slot is out of range for the parent TSB");
                }
                child_schema = parent_context.schema->fields()[slot].ts_type;
                break;

            case TSKind::TSL:
                child_schema = parent_context.schema->element_ts();
                break;

            default:
                throw std::logic_error("TSInputView::bind_output is only supported for TSL/TSB child views");
        }

        const LinkedTSContext output_context = output.linked_context();
        if (child_schema == nullptr || output_context.schema != child_schema) {
            throw std::invalid_argument("TSInputView::bind_output requires an output matching the selected child schema");
        }

        bool replaced = false;
        hgraph::visit(
            state->parent,
            [&](auto *parent_state) {
                using T = std::remove_pointer_t<decltype(parent_state)>;

                if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState>) {
                    if (parent_state == nullptr) {
                        throw std::logic_error("TSInputView::bind_output requires a live parent collection state");
                    }
                    if (parent_state->child_states.size() <= slot || parent_state->child_states[slot] == nullptr ||
                        !std::holds_alternative<TargetLinkState>(*parent_state->child_states[slot])) {
                        throw std::logic_error("TSInputView::bind_output requires a prebuilt target-link terminal");
                    }

                    auto &link_state = std::get<TargetLinkState>(*parent_state->child_states[slot]);
                    link_state.set_target(output_context);
                    replaced = true;
                }
            },
            [] {});

        if (!replaced) {
            throw std::logic_error("TSInputView::bind_output requires a TSL/TSB-backed parent state");
        }
    }

    void TSInputView::subscribe_scheduling_notifier() noexcept
    {
        if (m_state != nullptr && m_scheduling_notifier != nullptr) { m_state->subscribe(m_scheduling_notifier); }
    }

    void TSInputView::unsubscribe_scheduling_notifier() noexcept
    {
        if (m_state != nullptr && m_scheduling_notifier != nullptr) { m_state->unsubscribe(m_scheduling_notifier); }
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
