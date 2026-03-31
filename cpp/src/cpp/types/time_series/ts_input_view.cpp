#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <stdexcept>

namespace hgraph
{
    TSInputView::TSInputView(TSViewContext context,
                             TSViewContext parent,
                             engine_time_t evaluation_time) :
        TSView<TSInputView>(context, parent, evaluation_time),
        m_active_pos(context.active_pos),
        m_state(context.ts_state),
        m_scheduling_notifier(context.scheduling_notifier)
    {
    }

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
        // Already active?
        if (m_active_pos.node != nullptr && m_active_pos.node->locally_active) { return; }

        // Ensure trie node exists by reconstructing the input-side slot
        // path from the BaseState parent chain (using link_crossings to
        // bridge TargetLinkState boundaries).
        if (m_active_pos.node == nullptr && m_active_pos.trie != nullptr) {
            ensure_trie_path(m_active_pos, m_state);
        }

        if (m_active_pos.node == nullptr) { return; }

        m_active_pos.node->locally_active = true;
        subscribe_scheduling_notifier();

        // NOTE: TSD registration (active_tries on TSDState) is handled by
        // DictTSDispatch::child_at_slot during navigation, not here. That
        // ensures the correct TSD-level trie node is registered regardless
        // of how deep the activation point is.

        // If we're in an active evaluation cycle and the bound state was
        // already modified at or after the current evaluation time, notify
        // the scheduling notifier immediately so the owning node is
        // scheduled for this tick.  This covers the case where
        // resolve_pending resubscribes against a slot that already has
        // valid data.  During wiring (evaluation_time == MIN_DT) no
        // initial notification is needed.
        if (m_evaluation_time > MIN_DT &&
            m_scheduling_notifier != nullptr && m_state != nullptr) {
            const BaseState *resolved = m_state->resolved_state();
            if (resolved != nullptr && resolved->last_modified_time == m_evaluation_time) {
                m_scheduling_notifier->notify(resolved->last_modified_time);
            }
        }
    }

    void TSInputView::make_passive()
    {
        if (m_active_pos.node == nullptr || !m_active_pos.node->locally_active) { return; }

        m_active_pos.node->locally_active = false;
        unsubscribe_scheduling_notifier();

        // If this node has no remaining active descendants, it may be pruned
        // by the trie. Clear our local pointer to avoid dangling references.
        if (!m_active_pos.node->has_any_active()) {
            if (m_active_pos.trie != nullptr) { m_active_pos.trie->try_prune_root(); }
            m_active_pos.node = nullptr;
        }
    }

    bool TSInputView::active() const
    {
        return m_active_pos.node != nullptr && m_active_pos.node->locally_active;
    }

    void TSInputCollectionView::make_active()
    {
        TSInputView::make_active();
    }

    void TSInputCollectionView::make_passive()
    {
        TSInputView::make_passive();
    }

    bool TSInputCollectionView::active() const
    {
        return TSInputView::active();
    }
}  // namespace hgraph
