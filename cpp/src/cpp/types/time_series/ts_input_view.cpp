#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <stdexcept>
#include <type_traits>
#include <variant>

namespace hgraph
{
    namespace detail
    {
        namespace
        {
            struct DefaultTSInputViewOps final : TSInputViewOps
            {
                void bind_output(TSViewContext &context,
                                 const TSViewContext &parent_context,
                                 engine_time_t,
                                 const TSOutputView &output) const override
                {
                    BaseState *state = context.ts_state;
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

                void make_active(TSViewContext &context, engine_time_t evaluation_time) const override
                {
                    if (context.active_pos.node != nullptr && context.active_pos.node->locally_active) { return; }

                    if (context.active_pos.node == nullptr && context.active_pos.trie != nullptr) {
                        ensure_trie_path(context.active_pos, context.ts_state);
                    }

                    if (context.active_pos.node == nullptr) { return; }

                    context.active_pos.node->locally_active = true;
                    if (context.ts_state != nullptr && context.scheduling_notifier != nullptr) {
                        context.ts_state->subscribe(context.scheduling_notifier);
                    }

                    if (evaluation_time > MIN_DT && context.scheduling_notifier != nullptr && context.ts_state != nullptr) {
                        const BaseState *resolved = context.ts_state->resolved_state();
                        if (resolved != nullptr && resolved->last_modified_time == evaluation_time) {
                            context.scheduling_notifier->notify(resolved->last_modified_time);
                        }
                    }
                }

                void make_passive(TSViewContext &context, engine_time_t) const override
                {
                    if (context.active_pos.node == nullptr || !context.active_pos.node->locally_active) { return; }

                    context.active_pos.node->locally_active = false;
                    if (context.ts_state != nullptr && context.scheduling_notifier != nullptr) {
                        context.ts_state->unsubscribe(context.scheduling_notifier);
                    }

                    if (!context.active_pos.node->has_any_active()) {
                        if (context.active_pos.trie != nullptr) { context.active_pos.trie->try_prune_root(); }
                        context.active_pos.node = nullptr;
                    }
                }

                [[nodiscard]] bool active(const TSViewContext &context) const noexcept override
                {
                    return context.active_pos.node != nullptr && context.active_pos.node->locally_active;
                }
            };
        }  // namespace

        const TSInputViewOps &default_input_view_ops() noexcept
        {
            static DefaultTSInputViewOps ops;
            return ops;
        }
    }  // namespace detail

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (m_context.input_view_ops == nullptr) {
            throw std::logic_error("TSInputView::bind_output requires input runtime ops");
        }
        m_context.input_view_ops->bind_output(m_context, m_parent, m_evaluation_time, output);
    }

    void TSInputView::make_active()
    {
        if (m_context.input_view_ops != nullptr) { m_context.input_view_ops->make_active(m_context, m_evaluation_time); }
    }

    void TSInputView::make_passive()
    {
        if (m_context.input_view_ops != nullptr) { m_context.input_view_ops->make_passive(m_context, m_evaluation_time); }
    }

    bool TSInputView::active() const
    {
        return m_context.input_view_ops != nullptr && m_context.input_view_ops->active(m_context);
    }
}  // namespace hgraph
