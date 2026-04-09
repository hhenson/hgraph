#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <memory>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace hgraph
{
    namespace detail
    {
        namespace
        {
            [[nodiscard]] RefLinkState *ref_link_boundary(const TSInputView &view) noexcept
            {
                BaseState *state = view.context_ref().ts_state;
                if (state == nullptr) { return nullptr; }

                if (state->storage_kind == TSStorageKind::RefLink) { return static_cast<RefLinkState *>(state); }

                if (state->storage_kind != TSStorageKind::TargetLink) { return nullptr; }
                const LinkedTSContext *target = state->linked_target();
                return target != nullptr && target->ts_state != nullptr && target->ts_state->storage_kind == TSStorageKind::RefLink
                    ? static_cast<RefLinkState *>(target->ts_state)
                    : nullptr;
            }

            [[nodiscard]] Notifiable *child_scheduling_notifier(const TSInputView &parent, BaseState *child_state) noexcept
            {
                Notifiable *parent_notifier = parent.scheduling_notifier();
                if (RefLinkState *ref_link = ref_link_boundary(parent); ref_link != nullptr) {
                    auto &attachment = ref_link->attachment_for(parent_notifier);
                    attachment.forwarding_notifier.set_target(parent_notifier);
                    return &attachment.forwarding_notifier;
                }

                if (child_state == nullptr) { return parent_notifier; }

                Notifiable *child_notifier = child_state->boundary_notifier(parent_notifier);
                if (child_notifier != parent_notifier && parent_notifier != nullptr) {
                    static_cast<TargetLinkState::SchedulingNotifier *>(child_notifier)->set_target(parent_notifier);
                }
                return child_notifier;
            }

            [[nodiscard]] ActiveTriePosition child_active_position(const TSInputView &parent, BaseState *child_state)
            {
                if (RefLinkState *ref_link = ref_link_boundary(parent); ref_link != nullptr) {
                    auto &attachment = ref_link->attachment_for(parent.scheduling_notifier());
                    ActiveTriePosition child_pos;
                    child_pos.trie = &attachment.active_trie;
                    child_pos.boundary_root = ref_link->current_target_root_state();
                    child_pos.node = attachment.active_trie.root_node();

                    if (child_pos.node != nullptr && child_state != nullptr) { child_pos.node = child_pos.node->child_at(child_state->index); }
                    return child_pos;
                }

                ActiveTriePosition child_pos;
                child_pos.trie = parent.active_position().trie;
                child_pos.node = parent.active_position().node != nullptr && child_state != nullptr
                    ? parent.active_position().node->child_at(child_state->index)
                    : nullptr;
                child_pos.boundary_root = parent.active_position().boundary_root;
                child_pos.link_crossings = parent.active_position().link_crossings;

                if (child_state != nullptr && child_state->storage_kind == TSStorageKind::TargetLink) {
                    if (const LinkedTSContext *target = child_state->linked_target();
                        target != nullptr && target->ts_state != nullptr) {
                        child_pos.link_crossings.push_back(LinkCrossing{target->ts_state, child_state});
                    }
                }

                return child_pos;
            }

            void update_dict_navigation_state(const TSInputView &parent, TSInputView &child)
            {
                const TSViewContext &parent_context = parent.context_ref();
                if (parent_context.schema == nullptr || parent_context.schema->kind != TSKind::TSD) { return; }

                auto *state = parent_context.ts_state != nullptr ? static_cast<TSDState *>(parent_context.ts_state->resolved_state()) : nullptr;
                BaseState *child_state = child.context_ref().ts_state;
                if (state == nullptr || child_state == nullptr) { return; }

                const size_t slot = child_state->index;
                MapDeltaView delta = parent.value().as_map().delta();
                if (slot < delta.slot_capacity() && delta.slot_occupied(slot)) {
                    if (ActiveTrieNode *child_node = child.active_position_mutable().node;
                        child_node != nullptr && !child_node->slot_key) {
                        child_node->slot_key = std::make_unique<Value>(delta.key_at_slot(slot));
                    }
                }

                if (ActiveTrieNode *parent_trie = parent.active_position().node; parent_trie != nullptr) {
                    if (parent_trie->has_any_active() && child.scheduling_notifier() != nullptr) {
                        state->active_tries.emplace(child.scheduling_notifier(), parent_trie);
                    }
                } else if (child.scheduling_notifier() != nullptr) {
                    state->active_tries.erase(child.scheduling_notifier());
                }
            }

            struct DefaultTSInputViewOps final : TSInputViewOps
            {
                void bind_output(TSInputView &view, const TSOutputView &output) const override
                {
                    TSViewContext &context = view.context_mutable();
                    TSViewContext parent_context = view.parent_context_ref();
                    static_cast<void>(view.evaluation_time());
                    BaseState *state = context.ts_state;
                    if (state == nullptr || parent_context.schema == nullptr) {
                        throw std::logic_error("TSInputView::bind_output requires a child view reached through TSL/TSB navigation");
                    }

                    const bool parent_is_ref = parent_context.schema->kind == TSKind::REF && parent_context.schema->element_ts() != nullptr;
                    if (parent_is_ref) {
                        parent_context.schema = parent_context.schema->element_ts();
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

                    if (child_schema == nullptr) {
                        throw std::invalid_argument("TSInputView::bind_output requires the selected child schema");
                    }

                    TSOutputView bindable_output = output;
                    LinkedTSContext output_context = bindable_output.linked_context();
                    if (output_context.schema != child_schema) {
                        TSOutput *owning_output = output.owning_output();
                        if (owning_output == nullptr) {
                            throw std::logic_error("TSInputView::bind_output requires an owning output endpoint for casts");
                        }

                        bindable_output = owning_output->bindable_view(output, child_schema);
                        output_context = bindable_output.linked_context();
                    }

                    if (output_context.schema != child_schema) {
                        throw std::invalid_argument("TSInputView::bind_output requires a bindable output matching the selected child schema");
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
                                if (parent_is_ref) { parent_state->suppress_repeated_child_notifications = true; }
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

                void make_active(TSInputView &view) const override
                {
                    TSViewContext &context = view.context_mutable();
                    const TSViewContext &parent_context = view.parent_context_ref();
                    ActiveTriePosition &active_pos = view.active_position_mutable();
                    if (active_pos.node != nullptr && active_pos.node->locally_active) { return; }

                    if (active_pos.node == nullptr && active_pos.trie != nullptr) {
                        ensure_trie_path(active_pos, context.ts_state);
                    }

                    if (active_pos.node == nullptr) { return; }

                    if (parent_context.schema != nullptr && parent_context.schema->kind == TSKind::TSD && context.ts_state != nullptr &&
                        !active_pos.node->slot_key) {
                        const size_t slot = context.ts_state->index;
                        MapDeltaView delta = parent_context.value().as_map().delta();
                        if (slot < delta.slot_capacity() && delta.slot_occupied(slot)) {
                            active_pos.node->slot_key = std::make_unique<Value>(delta.key_at_slot(slot));
                        }
                    }

                    active_pos.node->locally_active = true;
                    if (context.ts_state != nullptr && view.scheduling_notifier() != nullptr) {
                        context.ts_state->subscribe(view.scheduling_notifier());
                    }

                    if (parent_context.schema != nullptr && parent_context.schema->kind == TSKind::TSD &&
                        view.scheduling_notifier() != nullptr) {
                        auto *state = parent_context.ts_state != nullptr
                                          ? static_cast<TSDState *>(parent_context.ts_state->resolved_state())
                                          : nullptr;
                        if (state != nullptr && active_pos.trie != nullptr) {
                            ActiveTriePosition parent_pos;
                            parent_pos.trie = active_pos.trie;
                            parent_pos.boundary_root = active_pos.boundary_root;
                            parent_pos.link_crossings = active_pos.link_crossings;
                            BaseState *parent_state = parent_context.resolved().ts_state;
                            if (ActiveTrieNode *parent_trie = ensure_trie_path(parent_pos, parent_state);
                                parent_trie != nullptr && parent_trie->has_any_active()) {
                                state->active_tries[view.scheduling_notifier()] = parent_trie;
                            }
                        }
                    }

                    if (view.evaluation_time() > MIN_DT && view.scheduling_notifier() != nullptr && context.ts_state != nullptr) {
                        const BaseState *resolved = context.ts_state->resolved_state();
                        if (resolved != nullptr && resolved->last_modified_time == view.evaluation_time()) {
                            view.scheduling_notifier()->notify(resolved->last_modified_time);
                        }
                    }
                }

                void make_passive(TSInputView &view) const override
                {
                    TSViewContext &context = view.context_mutable();
                    const TSViewContext &parent_context = view.parent_context_ref();
                    ActiveTriePosition &active_pos = view.active_position_mutable();
                    if (active_pos.node == nullptr || !active_pos.node->locally_active) { return; }

                    active_pos.node->locally_active = false;
                    if (context.ts_state != nullptr && view.scheduling_notifier() != nullptr) {
                        context.ts_state->unsubscribe(view.scheduling_notifier());
                    }

                    if (parent_context.schema != nullptr && parent_context.schema->kind == TSKind::TSD &&
                        view.scheduling_notifier() != nullptr) {
                        auto *state = parent_context.ts_state != nullptr
                                          ? static_cast<TSDState *>(parent_context.ts_state->resolved_state())
                                          : nullptr;
                        if (state != nullptr && active_pos.trie != nullptr) {
                            ActiveTriePosition parent_pos;
                            parent_pos.trie = active_pos.trie;
                            parent_pos.boundary_root = active_pos.boundary_root;
                            parent_pos.link_crossings = active_pos.link_crossings;
                            BaseState *parent_state = parent_context.resolved().ts_state;
                            if (ActiveTrieNode *parent_trie = ensure_trie_path(parent_pos, parent_state); parent_trie != nullptr) {
                                if (parent_trie->has_any_active()) {
                                    state->active_tries[view.scheduling_notifier()] = parent_trie;
                                } else {
                                    state->active_tries.erase(view.scheduling_notifier());
                                }
                            }
                        }
                    }

                    if (!active_pos.node->has_any_active()) {
                        if (active_pos.trie != nullptr) { active_pos.trie->try_prune_root(); }
                        active_pos.node = nullptr;
                    }
                }

                [[nodiscard]] bool active(const TSInputView &view) const noexcept override
                {
                    return view.active_position().node != nullptr && view.active_position().node->locally_active;
                }
            };
        }  // namespace

        const TSInputViewOps &default_input_view_ops() noexcept
        {
            static DefaultTSInputViewOps ops;
            return ops;
        }
    }  // namespace detail

    TSInputView TSInputView::make_child_view_impl(TSViewContext context,
                                                  TSViewContext parent,
                                                  engine_time_t evaluation_time) const
    {
        TSInputView child{
            std::move(context),
            parent,
            evaluation_time,
            m_owning_input,
            detail::child_active_position(*this, context.ts_state),
            detail::child_scheduling_notifier(*this, context.ts_state),
            m_input_view_ops,
        };
        detail::update_dict_navigation_state(*this, child);
        return child;
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (m_input_view_ops == nullptr) {
            throw std::logic_error("TSInputView::bind_output requires input runtime ops");
        }
        m_input_view_ops->bind_output(*this, output);
    }

    void TSInputView::make_active()
    {
        if (m_input_view_ops != nullptr) { m_input_view_ops->make_active(*this); }
    }

    void TSInputView::make_passive()
    {
        if (m_input_view_ops != nullptr) { m_input_view_ops->make_passive(*this); }
    }

    bool TSInputView::active() const
    {
        return m_input_view_ops != nullptr && m_input_view_ops->active(*this);
    }
}  // namespace hgraph
