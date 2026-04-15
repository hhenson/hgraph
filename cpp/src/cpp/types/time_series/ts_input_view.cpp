#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input.h>
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

            [[nodiscard]] BaseState *state_address(TimeSeriesStateV &state) noexcept
            {
                return std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, state);
            }

            [[nodiscard]] TimeSeriesStateV *owning_state_slot(TSInputView &view, BaseState *state) noexcept
            {
                if (state == nullptr) { return nullptr; }

                TimeSeriesStateV *slot = nullptr;
                hgraph::visit(
                    state->parent,
                    [&](auto *parent_state) {
                        using T = std::remove_pointer_t<decltype(parent_state)>;

                        if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState> ||
                                      std::same_as<T, SignalState>) {
                            if (parent_state != nullptr && state->index < parent_state->child_states.size() &&
                                parent_state->child_states[state->index] != nullptr &&
                                state_address(*parent_state->child_states[state->index]) == state) {
                                slot = parent_state->child_states[state->index].get();
                            }
                        }
                    },
                    [] {});

                if (slot == nullptr) {
                    TSInput *owning_input = view.owning_input();
                    if (owning_input != nullptr) {
                        TimeSeriesStateV &root = owning_input->root_state_variant();
                        if (state_address(root) == state) { slot = &root; }
                    }
                }

                return slot;
            }

            [[nodiscard]] TargetLinkState &ensure_target_link_state(TSInputView &view, BaseState *state)
            {
                if (state == nullptr) {
                    throw std::logic_error("TSInputView dynamic binding requires a live input state");
                }
                if (state->storage_kind == TSStorageKind::TargetLink) { return *static_cast<TargetLinkState *>(state); }
                if (state->storage_kind != TSStorageKind::Native) {
                    throw std::logic_error("TSInputView dynamic binding only supports native input states");
                }

                TimeSeriesStateV *slot = owning_state_slot(view, state);
                if (slot == nullptr) {
                    throw std::logic_error("TSInputView dynamic binding could not resolve the owning state slot");
                }

                const TimeSeriesStateParentPtr parent = state->parent;
                const size_t index = state->index;
                const engine_time_t last_modified_time = state->last_modified_time;
                auto subscribers = std::move(state->subscribers);
                auto feature_registry = std::move(state->feature_registry);

                auto &link_state = slot->emplace<TargetLinkState>();
                link_state.parent = parent;
                link_state.index = index;
                link_state.last_modified_time = last_modified_time;
                link_state.storage_kind = TSStorageKind::TargetLink;
                link_state.subscribers = std::move(subscribers);
                link_state.feature_registry = std::move(feature_registry);
                link_state.target.clear();
                link_state.scheduling_notifier.set_target(nullptr);
                return link_state;
            }

            struct DefaultTSInputViewOps final : TSInputViewOps
            {
                void bind_output(TSInputView &view, const TSOutputView &output) const override
                {
                    TSViewContext &context = view.context_mutable();
                    BaseState *state = context.ts_state;
                    if (state == nullptr || context.schema == nullptr) {
                        throw std::logic_error("TSInputView::bind_output requires a live input view");
                    }

                    TSViewContext parent_context = view.parent_context_ref();
                    const bool parent_is_ref =
                        parent_context.schema != nullptr && parent_context.schema->kind == TSKind::REF &&
                        parent_context.schema->element_ts() != nullptr;
                    const TSMeta *child_schema = context.schema;

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

                    if (parent_is_ref) {
                        hgraph::visit(
                            state->parent,
                            [&](auto *parent_state) {
                                using T = std::remove_pointer_t<decltype(parent_state)>;
                                if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState>) {
                                    if (parent_state != nullptr) { parent_state->suppress_repeated_child_notifications = true; }
                                }
                            },
                            [] {});
                    }

                    TargetLinkState &link_state = ensure_target_link_state(view, state);
                    context.ts_state = &link_state;
                    if (context.notification_state == state || context.notification_state == nullptr) {
                        context.notification_state = &link_state;
                    }
                    const LinkedTSContext previous_target = link_state.target;
                    const bool had_binding = link_state.is_bound();
                    const engine_time_t evaluation_time = view.evaluation_time();
                    link_state.set_target(output_context);
                    link_state.last_modified_time = output_context.ts_state != nullptr ? output_context.ts_state->last_modified_time : MIN_DT;
                    if (!detail::linked_context_equal(previous_target, link_state.target)) {
                        link_state.previous_target_value = detail::snapshot_target_value(previous_target, evaluation_time);
                        link_state.switch_modified_time =
                            link_state.previous_target_value.has_value() ? evaluation_time : MIN_DT;
                    } else {
                        link_state.previous_target_value = {};
                        link_state.switch_modified_time = MIN_DT;
                    }

                    if (evaluation_time != MIN_DT &&
                        !detail::linked_context_equal(previous_target, link_state.target)) {
                        link_state.mark_modified(evaluation_time);
                    }
                }

                void unbind_output(TSInputView &view) const override
                {
                    TSViewContext &context = view.context_mutable();
                    BaseState *state = context.ts_state;
                    if (state == nullptr || state->storage_kind != TSStorageKind::TargetLink) { return; }

                    auto &link_state = *static_cast<TargetLinkState *>(state);
                    if (!link_state.is_bound()) { return; }
                    const LinkedTSContext previous_target = link_state.target;
                    const bool was_valid = view.valid();
                    link_state.reset_target();
                    link_state.previous_target_value = detail::snapshot_target_value(previous_target, view.evaluation_time());
                    link_state.switch_modified_time = link_state.previous_target_value.has_value() ? view.evaluation_time() : MIN_DT;
                    link_state.last_modified_time = link_state.previous_target_value.has_value() ? view.evaluation_time() : MIN_DT;

                    const engine_time_t evaluation_time = view.evaluation_time();
                    if (evaluation_time != MIN_DT && was_valid) { link_state.mark_modified(evaluation_time); }
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

                    if (view.evaluation_time() > MIN_DT && context.ts_state != nullptr && context.schema != nullptr &&
                        context.schema->kind == TSKind::REF && context.ts_state->last_modified_time == MIN_DT &&
                        context.value().has_value()) {
                        // REF inputs can carry a live handle even when the bound target
                        // has not produced data yet. Record the activation-time sample on
                        // the input-local state so node scheduling observes the handle.
                        context.ts_state->last_modified_time = view.evaluation_time();
                    }

                    if (view.evaluation_time() > MIN_DT && view.scheduling_notifier() != nullptr && context.ts_state != nullptr) {
                        const BaseState *resolved = context.ts_state->resolved_state();
                        if (context.ts_state->last_modified_time == MIN_DT &&
                            (resolved == nullptr || resolved->last_modified_time == MIN_DT)) {
                            // Active inputs need one activation-time evaluation even when
                            // neither the local input state nor the upstream target has
                            // ever produced data. That lets operators such as valid() and
                            // modified() emit their initial False result instead of staying
                            // silent until the first real tick arrives.
                            view.scheduling_notifier()->notify(view.evaluation_time());
                            return;
                        }
                        if (context.ts_state->last_modified_time == view.evaluation_time()) {
                            view.scheduling_notifier()->notify(view.evaluation_time());
                            return;
                        }
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

    void TSInputView::unbind_output()
    {
        if (m_input_view_ops == nullptr) {
            throw std::logic_error("TSInputView::unbind_output requires input runtime ops");
        }
        m_input_view_ops->unbind_output(*this);
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
