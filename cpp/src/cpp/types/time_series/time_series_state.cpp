#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <type_traits>

namespace hgraph
{
    TimeSeriesFeatureRegistry::~TimeSeriesFeatureRegistry() = default;
    void TimeSeriesFeatureRegistry::rebind_link_source(const LinkedTSContext *, engine_time_t) {}

    namespace
    {
        enum class RootNodePort : size_t
        {
            Input = 0,
            Output = 1,
            ErrorOutput = 2,
            RecordableState = 3,
        };

        [[nodiscard]] BaseState *state_address(const std::unique_ptr<TimeSeriesStateV> &state) noexcept
        {
            return state != nullptr
                       ? std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *state)
                       : nullptr;
        }

        [[nodiscard]] bool has_any_child_state(const BaseCollectionState &state) noexcept
        {
            return std::ranges::any_of(state.child_states, [](const auto &child) { return child != nullptr; });
        }

        [[nodiscard]] TimeSeriesStateV *owning_state_variant(BaseState *state) noexcept
        {
            if (state == nullptr) { return nullptr; }

            TimeSeriesStateV *slot = nullptr;
            hgraph::visit(
                state->parent,
                [&](auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState> ||
                                  std::same_as<T, SignalState>) {
                        if (parent != nullptr && state->index < parent->child_states.size() && parent->child_states[state->index] != nullptr &&
                            state_address(parent->child_states[state->index]) == state) {
                            slot = parent->child_states[state->index].get();
                        }
                    }
                },
                [] {});
            return slot;
        }

        [[nodiscard]] TSViewContext view_context_from_target(const LinkedTSContext &target) noexcept
        {
            return TSViewContext{TSContext{
                target.schema,
                target.value_dispatch,
                target.ts_dispatch,
                target.value_data,
                target.ts_state,
                target.owning_output,
                target.output_view_ops,
                target.notification_state,
                target.pending_dict_child,
            }};
        }

        [[nodiscard]] TSOutputView output_view_from_target(const LinkedTSContext &target) noexcept
        {
            return TSOutputView{
                view_context_from_target(target),
                TSViewContext::none(),
                target.notification_state != nullptr
                    ? target.notification_state->last_modified_time
                    : (target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT),
                target.owning_output,
                target.output_view_ops != nullptr ? target.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] TSViewContext context_from_root_state(const TSViewContext &context) noexcept
        {
            BaseState *state = context.ts_state;
            if (state == nullptr) { return context; }
            if (state->parent && state->parent.tag() >= TimeSeriesStateParentPtr::alternative_count) { return context; }
            bool is_root_output_state = false;
            hgraph::visit(state->parent, [&](TSOutput *) { is_root_output_state = true; }, [] {});
            if (state->linked_target() != nullptr && state->index == 0 && is_root_output_state) {
                // Root output links already resolve through their bound target.
                // Re-rooting them through the owning output would reintroduce
                // the local output storage and discard the live target context.
                return context;
            }

            std::vector<size_t> path;
            const Node *root_node = nullptr;
            RootNodePort root_port = RootNodePort::Input;
            const TSInput *root_input = nullptr;
            const TSOutput *root_output = nullptr;
            BaseState *cursor = state;

            while (cursor != nullptr) {
                bool advanced = false;
                hgraph::visit(
                    cursor->parent,
                    [&](TSLState *parent) {
                        path.push_back(cursor->index);
                        cursor = parent;
                        advanced = true;
                    },
                    [&](TSDState *parent) {
                        path.push_back(cursor->index);
                        cursor = parent;
                        advanced = true;
                    },
                    [&](TSBState *parent) {
                        path.push_back(cursor->index);
                        cursor = parent;
                        advanced = true;
                    },
                    [&](SignalState *parent) {
                        path.push_back(cursor->index);
                        cursor = parent;
                        advanced = true;
                    },
                    [&](Node *parent) {
                        root_node = parent;
                        root_port = static_cast<RootNodePort>(cursor->index);
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](TSInput *parent) {
                        root_input = parent;
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](TSOutput *parent) {
                        root_output = parent;
                        cursor = nullptr;
                        advanced = true;
                    },
                    [] {});

                if (!advanced) { break; }
            }

            TSViewContext refreshed = context;
            TSViewContext current;
            if (root_node != nullptr) {
                auto *node = const_cast<Node *>(root_node);
                switch (root_port) {
                    case RootNodePort::Input: current = node->input_view(MIN_DT).context_ref(); break;
                    case RootNodePort::Output: current = node->output_view(MIN_DT).context_ref(); break;
                    case RootNodePort::ErrorOutput: current = node->error_output_view(MIN_DT).context_ref(); break;
                    case RootNodePort::RecordableState: current = node->recordable_state_view(MIN_DT).context_ref(); break;
                }
            } else if (root_output != nullptr) {
                TSOutput *output_root = const_cast<TSOutput *>(root_output);
                current = output_root->view(MIN_DT).context_ref();
            } else if (root_input != nullptr) {
                current = const_cast<TSInput *>(root_input)->view(nullptr, MIN_DT).context_ref();
            } else {
                // Detached native trees, such as output-owned alternatives,
                // are not rooted in the owning output's native state tree.
                // Re-rooting them through owning_output would resolve against
                // the source output instead of the local alternative storage.
                return refreshed;
            }

            std::ranges::reverse(path);
            for (const size_t slot : path) {
                const TSViewContext resolved_parent = current.resolved();
                const auto *collection = resolved_parent.ts_dispatch != nullptr ? resolved_parent.ts_dispatch->as_collection() : nullptr;
                if (collection == nullptr) { return refreshed; }

                current = collection->child_at(current, slot);
                if (!current.is_bound()) { return refreshed; }
            }

            refreshed.schema = current.schema != nullptr ? current.schema : refreshed.schema;
            refreshed.value_dispatch = current.value_dispatch != nullptr ? current.value_dispatch : refreshed.value_dispatch;
            refreshed.ts_dispatch = current.ts_dispatch != nullptr ? current.ts_dispatch : refreshed.ts_dispatch;
            refreshed.value_data = current.value_data;
            refreshed.ts_state = current.ts_state != nullptr ? current.ts_state : refreshed.ts_state;
            refreshed.owning_output = current.owning_output != nullptr ? current.owning_output : refreshed.owning_output;
            refreshed.output_view_ops = current.output_view_ops != nullptr ? current.output_view_ops : refreshed.output_view_ops;
            refreshed.notification_state =
                current.notification_state != nullptr ? current.notification_state : refreshed.notification_state;
            refreshed.pending_dict_child = current.pending_dict_child;
            return refreshed;
        }

        [[nodiscard]] bool reference_value_all_valid(const TimeSeriesReference &ref) noexcept
        {
            switch (ref.kind()) {
                case TimeSeriesReference::Kind::EMPTY:
                    return false;

                case TimeSeriesReference::Kind::PEERED:
                    return ref.target().is_bound() && ref.target_view().all_valid();

                case TimeSeriesReference::Kind::NON_PEERED:
                    return !ref.items().empty() &&
                           std::ranges::all_of(ref.items(), [](const auto &item) { return reference_value_all_valid(item); });
            }

            return false;
        }

        [[nodiscard]] TimeSeriesReference materialize_local_reference(const TSMeta &schema, BaseState *state) noexcept
        {
            if (state == nullptr) { return TimeSeriesReference::make(); }

            if (const LinkedTSContext *target = state->linked_target(); target != nullptr && target->is_bound()) {
                return TimeSeriesReference::make(output_view_from_target(*target));
            }

            switch (schema.kind) {
                case TSKind::TSB:
                    {
                        const auto &bundle_state = *static_cast<TSBState *>(state);
                        std::vector<TimeSeriesReference> items;
                        items.reserve(schema.field_count());
                        for (size_t index = 0; index < schema.field_count(); ++index) {
                            const BaseState *child = index < bundle_state.child_states.size() ? state_address(bundle_state.child_states[index]) : nullptr;
                            const TSMeta *child_schema = schema.fields()[index].ts_type;
                            if (child_schema == nullptr) {
                                items.push_back(TimeSeriesReference::make());
                                continue;
                            }

                            TimeSeriesReference item = materialize_local_reference(*child_schema, const_cast<BaseState *>(child));
                            items.push_back(item.is_valid() ? std::move(item) : TimeSeriesReference::make());
                        }
                        return TimeSeriesReference::make(std::move(items));
                    }

                case TSKind::TSL:
                    {
                        const auto &list_state = *static_cast<TSLState *>(state);
                        std::vector<TimeSeriesReference> items;
                        items.reserve(schema.fixed_size());
                        for (size_t index = 0; index < schema.fixed_size(); ++index) {
                            const BaseState *child = index < list_state.child_states.size() ? state_address(list_state.child_states[index]) : nullptr;
                            const TSMeta *child_schema = schema.element_ts();
                            if (child_schema == nullptr) {
                                items.push_back(TimeSeriesReference::make());
                                continue;
                            }

                            TimeSeriesReference item = materialize_local_reference(*child_schema, const_cast<BaseState *>(child));
                            items.push_back(item.is_valid() ? std::move(item) : TimeSeriesReference::make());
                        }
                        return TimeSeriesReference::make(std::move(items));
                    }

                default:
                    return TimeSeriesReference::make();
            }
        }

        [[nodiscard]] BaseCollectionState *reference_collection_state(const TSMeta &schema, BaseState *state) noexcept
        {
            if (state == nullptr || schema.kind != TSKind::REF || schema.element_ts() == nullptr) { return nullptr; }

            TimeSeriesStateV *slot = owning_state_variant(state);
            if (slot == nullptr) { return nullptr; }

            switch (schema.element_ts()->kind) {
                case TSKind::TSB:
                    return std::holds_alternative<TSBState>(*slot) ? static_cast<TSBState *>(state) : nullptr;

                case TSKind::TSL:
                    return std::holds_alternative<TSLState>(*slot) ? static_cast<TSLState *>(state) : nullptr;

                default: return nullptr;
            }
        }

        template <typename TFn>
        void with_target_state(const LinkedTSContext &target, TFn &&fn) noexcept
        {
            BaseState *state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
            if (state != nullptr) { std::forward<TFn>(fn)(state); }
        }

    } // namespace

    namespace
    {
        [[nodiscard]] LinkedTSContext anchored_dereferenced_context(const LinkedTSContext &source,
                                                                    const LinkedTSContext &target) noexcept
        {
            const bool preserve_source_output_identity =
                source.output_view_ops != nullptr && source.output_view_ops != &detail::default_output_view_ops();
            return LinkedTSContext{
                target.schema,
                target.value_dispatch,
                target.ts_dispatch,
                target.value_data,
                target.ts_state,
                preserve_source_output_identity
                    ? (source.owning_output != nullptr ? source.owning_output : target.owning_output)
                    : (target.owning_output != nullptr ? target.owning_output : source.owning_output),
                preserve_source_output_identity
                    ? source.output_view_ops
                    : (target.output_view_ops != nullptr ? target.output_view_ops : source.output_view_ops),
                target.notification_state != nullptr ? target.notification_state : target.ts_state,
                source.pending_dict_child.active() ? source.pending_dict_child : target.pending_dict_child,
            };
        }

        [[nodiscard]] LinkedTSContext normalized_target_context(const LinkedTSContext &target) noexcept
        {
            if (!target.is_bound()) { return target; }
            LinkedTSContext normalized = output_view_from_target(target).linked_context();
            return normalized.is_bound() ? normalized : target;
        }
    }

    LinkedTSContext detail::dereferenced_target_from_source(const LinkedTSContext &source) noexcept
    {
        if (!source.is_bound() || source.schema == nullptr || source.schema->kind != TSKind::REF) {
            return LinkedTSContext::none();
        }

        if (source.value_dispatch == nullptr || source.value_data == nullptr || source.schema->value_type == nullptr) {
            if (source.ts_state != nullptr) {
                if (const LinkedTSContext *target = source.ts_state->linked_target();
                    target != nullptr && target->is_bound() && (target->schema == nullptr || target->schema->kind != TSKind::REF)) {
                    return anchored_dereferenced_context(source, normalized_target_context(*target));
                }
            }
            return LinkedTSContext::none();
        }

        const View source_value{source.value_dispatch, source.value_data, source.schema->value_type};
        if (!source_value.has_value()) { return LinkedTSContext::none(); }

        const auto *value = source_value.as_atomic().try_as<hgraph::TimeSeriesReference>();
        if (value != nullptr && value->is_peered()) {
            return anchored_dereferenced_context(source, normalized_target_context(value->target()));
        }

        if (source.ts_state != nullptr) {
            if (const LinkedTSContext *target = source.ts_state->linked_target();
                target != nullptr && target->is_bound() && (target->schema == nullptr || target->schema->kind != TSKind::REF)) {
                return anchored_dereferenced_context(source, normalized_target_context(*target));
            }
        }
        return LinkedTSContext::none();
    }

    namespace
    {
        [[nodiscard]] bool linked_context_equal_impl(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept
        {
            return lhs.schema == rhs.schema && lhs.value_dispatch == rhs.value_dispatch && lhs.ts_dispatch == rhs.ts_dispatch &&
                   lhs.value_data == rhs.value_data && lhs.ts_state == rhs.ts_state &&
                   lhs.notification_state == rhs.notification_state &&
                   lhs.owning_output == rhs.owning_output && lhs.output_view_ops == rhs.output_view_ops &&
                   lhs.pending_dict_child.parent_schema == rhs.pending_dict_child.parent_schema &&
                   lhs.pending_dict_child.parent_value_dispatch == rhs.pending_dict_child.parent_value_dispatch &&
                   lhs.pending_dict_child.parent_ts_dispatch == rhs.pending_dict_child.parent_ts_dispatch &&
                   lhs.pending_dict_child.parent_value_data == rhs.pending_dict_child.parent_value_data &&
                   lhs.pending_dict_child.parent_ts_state == rhs.pending_dict_child.parent_ts_state &&
                   lhs.pending_dict_child.parent_owning_output == rhs.pending_dict_child.parent_owning_output &&
                   lhs.pending_dict_child.parent_output_view_ops == rhs.pending_dict_child.parent_output_view_ops &&
                   lhs.pending_dict_child.parent_notification_state == rhs.pending_dict_child.parent_notification_state &&
                   lhs.pending_dict_child.key.equals(rhs.pending_dict_child.key);
        }

        [[nodiscard]] bool retains_previous_target_value(const LinkedTSContext &target) noexcept
        {
            return target.schema != nullptr && (target.schema->kind == TSKind::TSS || target.schema->kind == TSKind::TSD);
        }

        [[nodiscard]] Value snapshot_target_value_impl(const LinkedTSContext &target, engine_time_t modified_time = MIN_DT)
        {
            if (!retains_previous_target_value(target) || !target.is_bound() || target.value_dispatch == nullptr ||
                target.value_data == nullptr) {
                return {};
            }

            View current{target.value_dispatch, target.value_data, target.schema->value_type};
            Value snapshot = current.clone(target.schema->kind == TSKind::TSD ? MutationTracking::Plain : MutationTracking::Delta);

            if (target.schema->kind == TSKind::TSS) {
                auto current_set = current.as_set();
                BaseState *target_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
                if (modified_time == MIN_DT || target_state == nullptr || target_state->last_modified_time != modified_time) {
                    return snapshot;
                }

                const auto current_delta = current_set.delta();
                bool has_delta = false;
                for (const View &item : current_delta.added()) {
                    static_cast<void>(item);
                    has_delta = true;
                    break;
                }
                if (!has_delta) {
                    for (const View &item : current_delta.removed()) {
                        static_cast<void>(item);
                        has_delta = true;
                        break;
                    }
                }
                if (!has_delta) { return snapshot; }

                auto snapshot_set = snapshot.view().as_set();
                auto mutation = snapshot_set.begin_mutation(modified_time != MIN_DT ? modified_time : MIN_ST);
                for (const View &item : current_delta.added()) { static_cast<void>(mutation.remove(item)); }
                for (const View &item : current_delta.removed()) { static_cast<void>(mutation.add(item)); }
                snapshot_set.clear_delta_tracking();
            }

            if (target.schema->kind == TSKind::TSD) {
                auto current_map = current.as_map();
                BaseState *target_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
                if (modified_time == MIN_DT || target_state == nullptr || target_state->last_modified_time != modified_time) {
                    return snapshot;
                }

                const auto current_delta = current_map.delta();
                bool has_delta = false;
                for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                    if (!current_delta.slot_occupied(slot)) { continue; }
                    if (current_delta.slot_added(slot) || current_delta.slot_removed(slot) || current_delta.slot_updated(slot)) {
                        has_delta = true;
                        break;
                    }
                }
                if (!has_delta) { return snapshot; }

                auto snapshot_map = snapshot.view().as_map();
                auto mutation = snapshot_map.begin_mutation(modified_time != MIN_DT ? modified_time : MIN_ST);

                for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                    if (!current_delta.slot_occupied(slot)) { continue; }

                    const View key = current_delta.key_at_slot(slot);
                    if (current_delta.slot_added(slot) && !current_delta.slot_removed(slot)) {
                        static_cast<void>(mutation.remove(key));
                    } else if (current_delta.slot_removed(slot)) {
                        mutation.set(key, current_delta.value_at_slot(slot));
                    }
                }

                snapshot_map.clear_delta_tracking();
            }

            return snapshot;
        }

        [[nodiscard]] Value empty_target_value_impl(const LinkedTSContext &target)
        {
            if (!retains_previous_target_value(target) || !target.is_bound() || target.value_dispatch == nullptr ||
                target.value_data == nullptr) {
                return {};
            }

            View current{target.value_dispatch, target.value_data, target.schema->value_type};
            if (!current.has_value()) { return {}; }

            Value snapshot = current.clone(target.schema->kind == TSKind::TSD ? MutationTracking::Plain : MutationTracking::Delta);

            switch (target.schema->kind) {
                case TSKind::TSS:
                    {
                        const auto current_delta = current.as_set().delta();
                        auto set = snapshot.view().as_set();
                        auto mutation = set.begin_mutation(MIN_ST);
                        for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                            if (!current_delta.slot_occupied(slot) || current_delta.slot_removed(slot)) { continue; }
                            static_cast<void>(mutation.remove(current_delta.at_slot(slot)));
                        }
                        set.clear_delta_tracking();
                        return snapshot;
                    }

                case TSKind::TSD:
                    {
                        const auto current_delta = current.as_map().delta();
                        auto map = snapshot.view().as_map();
                        auto mutation = map.begin_mutation(MIN_ST);
                        for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                            if (!current_delta.slot_occupied(slot) || current_delta.slot_removed(slot)) { continue; }
                            static_cast<void>(mutation.remove(current_delta.key_at_slot(slot)));
                        }
                        map.clear_delta_tracking();
                        return snapshot;
                    }

                default:
                    return {};
            }
        }

        void replay_attachment_subtree(const LinkedTSContext &context,
                                       ActiveTrieNode *trie_node,
                                       Notifiable *notifier,
                                       bool subscribe) noexcept
        {
            if (!context.is_bound() || context.ts_state == nullptr || trie_node == nullptr || notifier == nullptr) { return; }

            if (trie_node->locally_active) {
                if (subscribe) {
                    context.ts_state->subscribe(notifier);
                } else {
                    context.ts_state->unsubscribe(notifier);
                }
            }

            if (trie_node->children.empty()) { return; }

            TSViewContext view_context{context.schema, context.value_dispatch, context.ts_dispatch, context.value_data, context.ts_state};
            const TSViewContext resolved = view_context.resolved();
            const auto *collection = resolved.ts_dispatch != nullptr ? resolved.ts_dispatch->as_collection() : nullptr;
            if (collection == nullptr) { return; }

            for (const auto &[slot, child_trie] : trie_node->children) {
                if (!child_trie) { continue; }

                const TSViewContext child = collection->child_at(view_context, slot);
                if (!child.is_bound()) { continue; }

                replay_attachment_subtree(
                    LinkedTSContext{child.schema, child.value_dispatch, child.ts_dispatch, child.value_data, child.ts_state},
                    child_trie.get(),
                    notifier,
                    subscribe);
            }
        }
    }  // namespace

    bool detail::linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept
    {
        return linked_context_equal_impl(lhs, rhs);
    }

    detail::LinkTransitionSnapshot detail::transition_snapshot(const TSViewContext &context) noexcept
    {
        const BaseState *state = context.notification_state != nullptr ? context.notification_state : context.ts_state;
        size_t steps = 0;
        while (state != nullptr) {
            if (++steps > 256) { return {}; }
            switch (state->storage_kind) {
                case TSStorageKind::OutputLink:
                    {
                        const auto &link = *static_cast<const OutputLinkState *>(state);
                        if (link.switch_modified_time != MIN_DT && link.previous_target_value.has_value()) {
                            return {&link.previous_target_value, link.switch_modified_time};
                        }
                        const LinkedTSContext *target = link.linked_target();
                        state = target != nullptr ? target->ts_state : nullptr;
                        break;
                    }

                case TSStorageKind::TargetLink:
                    {
                        const auto &link = *static_cast<const TargetLinkState *>(state);
                        if (link.switch_modified_time != MIN_DT && link.previous_target_value.has_value()) {
                            return {&link.previous_target_value, link.switch_modified_time};
                        }
                        const LinkedTSContext *target = link.linked_target();
                        state = target != nullptr ? target->ts_state : nullptr;
                        break;
                    }

                case TSStorageKind::RefLink:
                    {
                        state = nullptr;
                        break;
                    }

                case TSStorageKind::Native:
                    state = nullptr;
                    break;
            }
        }

        return {};
    }

    TSViewContext detail::refresh_native_context(const TSViewContext &context) noexcept
    {
        // Only call through custom output_view_ops: the default ops would re-enter
        // context.resolved() → refresh_native_context (has_live_context is false when
        // ts_state is null), producing unbounded recursion with no new information.
        if (context.ts_state == nullptr && context.output_view_ops != nullptr &&
            context.output_view_ops != &detail::default_output_view_ops() && context.owning_output != nullptr) {
            LinkedTSContext current{
                context.schema,
                context.value_dispatch,
                context.ts_dispatch,
                context.value_data,
                context.ts_state,
                context.owning_output,
                context.output_view_ops,
                context.notification_state,
                context.pending_dict_child,
            };

            LinkedTSContext refreshed = output_view_from_target(current).linked_context();
            if (!linked_context_equal_impl(current, refreshed)) { return view_context_from_target(refreshed); }
        }

        return context_from_root_state(context);
    }

    Value detail::snapshot_target_value(const LinkedTSContext &target, engine_time_t modified_time)
    {
        return snapshot_target_value_impl(target, modified_time);
    }

    Value detail::empty_target_value(const LinkedTSContext &target)
    {
        return empty_target_value_impl(target);
    }

    void BaseState::subscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.insert(subscriber); }
    }

    void BaseState::unsubscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.erase(subscriber); }
    }

    LinkedTSContext *BaseState::linked_target() noexcept
    {
        return const_cast<LinkedTSContext *>(const_cast<const BaseState *>(this)->linked_target());
    }

    const LinkedTSContext *BaseState::linked_target() const noexcept
    {
        switch (storage_kind) {
            case TSStorageKind::Native:
                return nullptr;

            case TSStorageKind::OutputLink:
                return &static_cast<const OutputLinkState *>(this)->target;

            case TSStorageKind::TargetLink:
                return &static_cast<const TargetLinkState *>(this)->target;

            case TSStorageKind::RefLink:
                return &static_cast<const RefLinkState *>(this)->bound_link.target;
        }

        return nullptr;
    }

    BaseState *BaseState::resolved_state() noexcept
    {
        return const_cast<BaseState *>(const_cast<const BaseState *>(this)->resolved_state());
    }

    const BaseState *BaseState::resolved_state() const noexcept
    {
        if (const LinkedTSContext *target = linked_target(); target != nullptr) {
            return target->ts_state != nullptr ? target->ts_state->resolved_state() : nullptr;
        }
        return this;
    }

    Notifiable *BaseState::boundary_notifier(Notifiable *fallback) noexcept
    {
        switch (storage_kind) {
            case TSStorageKind::Native:
                return fallback;

            case TSStorageKind::OutputLink:
                return fallback;

            case TSStorageKind::TargetLink:
                return &static_cast<TargetLinkState *>(this)->scheduling_notifier;

            case TSStorageKind::RefLink:
                {
                    auto &attachment = static_cast<RefLinkState *>(this)->attachment_for(fallback);
                    attachment.forwarding_notifier.set_target(fallback);
                    return &attachment.forwarding_notifier;
                }
        }

        return fallback;
    }

    void BaseState::mark_modified(engine_time_t modified_time) noexcept {
        last_modified_time = modified_time;

        for (auto *subscriber : subscribers) { subscriber->notify(modified_time); }

        notify_parent_that_child_is_modified(modified_time);
    }

    void BaseState::mark_modified_local(engine_time_t modified_time) noexcept {
        last_modified_time = modified_time;

        for (auto *subscriber : subscribers) { subscriber->notify(modified_time); }
    }

    void BaseState::notify_parent_that_child_is_modified(engine_time_t modified_time) noexcept
    {
        hgraph::visit(
            parent,
            [this, modified_time](auto *ptr) {
                using T = std::remove_pointer_t<decltype(ptr)>;

                if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState> || std::same_as<T, TSDState> ||
                              std::same_as<T, SignalState>) {
                    ptr->child_modified(index, modified_time);
                }
            },
            [] {});
    }

    bool detail::has_local_reference_binding(const TSViewContext &context) noexcept
    {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->element_ts() == nullptr ||
            context.ts_state == nullptr) {
            return false;
        }

        BaseCollectionState *collection_state = reference_collection_state(*context.schema, context.ts_state);
        return collection_state != nullptr && has_any_child_state(*collection_state);
    }

    bool detail::linked_context_valid(const LinkedTSContext &context) noexcept
    {
        if (context.schema == nullptr || context.value_dispatch == nullptr || context.ts_dispatch == nullptr) { return false; }
        if (context.ts_state == nullptr && context.notification_state == nullptr && context.output_view_ops == nullptr) { return false; }

        const TSViewContext target_context = view_context_from_target(context);
        return target_context.ts_dispatch != nullptr && target_context.ts_dispatch->valid(target_context);
    }

    bool detail::linked_context_all_valid(const LinkedTSContext &context) noexcept
    {
        if (context.schema == nullptr || context.value_dispatch == nullptr || context.ts_dispatch == nullptr) { return false; }
        if (context.ts_state == nullptr && context.notification_state == nullptr && context.output_view_ops == nullptr) { return false; }

        const TSViewContext target_context = view_context_from_target(context);
        return target_context.ts_dispatch != nullptr && target_context.ts_dispatch->all_valid(target_context);
    }

    const Value *detail::materialized_target_link_value(const TSViewContext &context) noexcept
    {
        if (context.ts_state == nullptr || context.ts_state->storage_kind != TSStorageKind::TargetLink) { return nullptr; }

        const auto *state = static_cast<const TargetLinkState *>(context.ts_state);
        return !state->target.is_bound() && state->switch_modified_time != MIN_DT && state->previous_target_value.has_value()
                   ? &state->previous_target_value
                   : nullptr;
    }

    const Value *detail::materialized_reference_value(const TSViewContext &context) noexcept
    {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->element_ts() == nullptr ||
            context.ts_state == nullptr) {
            return nullptr;
        }

        if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr && target->is_bound()) { return nullptr; }

        BaseCollectionState *collection_state = reference_collection_state(*context.schema, context.ts_state);
        if (collection_state == nullptr || !has_any_child_state(*collection_state) || context.schema->value_type == nullptr) {
            return nullptr;
        }

        if (!collection_state->materialized_reference_storage.has_value() ||
            collection_state->materialized_reference_storage->schema() != context.schema->value_type) {
            collection_state->materialized_reference_storage.emplace(*context.schema->value_type);
        } else {
            collection_state->materialized_reference_storage->reset();
        }

        collection_state->materialized_reference_storage->view().as_atomic().set(
            materialize_local_reference(*context.schema->element_ts(), context.ts_state));
        return &*collection_state->materialized_reference_storage;
    }

    bool detail::reference_all_valid(const TSViewContext &context) noexcept
    {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF) { return false; }

        if (context.ts_state != nullptr) {
            if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr) {
                return target->is_bound();
            }
        }

        if (const Value *materialized = materialized_reference_value(context); materialized != nullptr) {
            if (const auto *ref = materialized->view().as_atomic().try_as<TimeSeriesReference>()) {
                return reference_value_all_valid(*ref);
            }
            return false;
        }

        View value = context.value();
        if (value.as_atomic().try_as<TimeSeriesReference>() != nullptr) { return true; }
        return false;
    }

    void BaseCollectionState::child_modified(size_t child_index, engine_time_t modified_time) noexcept {
        if (suppress_repeated_child_notifications) {
            if (last_modified_time != modified_time) {
                modified_children.clear();
                modified_children.insert(child_index);
                mark_modified(modified_time);
            } else {
                modified_children.insert(child_index);
            }
            return;
        }

        if (last_modified_time != modified_time) {
            modified_children.clear();
            modified_children.insert(child_index);
            mark_modified(modified_time);
            return;
        }
        modified_children.insert(child_index);
    }

    void TSDState::child_modified(size_t child_index, engine_time_t modified_time) noexcept
    {
        if (map_dispatch != nullptr && map_value_data != nullptr) {
            map_dispatch->mark_value_updated(map_value_data, child_index, modified_time);
        }
        BaseCollectionState::child_modified(child_index, modified_time);
    }

    BaseCollectionState::~BaseCollectionState() = default;

    BaseCollectionState::BaseCollectionState(BaseCollectionState &&other) noexcept
        : BaseState(std::move(other)),
          child_states(std::move(other.child_states)),
          modified_children(std::move(other.modified_children)),
          materialized_reference_storage(std::move(other.materialized_reference_storage))
    {
        other.child_states.clear();
        other.modified_children.clear();
        other.materialized_reference_storage.reset();
    }

    BaseCollectionState &BaseCollectionState::operator=(BaseCollectionState &&other) noexcept
    {
        if (this == &other) { return *this; }

        reset_child_states();
        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);
        child_states = std::move(other.child_states);
        modified_children = std::move(other.modified_children);
        materialized_reference_storage = std::move(other.materialized_reference_storage);

        other.child_states.clear();
        other.modified_children.clear();
        other.materialized_reference_storage.reset();
        return *this;
    }

    void BaseCollectionState::reset_child_states() noexcept
    {
        child_states.clear();
        materialized_reference_storage.reset();
    }

    TSDState::~TSDState()
    {
        unbind_value_storage();
    }

    void TSSState::mark_added(size_t item_index, engine_time_t modified_time) noexcept {
        reset_change_sets_if_time_changed(modified_time);
        added_items.insert(item_index);
        mark_modified(modified_time);
    }

    void TSSState::mark_removed(size_t item_index, engine_time_t modified_time) noexcept {
        reset_change_sets_if_time_changed(modified_time);
        removed_items.insert(item_index);
        mark_modified(modified_time);
    }

    void TSSState::reset_change_sets_if_time_changed(engine_time_t modified_time) noexcept {
        if (last_modified_time != modified_time) {
            added_items.clear();
            removed_items.clear();
        }
    }

    OutputLinkState::TargetNotifiable::TargetNotifiable(OutputLinkState *self_) noexcept : self(self_) {}

    void OutputLinkState::TargetNotifiable::notify(engine_time_t modified_time)
    {
        if (self != nullptr) {
            if (self->switch_modified_time != modified_time) {
                self->previous_target_value = {};
                self->switch_modified_time = MIN_DT;
            }
            self->mark_modified(modified_time);
        }
    }

    OutputLinkState::OutputLinkState() noexcept : target_notifiable(this) {}

    OutputLinkState::~OutputLinkState()
    {
        reset_target();
    }

    OutputLinkState::OutputLinkState(OutputLinkState &&other) noexcept
        : target_notifiable(this)
    {
        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;
    }

    OutputLinkState &OutputLinkState::operator=(OutputLinkState &&other) noexcept
    {
        if (this == &other) { return *this; }

        reset_target();
        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;

        return *this;
    }

    void OutputLinkState::set_target(LinkedTSContext target_state, engine_time_t modified_time) noexcept
    {
        const bool target_changed = !linked_context_equal_impl(target, target_state);
        Value transition_value =
            target_changed && modified_time != MIN_DT ? snapshot_target_value_impl(target, modified_time) : Value{};

        unregister_from_target();
        target = std::move(target_state);
        last_modified_time = target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT;
        previous_target_value = std::move(transition_value);
        switch_modified_time = previous_target_value.has_value() ? modified_time : MIN_DT;
        register_with_target();
    }

    void OutputLinkState::reset_target(engine_time_t modified_time) noexcept
    {
        previous_target_value = modified_time != MIN_DT ? snapshot_target_value_impl(target, modified_time) : Value{};
        switch_modified_time = previous_target_value.has_value() ? modified_time : MIN_DT;
        unregister_from_target();
        target.clear();
        last_modified_time = switch_modified_time != MIN_DT ? switch_modified_time : MIN_DT;
    }

    void OutputLinkState::register_with_target() noexcept
    {
        with_target_state(target, [this](BaseState *ptr) { ptr->subscribe(&target_notifiable); });
    }

    void OutputLinkState::unregister_from_target() noexcept
    {
        with_target_state(target, [this](BaseState *ptr) { ptr->unsubscribe(&target_notifiable); });
    }

    TargetLinkState::TargetLinkStateNotifiable::TargetLinkStateNotifiable(TargetLinkState *self_) noexcept : self(self_) {}

    void TargetLinkState::TargetLinkStateNotifiable::notify(engine_time_t modified_time) {
        if (self != nullptr) {
            if (self->switch_modified_time != modified_time) {
                self->previous_target_value = {};
                self->switch_modified_time = MIN_DT;
            }
            self->mark_modified(modified_time);
        }
    }

    void TargetLinkState::SchedulingNotifier::notify(engine_time_t modified_time) {
        if (target != nullptr) { target->notify(modified_time); }
    }

    TargetLinkState::TargetLinkState() noexcept : target_notifiable(this) {}

    TargetLinkState::~TargetLinkState()
    {
        reset_target();
    }

    TargetLinkState::TargetLinkState(TargetLinkState &&other) noexcept
        : target_notifiable(this)
    {
        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;
        scheduling_notifier.set_target(other.scheduling_notifier.get_target());

        if (other.target.schema != nullptr || other.target.ts_state != nullptr || other.target.output_view_ops != nullptr) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.scheduling_notifier.set_target(nullptr);
        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;
    }

    TargetLinkState &TargetLinkState::operator=(TargetLinkState &&other) noexcept
    {
        if (this == &other) { return *this; }

        reset_target();

        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;
        scheduling_notifier.set_target(other.scheduling_notifier.get_target());

        if (other.target.schema != nullptr || other.target.ts_state != nullptr || other.target.output_view_ops != nullptr) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.scheduling_notifier.set_target(nullptr);
        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;
        return *this;
    }

    void TargetLinkState::set_target(LinkedTSContext target_state) noexcept {
        unregister_from_target();
        target = std::move(target_state);
        register_with_target();
    }

    void TargetLinkState::reset_target() noexcept {
        unregister_from_target();
        target.clear();
    }

    bool TargetLinkState::is_bound() const noexcept {
        return target.is_bound();
    }
    engine_time_t TargetLinkState::last_target_modified_time() const noexcept {
        BaseState *target_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
        return target_state != nullptr ? target_state->last_modified_time : MIN_DT;
    }

    bool TargetLinkState::is_sampled() const noexcept {
        return last_modified_time > last_target_modified_time();
    }

    void TargetLinkState::register_with_target() noexcept {
        with_target_state(target, [this](BaseState *ptr) { ptr->subscribe(&target_notifiable); });
    }

    void TargetLinkState::unregister_from_target() noexcept {
        with_target_state(target, [this](BaseState *ptr) { ptr->unsubscribe(&target_notifiable); });
    }

    engine_time_t RefLinkState::last_target_modified_time() const { return bound_link.last_modified_time; }

    bool RefLinkState::is_sampled() const { return last_modified_time > last_target_modified_time(); }

    RefLinkState::RefSourceNotifiable::RefSourceNotifiable(RefLinkState *self_) noexcept : self(self_) {}

    void RefLinkState::RefSourceNotifiable::notify(engine_time_t modified_time)
    {
        // A REF source tick means the reference may now point somewhere else.
        // Re-resolve the current target before propagating modification.
        if (self != nullptr) { self->refresh_target(modified_time, true); }
    }

    RefLinkState::DereferencedTargetNotifiable::DereferencedTargetNotifiable(RefLinkState *self_) noexcept : self(self_) {}

    void RefLinkState::DereferencedTargetNotifiable::notify(engine_time_t modified_time)
    {
        if (self != nullptr) {
            // Target-side data changed without the REF itself rebinding. Keep
            // the dereferenced target time in sync and propagate normally.
            if (self->switch_modified_time != modified_time) {
                self->previous_target_value = {};
                self->switch_modified_time = MIN_DT;
            }
            self->bound_link.last_modified_time = modified_time;
            self->mark_modified(modified_time);
        }
    }

    RefLinkState::RefLinkState() noexcept : source_notifiable(this), target_notifiable(this) {}

    RefLinkState::~RefLinkState()
    {
        reset_source();
    }

    RefLinkState::RefLinkState(RefLinkState &&other) noexcept
        : source_notifiable(this), target_notifiable(this)
    {
        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);

        bound_link.parent = other.bound_link.parent;
        bound_link.index = other.bound_link.index;
        bound_link.last_modified_time = other.bound_link.last_modified_time;
        bound_link.storage_kind = other.bound_link.storage_kind;
        bound_link.subscribers = std::move(other.bound_link.subscribers);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
        retain_transition_value = other.retain_transition_value;
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;
        boundary_attachments = std::move(other.boundary_attachments);

        if (other.source.is_bound()) {
            const LinkedTSContext bound_source = other.source;
            other.unregister_from_source();
            source = bound_source;
            register_with_source();
            other.source.clear();
        }

        if (other.bound_link.target.is_bound()) {
            const LinkedTSContext bound_target = other.bound_link.target;
            other.unregister_from_target();
            bound_link.target = bound_target;
            register_with_target();
            other.bound_link.target.clear();
        }

        other.bound_link.scheduling_notifier.set_target(nullptr);
        other.retain_transition_value = true;
        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;
    }

    RefLinkState &RefLinkState::operator=(RefLinkState &&other) noexcept
    {
        if (this == &other) { return *this; }

        reset_source();

        parent = other.parent;
        index = other.index;
        last_modified_time = other.last_modified_time;
        storage_kind = other.storage_kind;
        subscribers = std::move(other.subscribers);
        feature_registry = std::move(other.feature_registry);

        bound_link.parent = other.bound_link.parent;
        bound_link.index = other.bound_link.index;
        bound_link.last_modified_time = other.bound_link.last_modified_time;
        bound_link.storage_kind = other.bound_link.storage_kind;
        bound_link.subscribers = std::move(other.bound_link.subscribers);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
        retain_transition_value = other.retain_transition_value;
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time = other.switch_modified_time;
        boundary_attachments = std::move(other.boundary_attachments);

        if (other.source.is_bound()) {
            const LinkedTSContext bound_source = other.source;
            other.unregister_from_source();
            source = bound_source;
            register_with_source();
            other.source.clear();
        }

        if (other.bound_link.target.is_bound()) {
            const LinkedTSContext bound_target = other.bound_link.target;
            other.unregister_from_target();
            bound_link.target = bound_target;
            register_with_target();
            other.bound_link.target.clear();
        }

        other.bound_link.scheduling_notifier.set_target(nullptr);
        other.retain_transition_value = true;
        other.previous_target_value = {};
        other.switch_modified_time = MIN_DT;
        return *this;
    }

    void RefLinkState::set_source(LinkedTSContext source_state) noexcept
    {
        reset_source();
        source = std::move(source_state);
        register_with_source();
        refresh_target(source.ts_state != nullptr ? source.ts_state->last_modified_time : MIN_DT, false);
    }

    void RefLinkState::reset_source() noexcept
    {
        replay_boundary_attachments(false);
        unregister_from_source();
        unregister_from_target();
        source.clear();
        bound_link.target.clear();
        bound_link.last_modified_time = MIN_DT;
        previous_target_value = {};
        switch_modified_time = MIN_DT;
    }

    void RefLinkState::register_with_source() noexcept
    {
        with_target_state(source, [this](BaseState *ptr) { ptr->subscribe(&source_notifiable); });
    }

    void RefLinkState::unregister_from_source() noexcept
    {
        with_target_state(source, [this](BaseState *ptr) { ptr->unsubscribe(&source_notifiable); });
    }

    void RefLinkState::register_with_target() noexcept
    {
        with_target_state(bound_link.target, [this](BaseState *ptr) { ptr->subscribe(&target_notifiable); });
    }

    void RefLinkState::unregister_from_target() noexcept
    {
        with_target_state(bound_link.target, [this](BaseState *ptr) { ptr->unsubscribe(&target_notifiable); });
    }

    RefLinkState::BoundaryAttachment &RefLinkState::attachment_for(Notifiable *upstream_notifier) noexcept
    {
        auto [it, inserted] = boundary_attachments.try_emplace(upstream_notifier);
        auto &attachment = it->second;
        attachment.forwarding_notifier.set_target(upstream_notifier);
        return attachment;
    }

    BaseState *RefLinkState::current_target_root_state() const noexcept
    {
        return bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->resolved_state() : nullptr;
    }

    void RefLinkState::replay_boundary_attachments(bool subscribe) noexcept
    {
        if (!bound_link.target.is_bound()) { return; }

        for (auto &[upstream_notifier, attachment] : boundary_attachments) {
            attachment.forwarding_notifier.set_target(upstream_notifier);
            replay_attachment_subtree(bound_link.target, attachment.active_trie.root_node(), &attachment.forwarding_notifier, subscribe);
        }
    }

    void RefLinkState::refresh_target(engine_time_t modified_time, bool propagate) noexcept
    {
        const LinkedTSContext previous_target = bound_link.target;
        replay_boundary_attachments(false);
        unregister_from_target();
        bound_link.target = detail::dereferenced_target_from_source(source);
        bound_link.last_modified_time = bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->last_modified_time : MIN_DT;
        register_with_target();
        replay_boundary_attachments(true);

        if (propagate) {
            const bool target_changed = !linked_context_equal_impl(previous_target, bound_link.target);

            if (target_changed) {
                previous_target_value =
                    retain_transition_value ? snapshot_target_value_impl(previous_target, modified_time) : Value{};
                switch_modified_time = previous_target_value.has_value() ? modified_time : MIN_DT;
                mark_modified(modified_time);
            } else {
                // A source-side REF tick that still resolves to the same
                // target is a sampled update. Downstream nodes such as
                // selector-driven REF views must still observe a modification
                // this tick even though there is no transition delta.
                previous_target_value = {};
                switch_modified_time = MIN_DT;
                if (modified_time != MIN_DT) {
                    mark_modified(modified_time);
                } else {
                    last_modified_time = bound_link.last_modified_time;
                }
            }
        } else {
            // Initial binding should inherit the current target time without
            // manufacturing a sampled modification.
            previous_target_value = {};
            switch_modified_time = MIN_DT;
            last_modified_time = bound_link.last_modified_time;
        }
    }

}  // namespace hgraph
