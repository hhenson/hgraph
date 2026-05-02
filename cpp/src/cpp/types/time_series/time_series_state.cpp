#include <hgraph/hgraph_base.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <type_traits>

namespace hgraph
{
    namespace
    {
        void rebind_linked_context_state(LinkedTSContext &context, BaseState *old_state, BaseState *new_state) noexcept {
            if (old_state == nullptr || new_state == nullptr || old_state == new_state) { return; }
            if (context.ts_state == old_state) { context.ts_state = new_state; }
            if (context.notification_state == old_state) { context.notification_state = new_state; }
            if (context.pending_dict_child.parent_ts_state == old_state) { context.pending_dict_child.parent_ts_state = new_state; }
            if (context.pending_dict_child.parent_notification_state == old_state) {
                context.pending_dict_child.parent_notification_state = new_state;
            }
        }

        void rebind_target_link_invalidator(TargetLinkInvalidator &invalidator, BaseState *old_state,
                                            BaseState *new_state) noexcept {
            invalidator.target_destroyed = false;
            if (TargetLinkState *owner = invalidator.owner; owner != nullptr) {
                rebind_linked_context_state(owner->target, old_state, new_state);
            }
        }

        void invalidate_lifetime_registrations(BaseState &state) noexcept {
            auto ref_snapshot = std::move(state.ref_invalidators);
            for (ReferenceInvalidator *invalidator : ref_snapshot) {
                if (invalidator != nullptr) { invalidate_ref(*invalidator); }
            }

            auto target_link_snapshot = std::move(state.target_link_invalidators);
            for (TargetLinkInvalidator *invalidator : target_link_snapshot) {
                if (invalidator != nullptr) { invalidate_target_link(*invalidator); }
            }

            auto state_snapshot = std::move(state.state_invalidators);
            for (StateInvalidator *invalidator : state_snapshot) {
                if (invalidator != nullptr) { invalidate_state(*invalidator); }
            }
        }

        void transfer_ref_invalidators(BaseState &target, BaseState &source) noexcept {
            target.ref_invalidators = std::move(source.ref_invalidators);
            for (ReferenceInvalidator *invalidator : target.ref_invalidators) {
                if (invalidator != nullptr) { rebind_ref_target(*invalidator, &source, &target); }
            }
            source.ref_invalidators.clear();
        }

        void transfer_target_link_invalidators(BaseState &target, BaseState &source) noexcept {
            target.target_link_invalidators = std::move(source.target_link_invalidators);
            for (TargetLinkInvalidator *invalidator : target.target_link_invalidators) {
                if (invalidator != nullptr) { rebind_target_link_invalidator(*invalidator, &source, &target); }
            }
            source.target_link_invalidators.clear();
        }

        void transfer_state_invalidators(BaseState &target, BaseState &source) noexcept {
            for (StateInvalidator *invalidator : source.state_invalidators) {
                if (invalidator == nullptr) { continue; }
                if (invalidator->rebind != nullptr) {
                    invalidator->state_destroyed = false;
                    invalidator->rebind(invalidator->owner, &source, &target);
                    target.state_invalidators.insert(invalidator);
                } else {
                    invalidate_state(*invalidator);
                }
            }
            source.state_invalidators.clear();
        }

        void move_base_state_fields(BaseState &target, BaseState &source) noexcept {
            if (&target == &source) { return; }

            invalidate_lifetime_registrations(target);
            target.parent             = source.parent;
            target.index              = source.index;
            target.last_modified_time = source.last_modified_time;
            target.storage_kind       = source.storage_kind;
            target.subscribers        = std::move(source.subscribers);
            target.feature_registry   = std::move(source.feature_registry);
            transfer_ref_invalidators(target, source);
            transfer_target_link_invalidators(target, source);
            transfer_state_invalidators(target, source);
        }
    }  // namespace

    TimeSeriesFeatureRegistry::~TimeSeriesFeatureRegistry() = default;
    void TimeSeriesFeatureRegistry::rebind_link_source(const LinkedTSContext *, engine_time_t) {}

    void rebind_base_state_lifetime(BaseState &state, BaseState *old_state) noexcept {
        if (old_state == nullptr || old_state == &state) { return; }

        for (ReferenceInvalidator *invalidator : state.ref_invalidators) {
            if (invalidator != nullptr) { rebind_ref_target(*invalidator, old_state, &state); }
        }
        for (TargetLinkInvalidator *invalidator : state.target_link_invalidators) {
            if (invalidator != nullptr) { rebind_target_link_invalidator(*invalidator, old_state, &state); }
        }

        std::vector<StateInvalidator *> stale_invalidators;
        for (StateInvalidator *invalidator : state.state_invalidators) {
            if (invalidator == nullptr) { continue; }
            if (invalidator->rebind != nullptr) {
                invalidator->state_destroyed = false;
                invalidator->rebind(invalidator->owner, old_state, &state);
            } else {
                stale_invalidators.push_back(invalidator);
            }
        }

        for (StateInvalidator *invalidator : stale_invalidators) {
            state.state_invalidators.erase(invalidator);
            invalidate_state(*invalidator);
        }
    }

    BaseState::BaseState(BaseState &&other) noexcept { move_base_state_fields(*this, other); }

    BaseState &BaseState::operator=(BaseState &&other) noexcept {
        move_base_state_fields(*this, other);
        return *this;
    }

    // Destructor severs every peered REF that points at this state and then
    // poisons `storage_kind` as a stale-memory sentinel. The poison is
    // best-effort (the memory may be reused before any reader notices), but
    // the invalidation sweep below is the authoritative fix: it switches
    // every dangling ref to EMPTY before this state's storage goes away.
    BaseState::~BaseState() noexcept {
        // Snapshot first because invalidation will, in the normal flow, call
        // back into unregister_* and mutate these sets.
        invalidate_lifetime_registrations(*this);
        storage_kind = TSStorageKind::Destroyed;
    }

    void BaseState::register_ref_invalidator(ReferenceInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { ref_invalidators.insert(invalidator); }
    }

    void BaseState::unregister_ref_invalidator(ReferenceInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { ref_invalidators.erase(invalidator); }
    }

    void BaseState::register_target_link_invalidator(TargetLinkInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { target_link_invalidators.insert(invalidator); }
    }

    void BaseState::unregister_target_link_invalidator(TargetLinkInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { target_link_invalidators.erase(invalidator); }
    }

    void BaseState::register_state_invalidator(StateInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { state_invalidators.insert(invalidator); }
    }

    void BaseState::unregister_state_invalidator(StateInvalidator *invalidator) noexcept {
        if (invalidator != nullptr) { state_invalidators.erase(invalidator); }
    }

    void invalidate_state(StateInvalidator &invalidator) noexcept {
        invalidator.state_destroyed = true;
        if (invalidator.invalidate != nullptr) { invalidator.invalidate(invalidator.owner); }
    }

    namespace
    {
        enum class RootNodePort : size_t {
            Input           = 0,
            Output          = 1,
            ErrorOutput     = 2,
            RecordableState = 3,
        };

        [[nodiscard]] BaseState *state_address(const std::unique_ptr<TimeSeriesStateV> &state) noexcept {
            return state != nullptr ? std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *state) : nullptr;
        }

        [[nodiscard]] bool has_any_child_state(const BaseCollectionState &state) noexcept {
            return std::ranges::any_of(state.child_states, [](const auto &child) { return child != nullptr; });
        }

        [[nodiscard]] TimeSeriesStateV *owning_state_variant(BaseState *state) noexcept {
            if (state == nullptr) { return nullptr; }

            TimeSeriesStateV *slot = nullptr;
            hgraph::visit(
                state->parent,
                [&](auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState> ||
                                  std::same_as<T, SignalState>) {
                        if (parent != nullptr && state->index < parent->child_states.size() &&
                            parent->child_states[state->index] != nullptr &&
                            state_address(parent->child_states[state->index]) == state) {
                            slot = parent->child_states[state->index].get();
                        }
                    }
                },
                [] {});
            return slot;
        }

        [[nodiscard]] TSViewContext view_context_from_target(const LinkedTSContext &target) noexcept {
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

        [[nodiscard]] TSOutputView output_view_from_target(const LinkedTSContext &target) noexcept {
            return TSOutputView{
                view_context_from_target(target),
                TSViewContext::none(),
                target.notification_state != nullptr ? target.notification_state->last_modified_time
                                                     : (target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT),
                target.owning_output,
                target.output_view_ops != nullptr ? target.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] TSViewContext context_from_root_state(const TSViewContext &context) noexcept {
            BaseState *state = context.ts_state;
            if (state == nullptr) { return context; }
            // Detect states whose ~BaseState destructor has already run: their
            // storage_kind is poisoned to TSStorageKind::Destroyed (0xFF).
            // Without this guard we'd keep walking through freed memory via
            // state->parent into a dangling TSInput/TSOutput.
            if (state->storage_kind == TSStorageKind::Destroyed) { return context; }
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
            const Node         *root_node   = nullptr;
            RootNodePort        root_port   = RootNodePort::Input;
            const TSInput      *root_input  = nullptr;
            const TSOutput     *root_output = nullptr;
            BaseState          *cursor      = state;

            while (cursor != nullptr) {
                bool advanced = false;
                hgraph::visit(
                    cursor->parent,
                    [&](TSLState *parent) {
                        path.push_back(cursor->index);
                        cursor   = parent;
                        advanced = true;
                    },
                    [&](TSDState *parent) {
                        path.push_back(cursor->index);
                        cursor   = parent;
                        advanced = true;
                    },
                    [&](TSBState *parent) {
                        path.push_back(cursor->index);
                        cursor   = parent;
                        advanced = true;
                    },
                    [&](SignalState *parent) {
                        path.push_back(cursor->index);
                        cursor   = parent;
                        advanced = true;
                    },
                    [&](Node *parent) {
                        root_node = parent;
                        root_port = static_cast<RootNodePort>(cursor->index);
                        cursor    = nullptr;
                        advanced  = true;
                    },
                    [&](TSInput *parent) {
                        root_input = parent;
                        cursor     = nullptr;
                        advanced   = true;
                    },
                    [&](TSOutput *parent) {
                        root_output = parent;
                        cursor      = nullptr;
                        advanced    = true;
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
                current               = output_root->view(MIN_DT).context_ref();
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
                const auto         *collection =
                    resolved_parent.ts_dispatch != nullptr ? resolved_parent.ts_dispatch->as_collection() : nullptr;
                if (collection == nullptr) { return refreshed; }

                current = collection->child_at(current, slot);
                if (!current.is_bound()) { return refreshed; }
            }

            refreshed.schema          = current.schema != nullptr ? current.schema : refreshed.schema;
            refreshed.value_dispatch  = current.value_dispatch != nullptr ? current.value_dispatch : refreshed.value_dispatch;
            refreshed.ts_dispatch     = current.ts_dispatch != nullptr ? current.ts_dispatch : refreshed.ts_dispatch;
            refreshed.value_data      = current.value_data;
            refreshed.ts_state        = current.ts_state != nullptr ? current.ts_state : refreshed.ts_state;
            refreshed.owning_output   = current.owning_output != nullptr ? current.owning_output : refreshed.owning_output;
            refreshed.output_view_ops = current.output_view_ops != nullptr ? current.output_view_ops : refreshed.output_view_ops;
            refreshed.notification_state =
                current.notification_state != nullptr ? current.notification_state : refreshed.notification_state;
            refreshed.pending_dict_child = current.pending_dict_child;
            return refreshed;
        }

        [[nodiscard]] bool reference_value_all_valid(const TimeSeriesReference &ref) noexcept {
            switch (ref.kind()) {
                case TimeSeriesReference::Kind::EMPTY: return false;

                case TimeSeriesReference::Kind::PEERED: return ref.target().is_bound() && ref.target_view().all_valid();

                case TimeSeriesReference::Kind::NON_PEERED:
                    return !ref.items().empty() &&
                           std::ranges::all_of(ref.items(), [](const auto &item) { return reference_value_all_valid(item); });
            }

            return false;
        }

        [[nodiscard]] TimeSeriesReference materialize_local_reference(const TSMeta &schema, BaseState *state) noexcept {
            if (state == nullptr) { return TimeSeriesReference::make(); }

            if (const LinkedTSContext *target = state->linked_target(); target != nullptr && target->is_bound()) {
                return TimeSeriesReference::make(output_view_from_target(*target));
            }

            switch (schema.kind) {
                case TSKind::TSB:
                    {
                        const auto                      &bundle_state = *static_cast<TSBState *>(state);
                        std::vector<TimeSeriesReference> items;
                        items.reserve(schema.field_count());
                        for (size_t index = 0; index < schema.field_count(); ++index) {
                            const BaseState *child        = index < bundle_state.child_states.size()
                                                                ? state_address(bundle_state.child_states[index])
                                                                : nullptr;
                            const TSMeta    *child_schema = schema.fields()[index].ts_type;
                            if (child_schema == nullptr) {
                                items.push_back(TimeSeriesReference::make());
                                continue;
                            }

                            TimeSeriesReference item = materialize_local_reference(*child_schema, const_cast<BaseState *>(child));
                            items.push_back(!item.is_empty() ? std::move(item) : TimeSeriesReference::make());
                        }
                        return TimeSeriesReference::make(std::move(items));
                    }

                case TSKind::TSL:
                    {
                        const auto                      &list_state = *static_cast<TSLState *>(state);
                        std::vector<TimeSeriesReference> items;
                        items.reserve(schema.fixed_size());
                        for (size_t index = 0; index < schema.fixed_size(); ++index) {
                            const BaseState *child =
                                index < list_state.child_states.size() ? state_address(list_state.child_states[index]) : nullptr;
                            const TSMeta *child_schema = schema.element_ts();
                            if (child_schema == nullptr) {
                                items.push_back(TimeSeriesReference::make());
                                continue;
                            }

                            TimeSeriesReference item = materialize_local_reference(*child_schema, const_cast<BaseState *>(child));
                            items.push_back(!item.is_empty() ? std::move(item) : TimeSeriesReference::make());
                        }
                        return TimeSeriesReference::make(std::move(items));
                    }

                default: return TimeSeriesReference::make();
            }
        }

        [[nodiscard]] BaseCollectionState *reference_collection_state(const TSMeta &schema, BaseState *state) noexcept {
            if (state == nullptr || schema.kind != TSKind::REF || schema.element_ts() == nullptr) { return nullptr; }

            TimeSeriesStateV *slot = owning_state_variant(state);
            if (slot == nullptr) { return nullptr; }

            switch (schema.element_ts()->kind) {
                case TSKind::TSB: return std::holds_alternative<TSBState>(*slot) ? static_cast<TSBState *>(state) : nullptr;

                case TSKind::TSL: return std::holds_alternative<TSLState>(*slot) ? static_cast<TSLState *>(state) : nullptr;

                default: return nullptr;
            }
        }

        [[nodiscard]] BaseState *target_notification_state(const LinkedTSContext &target) noexcept {
            if (target.notification_state != nullptr) { return target.notification_state; }
            if (target.ts_state != nullptr) { return target.ts_state; }
            if (target.pending_dict_child.parent_notification_state != nullptr) {
                return target.pending_dict_child.parent_notification_state;
            }
            return target.pending_dict_child.parent_ts_state;
        }

        [[nodiscard]] BaseState *target_lifetime_state(const LinkedTSContext &target) noexcept {
            return target.ts_state != nullptr ? target.ts_state : target.pending_dict_child.parent_ts_state;
        }

        template <typename TFn> void with_target_state(const LinkedTSContext &target, TFn &&fn) noexcept {
            BaseState *state = target_notification_state(target);
            // Skip when the target state has already been torn down: ~BaseState
            // poisons storage_kind, which lets us bail before dereferencing
            // freed memory (e.g. unsubscribe-on-RefLinkState-dtor when the
            // target output was destroyed earlier in the teardown sequence).
            if (state != nullptr && state->storage_kind != TSStorageKind::Destroyed) { std::forward<TFn>(fn)(state); }
        }

    }  // namespace

    namespace
    {
        [[nodiscard]] LinkedTSContext anchored_dereferenced_context(const LinkedTSContext &source,
                                                                    const LinkedTSContext &target) noexcept {
            const bool preserve_source_output_identity =
                source.output_view_ops != nullptr && source.output_view_ops != &detail::default_output_view_ops();
            return LinkedTSContext{
                target.schema,
                target.value_dispatch,
                target.ts_dispatch,
                target.value_data,
                target.ts_state,
                preserve_source_output_identity ? (source.owning_output != nullptr ? source.owning_output : target.owning_output)
                                                : (target.owning_output != nullptr ? target.owning_output : source.owning_output),
                preserve_source_output_identity
                    ? source.output_view_ops
                    : (target.output_view_ops != nullptr ? target.output_view_ops : source.output_view_ops),
                target.notification_state != nullptr ? target.notification_state : target.ts_state,
                target.pending_dict_child,
            };
        }

        [[nodiscard]] LinkedTSContext normalized_target_context(const LinkedTSContext &target) noexcept {
            if (!target.is_bound()) { return target; }
            LinkedTSContext normalized = output_view_from_target(target).linked_context();
            return normalized.is_bound() ? normalized : target;
        }
    }  // namespace

    LinkedTSContext detail::dereferenced_target_from_source(const LinkedTSContext &source) noexcept {
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
        if (value != nullptr) {
            if (!value->is_peered()) { return LinkedTSContext::none(); }
            const LinkedTSContext target     = value->target();
            const LinkedTSContext normalized = normalized_target_context(target);
            return anchored_dereferenced_context(source, normalized);
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
        [[nodiscard]] bool linked_context_equal_impl(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept {
            return lhs.schema == rhs.schema && lhs.value_dispatch == rhs.value_dispatch && lhs.ts_dispatch == rhs.ts_dispatch &&
                   lhs.value_data == rhs.value_data && lhs.ts_state == rhs.ts_state && lhs.owning_output == rhs.owning_output &&
                   lhs.output_view_ops == rhs.output_view_ops &&
                   lhs.pending_dict_child.parent_schema == rhs.pending_dict_child.parent_schema &&
                   lhs.pending_dict_child.parent_value_dispatch == rhs.pending_dict_child.parent_value_dispatch &&
                   lhs.pending_dict_child.parent_ts_dispatch == rhs.pending_dict_child.parent_ts_dispatch &&
                   lhs.pending_dict_child.parent_value_data == rhs.pending_dict_child.parent_value_data &&
                   lhs.pending_dict_child.parent_ts_state == rhs.pending_dict_child.parent_ts_state &&
                   lhs.pending_dict_child.parent_owning_output == rhs.pending_dict_child.parent_owning_output &&
                   lhs.pending_dict_child.parent_output_view_ops == rhs.pending_dict_child.parent_output_view_ops &&
                   lhs.pending_dict_child.key.equals(rhs.pending_dict_child.key);
        }

        [[nodiscard]] bool retains_previous_target_value(const LinkedTSContext &target) noexcept {
            return target.schema != nullptr && (target.schema->kind == TSKind::TSS || target.schema->kind == TSKind::TSD);
        }

        [[nodiscard]] Value snapshot_target_value_impl(const LinkedTSContext &target, engine_time_t modified_time = MIN_DT) {
            if (!retains_previous_target_value(target) || !target.is_bound() || target.value_dispatch == nullptr ||
                target.value_data == nullptr) {
                return {};
            }

            View  current{target.value_dispatch, target.value_data, target.schema->value_type};
            Value snapshot = current.clone(target.schema->kind == TSKind::TSD ? MutationTracking::Plain : MutationTracking::Delta);

            if (target.schema->kind == TSKind::TSS) {
                auto       current_set  = current.as_set();
                BaseState *target_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
                if (modified_time == MIN_DT || target_state == nullptr || target_state->last_modified_time != modified_time) {
                    return snapshot;
                }

                const auto current_delta = current_set.delta();
                bool       has_delta     = false;
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
                auto mutation     = snapshot_set.begin_mutation(modified_time != MIN_DT ? modified_time : MIN_ST);
                for (const View &item : current_delta.added()) { static_cast<void>(mutation.remove(item)); }
                for (const View &item : current_delta.removed()) { static_cast<void>(mutation.add(item)); }
                snapshot_set.clear_delta_tracking();
            }

            if (target.schema->kind == TSKind::TSD) {
                auto       current_map  = current.as_map();
                BaseState *target_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
                if (modified_time == MIN_DT || target_state == nullptr || target_state->last_modified_time != modified_time) {
                    return snapshot;
                }

                const auto current_delta = current_map.delta();
                bool       has_delta     = false;
                for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                    if (!current_delta.slot_occupied(slot)) { continue; }
                    if (current_delta.slot_added(slot) || current_delta.slot_removed(slot) || current_delta.slot_updated(slot)) {
                        has_delta = true;
                        break;
                    }
                }
                if (!has_delta) { return snapshot; }

                auto snapshot_map = snapshot.view().as_map();
                auto mutation     = snapshot_map.begin_mutation(modified_time != MIN_DT ? modified_time : MIN_ST);

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

        [[nodiscard]] Value empty_target_value_impl(const LinkedTSContext &target) {
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
                        auto       set           = snapshot.view().as_set();
                        auto       mutation      = set.begin_mutation(MIN_ST);
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
                        auto       map           = snapshot.view().as_map();
                        auto       mutation      = map.begin_mutation(MIN_ST);
                        for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                            if (!current_delta.slot_occupied(slot) || current_delta.slot_removed(slot)) { continue; }
                            static_cast<void>(mutation.remove(current_delta.key_at_slot(slot)));
                        }
                        map.clear_delta_tracking();
                        return snapshot;
                    }

                default: return {};
            }
        }

        void replay_attachment_subtree(const LinkedTSContext &context, ActiveTrieNode *trie_node, Notifiable *notifier,
                                       bool subscribe) noexcept {
            if (!context.is_bound() || context.ts_state == nullptr || trie_node == nullptr || notifier == nullptr) { return; }

            if (trie_node->locally_active) {
                if (subscribe) {
                    context.ts_state->subscribe(notifier);
                } else {
                    context.ts_state->unsubscribe(notifier);
                }
            }

            if (trie_node->children.empty()) { return; }

            TSViewContext       view_context{context.schema, context.value_dispatch, context.ts_dispatch, context.value_data,
                                             context.ts_state};
            const TSViewContext resolved   = view_context.resolved();
            const auto         *collection = resolved.ts_dispatch != nullptr ? resolved.ts_dispatch->as_collection() : nullptr;
            if (collection == nullptr) { return; }

            for (const auto &[slot, child_trie] : trie_node->children) {
                if (!child_trie) { continue; }

                const TSViewContext child = collection->child_at(view_context, slot);
                if (!child.is_bound()) { continue; }

                replay_attachment_subtree(
                    LinkedTSContext{child.schema, child.value_dispatch, child.ts_dispatch, child.value_data, child.ts_state},
                    child_trie.get(), notifier, subscribe);
            }
        }
    }  // namespace

    bool detail::linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept {
        return linked_context_equal_impl(lhs, rhs);
    }

    detail::LinkTransitionSnapshot detail::transition_snapshot(const TSViewContext &context) noexcept {
        const BaseState *state = context.notification_state != nullptr ? context.notification_state : context.ts_state;
        size_t           steps = 0;
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
                        state                         = target != nullptr ? target->ts_state : nullptr;
                        break;
                    }

                case TSStorageKind::TargetLink:
                    {
                        const auto &link = *static_cast<const TargetLinkState *>(state);
                        if (link.switch_modified_time != MIN_DT && link.previous_target_value.has_value()) {
                            return {&link.previous_target_value, link.switch_modified_time};
                        }
                        const LinkedTSContext *target = link.linked_target();
                        state                         = target != nullptr ? target->ts_state : nullptr;
                        break;
                    }

                case TSStorageKind::RefLink:
                    {
                        const auto &link = *static_cast<const RefLinkState *>(state);
                        if (link.switch_modified_time != MIN_DT && link.previous_target_value.has_value()) {
                            return {&link.previous_target_value, link.switch_modified_time};
                        }
                        const LinkedTSContext *target = link.bound_link.linked_target();
                        state                         = target != nullptr ? target->ts_state : nullptr;
                        break;
                    }

                case TSStorageKind::Native: state = nullptr; break;
            }
        }

        return {};
    }

    TSViewContext detail::refresh_native_context(const TSViewContext &context) noexcept {
        // Only call through custom output_view_ops: the default ops would re-enter
        // context.resolved() → refresh_native_context (has_live_context is false when
        // ts_state is null), producing unbounded recursion with no new information.
        if (context.ts_state == nullptr && context.output_view_ops != nullptr &&
            context.output_view_ops != &detail::default_output_view_ops() && context.owning_output != nullptr) {
            LinkedTSContext current{
                context.schema,          context.value_dispatch,     context.ts_dispatch,
                context.value_data,      context.ts_state,           context.owning_output,
                context.output_view_ops, context.notification_state, context.pending_dict_child,
            };

            LinkedTSContext refreshed = output_view_from_target(current).linked_context();
            if (!linked_context_equal_impl(current, refreshed)) { return view_context_from_target(refreshed); }
        }

        return context_from_root_state(context);
    }

    Value detail::snapshot_target_value(const LinkedTSContext &target, engine_time_t modified_time) {
        return snapshot_target_value_impl(target, modified_time);
    }

    Value detail::empty_target_value(const LinkedTSContext &target) { return empty_target_value_impl(target); }

    void BaseState::subscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.insert(subscriber); }
    }

    void BaseState::unsubscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.erase(subscriber); }
    }

    LinkedTSContext *BaseState::linked_target() noexcept {
        return const_cast<LinkedTSContext *>(const_cast<const BaseState *>(this)->linked_target());
    }

    const LinkedTSContext *BaseState::linked_target() const noexcept {
        switch (storage_kind) {
            case TSStorageKind::Native: return nullptr;

            case TSStorageKind::OutputLink: return &static_cast<const OutputLinkState *>(this)->target;

            case TSStorageKind::TargetLink: return &static_cast<const TargetLinkState *>(this)->target;

            case TSStorageKind::RefLink: return &static_cast<const RefLinkState *>(this)->bound_link.target;
        }

        return nullptr;
    }

    BaseState *BaseState::resolved_state() noexcept {
        return const_cast<BaseState *>(const_cast<const BaseState *>(this)->resolved_state());
    }

    const BaseState *BaseState::resolved_state() const noexcept {
        if (const LinkedTSContext *target = linked_target(); target != nullptr) {
            return target->ts_state != nullptr ? target->ts_state->resolved_state() : nullptr;
        }
        return this;
    }

    Notifiable *BaseState::boundary_notifier(Notifiable *fallback) noexcept {
        switch (storage_kind) {
            case TSStorageKind::Native: return fallback;

            case TSStorageKind::OutputLink: return fallback;

            case TSStorageKind::TargetLink: return &static_cast<TargetLinkState *>(this)->scheduling_notifier;

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

    void BaseState::notify_parent_that_child_is_modified(engine_time_t modified_time) noexcept {
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

    bool detail::has_local_reference_binding(const TSViewContext &context) noexcept {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->element_ts() == nullptr ||
            context.ts_state == nullptr) {
            return false;
        }

        BaseCollectionState *collection_state = reference_collection_state(*context.schema, context.ts_state);
        return collection_state != nullptr && has_any_child_state(*collection_state);
    }

    bool detail::linked_context_valid(const LinkedTSContext &context) noexcept {
        if (context.schema == nullptr || context.value_dispatch == nullptr || context.ts_dispatch == nullptr) { return false; }
        if (context.ts_state == nullptr && context.notification_state == nullptr && context.output_view_ops == nullptr) {
            return false;
        }

        const TSViewContext target_context = view_context_from_target(context);
        return target_context.ts_dispatch != nullptr && target_context.ts_dispatch->valid(target_context);
    }

    bool detail::linked_context_all_valid(const LinkedTSContext &context) noexcept {
        if (context.schema == nullptr || context.value_dispatch == nullptr || context.ts_dispatch == nullptr) { return false; }
        if (context.ts_state == nullptr && context.notification_state == nullptr && context.output_view_ops == nullptr) {
            return false;
        }

        const TSViewContext target_context = view_context_from_target(context);
        return target_context.ts_dispatch != nullptr && target_context.ts_dispatch->all_valid(target_context);
    }

    const Value *detail::materialized_target_link_value(const TSViewContext &context) noexcept {
        if (context.ts_state == nullptr || context.ts_state->storage_kind != TSStorageKind::TargetLink) { return nullptr; }

        const auto *state = static_cast<const TargetLinkState *>(context.ts_state);
        return !state->target.is_bound() && state->switch_modified_time != MIN_DT && state->previous_target_value.has_value()
                   ? &state->previous_target_value
                   : nullptr;
    }

    const Value *detail::materialized_reference_value(const TSViewContext &context) noexcept {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->element_ts() == nullptr ||
            context.ts_state == nullptr) {
            return nullptr;
        }

        if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr && target->is_bound()) {
            return nullptr;
        }

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

    bool detail::reference_all_valid(const TSViewContext &context) noexcept {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF) { return false; }

        if (context.ts_state != nullptr) {
            if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr) { return target->is_bound(); }
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
        if (std::getenv("HGRAPH_DEBUG_TSD_CHILD_MOD") != nullptr) {
            std::fprintf(stderr, "child_modified parent=%p idx=%zu old_time=%lld new_time=%lld had=%d child_count=%zu\n",
                         static_cast<void *>(this), child_index, static_cast<long long>(last_modified_time.time_since_epoch().count()),
                         static_cast<long long>(modified_time.time_since_epoch().count()),
                         modified_children.contains(child_index), modified_children.size());
        }
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

    void TSDState::child_modified(size_t child_index, engine_time_t modified_time) noexcept {
        BaseCollectionState::child_modified(child_index, modified_time);
    }

    BaseCollectionState::~BaseCollectionState() = default;

    BaseCollectionState::BaseCollectionState(BaseCollectionState &&other) noexcept
        : BaseState(std::move(other)), child_states(std::move(other.child_states)),
          modified_children(std::move(other.modified_children)),
          materialized_reference_storage(std::move(other.materialized_reference_storage)) {
        other.child_states.clear();
        other.modified_children.clear();
        other.materialized_reference_storage.reset();
    }

    BaseCollectionState &BaseCollectionState::operator=(BaseCollectionState &&other) noexcept {
        if (this == &other) { return *this; }

        reset_child_states();
        BaseState::operator=(std::move(other));
        child_states                   = std::move(other.child_states);
        modified_children              = std::move(other.modified_children);
        materialized_reference_storage = std::move(other.materialized_reference_storage);

        other.child_states.clear();
        other.modified_children.clear();
        other.materialized_reference_storage.reset();
        return *this;
    }

    void BaseCollectionState::reset_child_states() noexcept {
        child_states.clear();
        materialized_reference_storage.reset();
    }

    void BaseCollectionState::reparent_child_states(TimeSeriesStateParentPtr parent) noexcept {
        for (auto &child : child_states) {
            if (BaseState *child_state = state_address(child); child_state != nullptr) { child_state->parent = parent; }
        }
    }

    TSLState::TSLState(TSLState &&other) noexcept : BaseCollectionState(std::move(other)) { reparent_child_states(this); }

    TSLState &TSLState::operator=(TSLState &&other) noexcept {
        if (this == &other) { return *this; }
        BaseCollectionState::operator=(std::move(other));
        reparent_child_states(this);
        return *this;
    }

    TSBState::TSBState(TSBState &&other) noexcept : BaseCollectionState(std::move(other)) { reparent_child_states(this); }

    TSBState &TSBState::operator=(TSBState &&other) noexcept {
        if (this == &other) { return *this; }
        BaseCollectionState::operator=(std::move(other));
        reparent_child_states(this);
        return *this;
    }

    SignalState::SignalState(SignalState &&other) noexcept
        : BaseCollectionState(std::move(other)), bound_schema(other.bound_schema) {
        reparent_child_states(this);
        other.bound_schema = nullptr;
    }

    SignalState &SignalState::operator=(SignalState &&other) noexcept {
        if (this == &other) { return *this; }
        BaseCollectionState::operator=(std::move(other));
        bound_schema = other.bound_schema;
        reparent_child_states(this);
        other.bound_schema = nullptr;
        return *this;
    }

    TSDState::~TSDState() { unbind_value_storage(); }

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

    void OutputLinkState::TargetNotifiable::notify(engine_time_t modified_time) {
        if (self != nullptr) {
            if (self->switch_modified_time != modified_time) {
                self->previous_target_value = {};
                self->switch_modified_time  = MIN_DT;
            }
            self->mark_modified(modified_time);
        }
    }

    namespace
    {
        void output_link_target_destroyed(void *owner) noexcept {
            auto *self = static_cast<OutputLinkState *>(owner);
            if (self == nullptr) { return; }

            BaseState *notification_state = self->target.notification_state != nullptr ? self->target.notification_state : nullptr;
            if (notification_state != nullptr && notification_state != self->target.ts_state &&
                notification_state->storage_kind != TSStorageKind::Destroyed) {
                notification_state->unsubscribe(&self->target_notifiable);
            }
            self->target.clear();
            self->previous_target_value = {};
            self->switch_modified_time  = MIN_DT;
            self->last_modified_time    = MIN_DT;
        }

        void output_link_target_rebind(void *owner, BaseState *old_state, BaseState *new_state) noexcept {
            auto *self = static_cast<OutputLinkState *>(owner);
            if (self == nullptr) { return; }

            rebind_linked_context_state(self->target, old_state, new_state);
        }
    }  // namespace

    OutputLinkState::OutputLinkState() noexcept : target_notifiable(this) {}

    OutputLinkState::~OutputLinkState() { reset_target(); }

    OutputLinkState::OutputLinkState(OutputLinkState &&other) noexcept : target_notifiable(this) {
        move_base_state_fields(*this, other);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time  = other.switch_modified_time;

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.previous_target_value = {};
        other.switch_modified_time  = MIN_DT;
    }

    OutputLinkState &OutputLinkState::operator=(OutputLinkState &&other) noexcept {
        if (this == &other) { return *this; }

        reset_target();
        move_base_state_fields(*this, other);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time  = other.switch_modified_time;

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.previous_target_value = {};
        other.switch_modified_time  = MIN_DT;

        return *this;
    }

    void OutputLinkState::set_target(LinkedTSContext target_state, engine_time_t modified_time) noexcept {
        const bool target_changed = !linked_context_equal_impl(target, target_state);
        Value      transition_value =
            target_changed && modified_time != MIN_DT ? snapshot_target_value_impl(target, modified_time) : Value{};

        unregister_from_target();
        target                = std::move(target_state);
        last_modified_time    = target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT;
        previous_target_value = std::move(transition_value);
        switch_modified_time  = previous_target_value.has_value() ? modified_time : MIN_DT;
        register_with_target();
    }

    void OutputLinkState::reset_target(engine_time_t modified_time) noexcept {
        previous_target_value = modified_time != MIN_DT ? snapshot_target_value_impl(target, modified_time) : Value{};
        switch_modified_time  = previous_target_value.has_value() ? modified_time : MIN_DT;
        unregister_from_target();
        target.clear();
        last_modified_time = switch_modified_time != MIN_DT ? switch_modified_time : MIN_DT;
    }

    void OutputLinkState::register_with_target() noexcept {
        if (!target.is_bound() && !target.pending_dict_child.active()) { return; }
        if (!target_invalidator) { target_invalidator = std::make_unique<StateInvalidator>(); }
        target_invalidator->owner           = this;
        target_invalidator->invalidate      = &output_link_target_destroyed;
        target_invalidator->rebind          = &output_link_target_rebind;
        target_invalidator->state_destroyed = false;

        with_target_state(target, [this](BaseState *ptr) { ptr->subscribe(&target_notifiable); });

        BaseState *lifetime_state = target_lifetime_state(target);
        if (lifetime_state != nullptr && lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->register_state_invalidator(target_invalidator.get());
        }
    }

    void OutputLinkState::unregister_from_target() noexcept {
        with_target_state(target, [this](BaseState *ptr) { ptr->unsubscribe(&target_notifiable); });
        BaseState *lifetime_state = target_lifetime_state(target);
        if (target_invalidator && !target_invalidator->state_destroyed && lifetime_state != nullptr &&
            lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->unregister_state_invalidator(target_invalidator.get());
        }
        target_invalidator.reset();
    }

    TargetLinkState::TargetLinkStateNotifiable::TargetLinkStateNotifiable(TargetLinkState *self_) noexcept : self(self_) {}

    void TargetLinkState::TargetLinkStateNotifiable::notify(engine_time_t modified_time) {
        if (self == nullptr) { return; }
        if (self->switch_modified_time != modified_time) {
            self->previous_target_value = {};
            self->switch_modified_time  = MIN_DT;
        }
        self->mark_modified(modified_time);
    }

    void TargetLinkState::SchedulingNotifier::notify(engine_time_t modified_time) {
        if (target != nullptr) { target->notify(modified_time); }
    }

    TargetLinkState::TargetLinkState() noexcept : target_notifiable(this) {}

    TargetLinkState::~TargetLinkState() { reset_target(); }

    TargetLinkState::TargetLinkState(TargetLinkState &&other) noexcept : target_notifiable(this) {
        move_base_state_fields(*this, other);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time  = other.switch_modified_time;
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
        other.switch_modified_time  = MIN_DT;
    }

    TargetLinkState &TargetLinkState::operator=(TargetLinkState &&other) noexcept {
        if (this == &other) { return *this; }

        reset_target();

        move_base_state_fields(*this, other);
        previous_target_value = std::move(other.previous_target_value);
        switch_modified_time  = other.switch_modified_time;
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
        other.switch_modified_time  = MIN_DT;
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

    bool          TargetLinkState::is_bound() const noexcept { return target.is_bound(); }
    engine_time_t TargetLinkState::last_target_modified_time() const noexcept {
        BaseState *target_state = target_notification_state(target);
        return target_state != nullptr ? target_state->last_modified_time : MIN_DT;
    }

    bool TargetLinkState::is_sampled() const noexcept { return last_modified_time > last_target_modified_time(); }

    void TargetLinkState::register_with_target() noexcept {
        if (!target.is_bound() && !target.pending_dict_child.active()) { return; }
        if (!target_invalidator) { target_invalidator = std::make_unique<TargetLinkInvalidator>(); }
        target_invalidator->owner            = this;
        target_invalidator->target_destroyed = false;
        BaseState *notification_state        = target_notification_state(target);
        if (notification_state != nullptr && notification_state->storage_kind != TSStorageKind::Destroyed) {
            notification_state->subscribe(&target_notifiable);
        }
        BaseState *lifetime_state = target_lifetime_state(target);
        if (lifetime_state != nullptr && lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->register_target_link_invalidator(target_invalidator.get());
        }
    }

    void TargetLinkState::unregister_from_target() noexcept {
        BaseState *notification_state = target_notification_state(target);
        if (notification_state != nullptr && notification_state->storage_kind != TSStorageKind::Destroyed) {
            notification_state->unsubscribe(&target_notifiable);
        }
        BaseState *lifetime_state = target_lifetime_state(target);
        if (target_invalidator && !target_invalidator->target_destroyed && lifetime_state != nullptr &&
            lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->unregister_target_link_invalidator(target_invalidator.get());
        }
        target_invalidator.reset();
    }

    void invalidate_target_link(TargetLinkInvalidator &invalidator) noexcept {
        invalidator.target_destroyed = true;
        if (TargetLinkState *owner = invalidator.owner; owner != nullptr) {
            BaseState *notification_state =
                owner->target.notification_state != nullptr ? owner->target.notification_state : nullptr;
            if (notification_state != nullptr && notification_state != owner->target.ts_state &&
                notification_state->storage_kind != TSStorageKind::Destroyed) {
                notification_state->unsubscribe(&owner->target_notifiable);
            }
            owner->target.clear();
            owner->previous_target_value = {};
            owner->switch_modified_time  = MIN_DT;
            owner->last_modified_time    = MIN_DT;
        }
    }

    engine_time_t RefLinkState::last_target_modified_time() const { return bound_link.last_modified_time; }

    bool RefLinkState::is_sampled() const { return last_modified_time > last_target_modified_time(); }

    RefLinkState::RefSourceNotifiable::RefSourceNotifiable(RefLinkState *self_) noexcept : self(self_) {}

    void RefLinkState::RefSourceNotifiable::notify(engine_time_t modified_time) {
        // A REF source tick means the reference may now point somewhere else.
        // Re-resolve the current target before propagating modification.
        if (self != nullptr) { self->refresh_target(modified_time, true); }
    }

    RefLinkState::DereferencedTargetNotifiable::DereferencedTargetNotifiable(RefLinkState *self_) noexcept : self(self_) {}

    void RefLinkState::DereferencedTargetNotifiable::notify(engine_time_t modified_time) {
        if (self != nullptr) {
            // Target-side data changed without the REF itself rebinding. Keep
            // the dereferenced target time in sync and propagate normally.
            if (self->switch_modified_time != modified_time) {
                self->previous_target_value = {};
                self->switch_modified_time  = MIN_DT;
            }
            self->bound_link.last_modified_time = modified_time;
            self->mark_modified(modified_time);
        }
    }

    namespace
    {
        void ref_link_source_destroyed(void *owner) noexcept {
            auto *self = static_cast<RefLinkState *>(owner);
            if (self == nullptr) { return; }

            self->source_destroyed();
        }

        void ref_link_source_rebind(void *owner, BaseState *old_state, BaseState *new_state) noexcept {
            auto *self = static_cast<RefLinkState *>(owner);
            if (self == nullptr) { return; }

            rebind_linked_context_state(self->source, old_state, new_state);
        }

        void ref_link_target_destroyed(void *owner) noexcept {
            auto *self = static_cast<RefLinkState *>(owner);
            if (self == nullptr) { return; }

            self->target_destroyed();
        }

        void ref_link_target_rebind(void *owner, BaseState *old_state, BaseState *new_state) noexcept {
            auto *self = static_cast<RefLinkState *>(owner);
            if (self == nullptr) { return; }

            rebind_linked_context_state(self->bound_link.target, old_state, new_state);
        }
    }  // namespace

    RefLinkState::RefLinkState() noexcept : source_notifiable(this), target_notifiable(this) {}

    RefLinkState::~RefLinkState() { reset_source(); }

    RefLinkState::RefLinkState(RefLinkState &&other) noexcept : source_notifiable(this), target_notifiable(this) {
        move_base_state_fields(*this, other);

        move_base_state_fields(bound_link, other.bound_link);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
        retain_transition_value = other.retain_transition_value;
        previous_target_value   = std::move(other.previous_target_value);
        switch_modified_time    = other.switch_modified_time;
        boundary_attachments    = std::move(other.boundary_attachments);

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
        other.previous_target_value   = {};
        other.switch_modified_time    = MIN_DT;
    }

    RefLinkState &RefLinkState::operator=(RefLinkState &&other) noexcept {
        if (this == &other) { return *this; }

        reset_source();

        move_base_state_fields(*this, other);

        move_base_state_fields(bound_link, other.bound_link);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
        retain_transition_value = other.retain_transition_value;
        previous_target_value   = std::move(other.previous_target_value);
        switch_modified_time    = other.switch_modified_time;
        boundary_attachments    = std::move(other.boundary_attachments);

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
        other.previous_target_value   = {};
        other.switch_modified_time    = MIN_DT;
        return *this;
    }

    void RefLinkState::set_source(LinkedTSContext source_state) noexcept {
        reset_source();
        source = std::move(source_state);
        register_with_source();
        refresh_target(source.ts_state != nullptr ? source.ts_state->last_modified_time : MIN_DT, false);
    }

    void RefLinkState::reset_source() noexcept {
        replay_boundary_attachments(false);
        unregister_from_source();
        unregister_from_target();
        source.clear();
        bound_link.target.clear();
        bound_link.last_modified_time = MIN_DT;
        previous_target_value         = {};
        switch_modified_time          = MIN_DT;
    }

    void RefLinkState::register_with_source() noexcept {
        if (!source.is_bound() && !source.pending_dict_child.active()) { return; }
        if (!source_invalidator) { source_invalidator = std::make_unique<StateInvalidator>(); }
        source_invalidator->owner           = this;
        source_invalidator->invalidate      = &ref_link_source_destroyed;
        source_invalidator->rebind          = &ref_link_source_rebind;
        source_invalidator->state_destroyed = false;

        with_target_state(source, [this](BaseState *ptr) { ptr->subscribe(&source_notifiable); });

        BaseState *lifetime_state = target_lifetime_state(source);
        if (lifetime_state != nullptr && lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->register_state_invalidator(source_invalidator.get());
        }
    }

    void RefLinkState::unregister_from_source() noexcept {
        with_target_state(source, [this](BaseState *ptr) { ptr->unsubscribe(&source_notifiable); });
        BaseState *lifetime_state = target_lifetime_state(source);
        if (source_invalidator && !source_invalidator->state_destroyed && lifetime_state != nullptr &&
            lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->unregister_state_invalidator(source_invalidator.get());
        }
        source_invalidator.reset();
    }

    void RefLinkState::register_with_target() noexcept {
        if (!bound_link.target.is_bound() && !bound_link.target.pending_dict_child.active()) { return; }
        if (!target_invalidator) { target_invalidator = std::make_unique<StateInvalidator>(); }
        target_invalidator->owner           = this;
        target_invalidator->invalidate      = &ref_link_target_destroyed;
        target_invalidator->rebind          = &ref_link_target_rebind;
        target_invalidator->state_destroyed = false;

        with_target_state(bound_link.target, [this](BaseState *ptr) { ptr->subscribe(&target_notifiable); });

        BaseState *lifetime_state = target_lifetime_state(bound_link.target);
        if (lifetime_state != nullptr && lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->register_state_invalidator(target_invalidator.get());
        }
    }

    void RefLinkState::unregister_from_target() noexcept {
        with_target_state(bound_link.target, [this](BaseState *ptr) { ptr->unsubscribe(&target_notifiable); });
        BaseState *lifetime_state = target_lifetime_state(bound_link.target);
        if (target_invalidator && !target_invalidator->state_destroyed && lifetime_state != nullptr &&
            lifetime_state->storage_kind != TSStorageKind::Destroyed) {
            lifetime_state->unregister_state_invalidator(target_invalidator.get());
        }
        target_invalidator.reset();
    }

    RefLinkState::BoundaryAttachment &RefLinkState::attachment_for(Notifiable *upstream_notifier) noexcept {
        auto [it, inserted] = boundary_attachments.try_emplace(upstream_notifier);
        auto &attachment    = it->second;
        attachment.forwarding_notifier.set_target(upstream_notifier);
        return attachment;
    }

    BaseState *RefLinkState::current_target_root_state() const noexcept {
        return bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->resolved_state() : nullptr;
    }

    void RefLinkState::source_destroyed() noexcept {
        BaseState *notification_state = source.notification_state != nullptr ? source.notification_state : nullptr;
        if (notification_state != nullptr && notification_state != source.ts_state &&
            notification_state->storage_kind != TSStorageKind::Destroyed) {
            notification_state->unsubscribe(&source_notifiable);
        }
        replay_boundary_attachments(false);
        unregister_from_target();
        source.clear();
        bound_link.target.clear();
        bound_link.last_modified_time = MIN_DT;
        previous_target_value         = {};
        switch_modified_time          = MIN_DT;
        last_modified_time            = MIN_DT;
    }

    void RefLinkState::target_destroyed() noexcept {
        BaseState *notification_state =
            bound_link.target.notification_state != nullptr ? bound_link.target.notification_state : nullptr;
        if (notification_state != nullptr && notification_state != bound_link.target.ts_state &&
            notification_state->storage_kind != TSStorageKind::Destroyed) {
            notification_state->unsubscribe(&target_notifiable);
        }
        bound_link.target.clear();
        bound_link.last_modified_time = MIN_DT;
        previous_target_value         = {};
        switch_modified_time          = MIN_DT;
    }

    void RefLinkState::replay_boundary_attachments(bool subscribe) noexcept {
        if (!bound_link.target.is_bound()) { return; }

        for (auto &[upstream_notifier, attachment] : boundary_attachments) {
            attachment.forwarding_notifier.set_target(upstream_notifier);
            replay_attachment_subtree(bound_link.target, attachment.active_trie.root_node(), &attachment.forwarding_notifier,
                                      subscribe);
        }
    }

    void RefLinkState::refresh_target(engine_time_t modified_time, bool propagate) noexcept {
        const LinkedTSContext previous_target = bound_link.target;
        replay_boundary_attachments(false);
        unregister_from_target();
        bound_link.target = detail::dereferenced_target_from_source(source);
        bound_link.last_modified_time =
            bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->last_modified_time : MIN_DT;
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
                switch_modified_time  = MIN_DT;
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
            switch_modified_time  = MIN_DT;
            last_modified_time    = bound_link.last_modified_time;
        }
    }

}  // namespace hgraph
