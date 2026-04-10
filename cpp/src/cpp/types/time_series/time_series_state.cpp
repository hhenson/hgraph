#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/v2/ref.h>

#include <algorithm>
#include <type_traits>

namespace hgraph
{
    TimeSeriesFeatureRegistry::~TimeSeriesFeatureRegistry() = default;

    namespace
    {
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
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState>) {
                        if (parent != nullptr && state->index < parent->child_states.size() && parent->child_states[state->index] != nullptr &&
                            state_address(parent->child_states[state->index]) == state) {
                            slot = parent->child_states[state->index].get();
                        }
                    }
                },
                [] {});
            return slot;
        }

        [[nodiscard]] TSOutputView output_view_from_target(const LinkedTSContext &target) noexcept
        {
            return TSOutputView{
                TSViewContext{target.schema, target.value_dispatch, target.ts_dispatch, target.value_data, target.ts_state},
                TSViewContext::none(),
                target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT,
                target.owning_output,
                target.output_view_ops != nullptr ? target.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] bool reference_value_all_valid(const v2::TimeSeriesReference &ref) noexcept
        {
            switch (ref.kind()) {
                case v2::TimeSeriesReference::Kind::EMPTY:
                    return false;

                case v2::TimeSeriesReference::Kind::PEERED:
                    return ref.target().is_bound() && ref.target_view().all_valid();

                case v2::TimeSeriesReference::Kind::NON_PEERED:
                    return !ref.items().empty() &&
                           std::ranges::all_of(ref.items(), [](const auto &item) { return reference_value_all_valid(item); });
            }

            return false;
        }

        [[nodiscard]] v2::TimeSeriesReference materialize_local_reference(const TSMeta &schema, BaseState *state) noexcept
        {
            if (state == nullptr) { return v2::TimeSeriesReference::make(); }

            if (const LinkedTSContext *target = state->linked_target(); target != nullptr && target->is_bound()) {
                return v2::TimeSeriesReference::make(output_view_from_target(*target));
            }

            switch (schema.kind) {
                case TSKind::TSB:
                    {
                        const auto &bundle_state = *static_cast<TSBState *>(state);
                        std::vector<v2::TimeSeriesReference> items;
                        items.reserve(schema.field_count());
                        for (size_t index = 0; index < schema.field_count(); ++index) {
                            const BaseState *child = index < bundle_state.child_states.size() ? state_address(bundle_state.child_states[index]) : nullptr;
                            const TSMeta *child_schema = schema.fields()[index].ts_type;
                            items.push_back(child_schema != nullptr
                                                ? materialize_local_reference(*child_schema, const_cast<BaseState *>(child))
                                                : v2::TimeSeriesReference::make());
                        }
                        return v2::TimeSeriesReference::make(std::move(items));
                    }

                case TSKind::TSL:
                    {
                        const auto &list_state = *static_cast<TSLState *>(state);
                        std::vector<v2::TimeSeriesReference> items;
                        items.reserve(schema.fixed_size());
                        for (size_t index = 0; index < schema.fixed_size(); ++index) {
                            const BaseState *child = index < list_state.child_states.size() ? state_address(list_state.child_states[index]) : nullptr;
                            const TSMeta *child_schema = schema.element_ts();
                            items.push_back(child_schema != nullptr
                                                ? materialize_local_reference(*child_schema, const_cast<BaseState *>(child))
                                                : v2::TimeSeriesReference::make());
                        }
                        return v2::TimeSeriesReference::make(std::move(items));
                    }

                default:
                    return v2::TimeSeriesReference::make();
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

        void notify_parent_child_modified(TimeSeriesStateParentPtr parent, size_t child_index, engine_time_t modified_time) noexcept
        {
            hgraph::visit(
                parent,
                [child_index, modified_time](auto *ptr) {
                    using T = std::remove_pointer_t<decltype(ptr)>;

                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState>) {
                        ptr->child_modified(child_index, modified_time);
                    }
                },
                [] {});
        }

        template <typename TFn>
        void with_target_state(const LinkedTSContext &target, TFn &&fn) noexcept
        {
            BaseState *state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
            if (state != nullptr) { std::forward<TFn>(fn)(state); }
        }

        [[nodiscard]] LinkedTSContext dereferenced_target_from_source(const LinkedTSContext &source) noexcept
        {
            if (!source.is_bound() || source.schema == nullptr || source.schema->kind != TSKind::REF || source.value_dispatch == nullptr ||
                source.value_data == nullptr) {
                return LinkedTSContext::none();
            }

            const auto view = View{source.value_dispatch, source.value_data, source.schema->value_type}.as_atomic();
            const auto *value = view.try_as<hgraph::v2::TimeSeriesReference>();
            return value != nullptr && value->is_peered() ? value->target() : LinkedTSContext::none();
        }

        [[nodiscard]] bool linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept
        {
            return lhs.schema == rhs.schema && lhs.value_dispatch == rhs.value_dispatch && lhs.ts_dispatch == rhs.ts_dispatch &&
                   lhs.value_data == rhs.value_data && lhs.ts_state == rhs.ts_state &&
                   lhs.notification_state == rhs.notification_state &&
                   lhs.owning_output == rhs.owning_output && lhs.output_view_ops == rhs.output_view_ops;
        }

        [[nodiscard]] Value snapshot_target_value(const LinkedTSContext &target, engine_time_t modified_time = MIN_DT)
        {
            if (!target.is_bound() || target.schema == nullptr || target.value_dispatch == nullptr || target.value_data == nullptr) {
                return {};
            }

            View current{target.value_dispatch, target.value_data, target.schema->value_type};
            Value snapshot = current.clone();

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
                auto mutation = snapshot_set.begin_mutation();
                for (const View &item : current_delta.added()) { static_cast<void>(mutation.remove(item)); }
                for (const View &item : current_delta.removed()) { static_cast<void>(mutation.add(item)); }
                snapshot_set.clear_delta_tracking();
            }

            return snapshot;
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
        if (const LinkedTSContext *target = linked_target(); target != nullptr) { return target->ts_state; }
        return this;
    }

    Notifiable *BaseState::boundary_notifier(Notifiable *fallback) noexcept
    {
        switch (storage_kind) {
            case TSStorageKind::Native:
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

    void BaseState::notify_parent_that_child_is_modified(engine_time_t modified_time) noexcept {
        notify_parent_child_modified(parent, index, modified_time);
    }

    bool detail::refresh_native_dict_child_context(TSViewContext &context) noexcept
    {
        BaseState *state = context.ts_state;
        if (state == nullptr || state->storage_kind != TSStorageKind::Native) { return false; }

        TSDState *parent_state = nullptr;
        hgraph::visit(
            state->parent,
            [&](auto *parent) {
                using T = std::remove_pointer_t<decltype(parent)>;
                if constexpr (std::same_as<T, TSDState>) { parent_state = parent; }
            },
            [] {});
        if (parent_state == nullptr) { return false; }

        const size_t slot = state->index;
        if (slot >= parent_state->child_states.size() || state_address(parent_state->child_states[slot]) != state ||
            parent_state->map_dispatch == nullptr || parent_state->map_value_data == nullptr) {
            if (context.schema != nullptr && context.schema->kind == TSKind::TSD) {
                static_cast<TSDState *>(state)->unbind_value_storage();
            }
            context.value_data = nullptr;
            return true;
        }

        const auto *dispatch = parent_state->map_dispatch;
        const size_t slot_capacity = dispatch->slot_capacity(parent_state->map_value_data);
        const bool occupied = slot < slot_capacity && dispatch->slot_occupied(parent_state->map_value_data, slot);
        const bool removed = occupied && dispatch->slot_removed(parent_state->map_value_data, slot);
        const bool live = occupied && !removed;
        const bool slot_changed =
            occupied && (dispatch->slot_added(parent_state->map_value_data, slot) ||
                         dispatch->slot_updated(parent_state->map_value_data, slot) || removed);
        if (parent_state->last_modified_time != MIN_DT && (slot_changed || (live && state->last_modified_time == MIN_DT))) {
            state->last_modified_time = parent_state->last_modified_time;
        }
        if (!occupied) {
            if (context.schema != nullptr && context.schema->kind == TSKind::TSD) {
                static_cast<TSDState *>(state)->unbind_value_storage();
            }
            context.value_data = nullptr;
            return true;
        }

        context.value_data = dispatch->value_data(parent_state->map_value_data, slot);
        if (context.schema != nullptr && context.schema->kind == TSKind::TSD && context.value_data != nullptr) {
            const auto &builder = TSValueBuilderFactory::checked_builder_for(*context.schema);
            static_cast<TSDState *>(state)->bind_value_storage(
                *context.schema->element_ts(),
                static_cast<const detail::MapViewDispatch &>(builder.value_builder().dispatch()),
                context.value_data);
        }

        return true;
    }

    bool detail::has_local_reference_binding(const TSViewContext &context) noexcept
    {
        if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->element_ts() == nullptr ||
            context.ts_state == nullptr) {
            return false;
        }

        if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr && target->is_bound()) { return true; }

        BaseCollectionState *collection_state = reference_collection_state(*context.schema, context.ts_state);
        return collection_state != nullptr && has_any_child_state(*collection_state);
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

        if (const Value *materialized = materialized_reference_value(context); materialized != nullptr) {
            if (const auto *ref = materialized->view().as_atomic().try_as<v2::TimeSeriesReference>()) {
                return reference_value_all_valid(*ref);
            }
            return false;
        }

        View value = context.value();
        if (const auto *ref = value.as_atomic().try_as<v2::TimeSeriesReference>()) { return reference_value_all_valid(*ref); }
        return false;
    }

    void BaseCollectionState::child_modified(size_t child_index, engine_time_t modified_time) noexcept {
        if (suppress_repeated_child_notifications) {
            if (last_modified_time == MIN_DT) {
                modified_children.clear();
                modified_children.insert(child_index);
                mark_modified(modified_time);
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

    TargetLinkState::TargetLinkStateNotifiable::TargetLinkStateNotifiable(TargetLinkState *self_) noexcept : self(self_) {}

    void TargetLinkState::TargetLinkStateNotifiable::notify(engine_time_t modified_time) {
        if (self != nullptr) { self->mark_modified(modified_time); }
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
        scheduling_notifier.set_target(other.scheduling_notifier.get_target());

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.scheduling_notifier.set_target(nullptr);
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
        scheduling_notifier.set_target(other.scheduling_notifier.get_target());

        if (other.target.is_bound()) {
            const LinkedTSContext bound_target = other.target;
            other.unregister_from_target();
            target = bound_target;
            register_with_target();
            other.target.clear();
        }

        other.scheduling_notifier.set_target(nullptr);
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
        bound_link.target = dereferenced_target_from_source(source);
        bound_link.last_modified_time = bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->last_modified_time : MIN_DT;
        register_with_target();
        replay_boundary_attachments(true);

        if (propagate) {
            const bool target_changed = !linked_context_equal(previous_target, bound_link.target);

            // Restrict sampled REF propagation for now: ordinary Python code
            // mostly treats refs as opaque handles, so a source-side tick that
            // resolves to the same target should not manufacture a fresh delta.
            if (target_changed) {
                previous_target_value = snapshot_target_value(previous_target, modified_time);
                switch_modified_time = modified_time;
                mark_modified(modified_time);
            } else {
                previous_target_value = {};
                switch_modified_time = MIN_DT;
                last_modified_time = bound_link.last_modified_time;
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
