#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/v2/ref.h>

#include <type_traits>

namespace hgraph
{
    namespace
    {
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
            if (target.ts_state != nullptr) { std::forward<TFn>(fn)(target.ts_state); }
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

    void BaseState::notify_parent_that_child_is_modified(engine_time_t modified_time) noexcept {
        notify_parent_child_modified(parent, index, modified_time);
    }

    void BaseCollectionState::child_modified(size_t child_index, engine_time_t modified_time) noexcept {
        if (last_modified_time != modified_time) { modified_children.clear(); }

        modified_children.insert(child_index);
        mark_modified(modified_time);
    }

    BaseCollectionState::~BaseCollectionState() = default;

    BaseCollectionState::BaseCollectionState(BaseCollectionState &&other) noexcept
        : BaseState(std::move(other)),
          child_states(std::move(other.child_states)),
          modified_children(std::move(other.modified_children))
    {
        other.child_states.clear();
        other.modified_children.clear();
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
        child_states = std::move(other.child_states);
        modified_children = std::move(other.modified_children);

        other.child_states.clear();
        other.modified_children.clear();
        return *this;
    }

    void BaseCollectionState::reset_child_states() noexcept
    {
        child_states.clear();
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

        bound_link.parent = other.bound_link.parent;
        bound_link.index = other.bound_link.index;
        bound_link.last_modified_time = other.bound_link.last_modified_time;
        bound_link.storage_kind = other.bound_link.storage_kind;
        bound_link.subscribers = std::move(other.bound_link.subscribers);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
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

        bound_link.parent = other.bound_link.parent;
        bound_link.index = other.bound_link.index;
        bound_link.last_modified_time = other.bound_link.last_modified_time;
        bound_link.storage_kind = other.bound_link.storage_kind;
        bound_link.subscribers = std::move(other.bound_link.subscribers);
        bound_link.scheduling_notifier.set_target(other.bound_link.scheduling_notifier.get_target());
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
        replay_boundary_attachments(false);
        unregister_from_target();
        bound_link.target = dereferenced_target_from_source(source);
        bound_link.last_modified_time = bound_link.target.ts_state != nullptr ? bound_link.target.ts_state->last_modified_time : MIN_DT;
        register_with_target();
        replay_boundary_attachments(true);

        if (propagate) {
            // When the REF source itself changed, the dereferenced view is
            // "sampled": consumers should observe this tick even if the new
            // target has not changed yet.
            mark_modified(modified_time);
        } else {
            // Initial binding should inherit the current target time without
            // manufacturing a sampled modification.
            last_modified_time = bound_link.last_modified_time;
        }
    }

}  // namespace hgraph
