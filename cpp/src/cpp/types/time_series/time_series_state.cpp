#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>

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
    }  // namespace

    void BaseState::subscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.insert(subscriber); }
    }

    void BaseState::unsubscribe(Notifiable *subscriber) noexcept {
        if (subscriber != nullptr) { subscribers.erase(subscriber); }
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

}  // namespace hgraph
