#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>

#include <type_traits>

namespace hgraph
{
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
        std::visit(
            [this, modified_time](auto *ptr) {
                using T = std::remove_pointer_t<decltype(ptr)>;

                if (ptr == nullptr) { return; }

                if constexpr (std::is_same_v<T, TSOutput>) {
                    return;
                } else {
                    ptr->child_modified(index, modified_time);
                }
            },
            parent);
    }

    void BaseCollectionState::child_modified(size_t child_index, engine_time_t modified_time) noexcept {
        if (last_modified_time != modified_time) { modified_children.clear(); }

        modified_children.insert(child_index);
        mark_modified(modified_time);
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

    TargetLinkState::TargetLinkState() noexcept : target_notifiable(this) {}

    void TargetLinkState::set_target(TimeSeriesLeafStatePtr target_state) noexcept {
        unregister_from_target();
        target = target_state;
        register_with_target();
    }

    void TargetLinkState::reset_target() noexcept {
        unregister_from_target();
        target = static_cast<TSState *>(nullptr);
    }

    bool TargetLinkState::is_bound() const noexcept {
        return std::visit([](const auto *ptr) { return ptr != nullptr; }, target);
    }
    void TargetLinkState::register_with_target() noexcept {
        std::visit(
            [this](auto *ptr) {
                if (ptr != nullptr) { ptr->subscribe(&target_notifiable); }
            },
            target);
    }

    void TargetLinkState::unregister_from_target() noexcept {
        std::visit(
            [this](auto *ptr) {
                if (ptr != nullptr) { ptr->unsubscribe(&target_notifiable); }
            },
            target);
    }

    engine_time_t RefLinkState::last_target_modified_time() const { return bound_link.last_modified_time; }

    bool RefLinkState::is_sampled() const { return last_modified_time > last_target_modified_time(); }

}  // namespace hgraph
