#include <hgraph/types/time_series/ts_input/target_link.h>

#include "impl/target_link_structural_storage.h"
#include "target_link_ops.h"

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/util/scope.h>

#include <algorithm>
#include <string>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    struct TSInputTargetLinkStructuralStorage::StructuralTransition
    {
        struct KeySetNotifier final : Notifiable
        {
            explicit KeySetNotifier(TSInputTargetLinkStorage &owner_) noexcept : owner(&owner_) {}

            void notify(DateTime modified_time) override
            {
                if (owner != nullptr) { owner->record_key_set_modified(modified_time); }
            }

            void source_invalidated(const TSDataTracking *source) noexcept override
            {
                if (owner != nullptr) { owner->key_set_source_invalidated(source); }
            }

            TSInputTargetLinkStorage *owner{nullptr};
        };

        explicit StructuralTransition(TSInputTargetLinkStorage &owner) noexcept
            : key_set_notifier(owner)
        {
        }

        TSOutputHandle previous_target{};
        DateTime       modified_time{MIN_DT};
        bool           sampled_current{false};
        TSDataTracking key_set_tracking{};
        KeySetNotifier key_set_notifier;
        bool           key_set_subscribed{false};

        void clear() noexcept
        {
            previous_target.reset();
            modified_time = MIN_DT;
            sampled_current = false;
        }

        void rebind_owner(TSInputTargetLinkStorage &owner) noexcept
        {
            key_set_notifier.owner = &owner;
        }
    };

    namespace
    {
        [[nodiscard]] bool is_closed_union_narrowing(
            const TSValueTypeMetaData &requested,
            const TSValueTypeMetaData *source) noexcept
        {
            if (source == nullptr || requested.kind != TSTypeKind::TS || source->kind != TSTypeKind::TS ||
                requested.value_schema == nullptr || source->value_schema == nullptr ||
                !requested.value_schema->is_named_bundle() || !source->value_schema->is_named_bundle())
            {
                return false;
            }
            return TypeRegistry::instance().bundle_is_a(requested.value_schema, source->value_schema);
        }

        void unsubscribe_node(TSInputTargetActiveNode &node,
                              TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            if (!node.observed.bound()) { return; }
            [[maybe_unused]] auto reset_observed = make_scope_exit([&]() noexcept { node.observed.reset(); });
            auto view = node.observed.data_view();
            if (view.valid() && view.tracking().observers.contains(&notifier))
            {
                [[maybe_unused]] auto unsubscribe_observer =
                    make_scope_exit<true>([&] { view.unsubscribe(&notifier); });
            }
        }

        void unsubscribe_handle_noexcept(TSOutputHandle &observed, Notifiable *observer) noexcept
        {
            if (!observed.bound()) { return; }
            // Destruction can run after the observed output has already cleared observers;
            // normal unbind() remains strict for graph operation.
            if (observer != nullptr)
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    auto view = observed.data_view();
                    if (view.valid() && view.tracking().observers.contains(observer)) { view.unsubscribe(observer); }
                    return true;
                }));
            }
            observed.reset();
        }

        void unsubscribe_tree(TSInputTargetActiveNode &node,
                              TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            unsubscribe_node(node, notifier);
            node.children.for_each([&](std::size_t, TSInputTargetActiveNode &child) {
                unsubscribe_tree(child, notifier);
            });
        }

        void unsubscribe_tree_noexcept(TSInputTargetActiveNode &node,
                                       TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            unsubscribe_handle_noexcept(node.observed, &notifier);
            node.children.for_each([&](std::size_t, TSInputTargetActiveNode &child) {
                unsubscribe_tree_noexcept(child, notifier);
            });
        }

        void replace_observer(const TSOutputHandle &observed,
                              Notifiable           *previous,
                              Notifiable           *replacement) noexcept
        {
            if (!observed.bound() || previous == nullptr || replacement == nullptr) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                observed.data_view().replace_observer(previous, replacement);
                return true;
            }));
        }

        void replace_tree_observers(TSInputTargetActiveNode                    &node,
                                    TSInputTargetLinkState::SchedulingNotifier &previous,
                                    TSInputTargetLinkState::SchedulingNotifier &replacement) noexcept
        {
            replace_observer(node.observed, &previous, &replacement);
            node.children.for_each([&](std::size_t, TSInputTargetActiveNode &child) {
                replace_tree_observers(child, previous, replacement);
            });
        }

        [[nodiscard]] TSSDataView target_slot_set(const TSInputTargetLinkStorage &link)
        {
            auto target = link.target_view();
            if (!target.valid() || target.schema() == nullptr)
            {
                throw std::logic_error("Target-link slot observer requires a bound structural target");
            }
            auto structural = structural_observation_for(target);
            return structural.as_set();
        }

        [[nodiscard]] TSInputTargetLinkStructuralStorage &
        structural_storage(TSInputTargetLinkStorage &owner) noexcept
        {
            return static_cast<TSInputTargetLinkStructuralStorage &>(owner);
        }

        [[nodiscard]] const TSInputTargetLinkStructuralStorage &
        structural_storage(const TSInputTargetLinkStorage &owner) noexcept
        {
            return static_cast<const TSInputTargetLinkStructuralStorage &>(owner);
        }

        [[nodiscard]] TSInputTargetLinkStructuralStorage::StructuralTransition &
        ensure_structural_transition(TSInputTargetLinkStorage &owner)
        {
            auto &storage = structural_storage(owner);
            if (!storage.structural_transition)
            {
                storage.structural_transition =
                    std::make_unique<TSInputTargetLinkStructuralStorage::StructuralTransition>(owner);
            }
            return *storage.structural_transition;
        }

        void structural_clear_transition(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &transition = structural_storage(owner).structural_transition;
            if (transition) { transition->clear(); }
        }

        void structural_record_key_set_modified(TSInputTargetLinkStorage &owner,
                                                DateTime modified_time)
        {
            auto &transition = ensure_structural_transition(owner);
            static_cast<void>(transition.key_set_tracking.record_modified(modified_time));
        }

        void structural_key_set_source_invalidated(TSInputTargetLinkStorage &owner,
                                                   const TSDataTracking *source) noexcept
        {
            static_cast<void>(source);
            auto &transition = structural_storage(owner).structural_transition;
            if (transition) { transition->key_set_subscribed = false; }
        }

        void structural_subscribe_key_set_tracking(TSInputTargetLinkStorage &owner)
        {
            auto &transition = structural_storage(owner).structural_transition;
            if (!owner.bound() || !transition || transition->key_set_subscribed) { return; }
            auto key_set = target_slot_set(owner);
            key_set.base().subscribe(&transition->key_set_notifier);
            transition->key_set_subscribed = true;
            const auto modified_time = key_set.last_modified_time();
            if (modified_time != MIN_DT) { structural_record_key_set_modified(owner, modified_time); }
        }

        void structural_unsubscribe_key_set_tracking(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &transition = structural_storage(owner).structural_transition;
            if (!owner.bound() || !transition || !transition->key_set_subscribed) { return; }
            auto clear_subscribed = make_scope_exit([&] { transition->key_set_subscribed = false; });
            static_cast<void>(fallback_on_exception(false, [&] {
                target_slot_set(owner).base().unsubscribe(&transition->key_set_notifier);
                return true;
            }));
        }

        void structural_subscribe_slot_observers(TSInputTargetLinkStorage &owner)
        {
            auto &storage = structural_storage(owner);
            if (!owner.bound() || storage.slot_observers.empty()) { return; }

            auto set = target_slot_set(owner);
            std::vector<SlotObserver *> subscribed;
            subscribed.reserve(storage.slot_observers.size());
            auto rollback = make_scope_exit<true>([&] {
                for (SlotObserver *observer : subscribed) { set.unsubscribe_slot_observer(observer); }
            });
            storage.slot_observers.for_each([&](SlotObserver *observer) {
                set.subscribe_slot_observer(observer);
                subscribed.push_back(observer);
            });
            storage.slot_observers_subscribed = true;
            rollback.release();
        }

        void structural_unsubscribe_slot_observers(TSInputTargetLinkStorage &owner)
        {
            auto &storage = structural_storage(owner);
            if (!owner.bound() || !storage.slot_observers_subscribed) { return; }
            auto clear_subscribed = make_scope_exit([&] { storage.slot_observers_subscribed = false; });
            auto set = target_slot_set(owner);
            storage.slot_observers.for_each(
                [&](SlotObserver *observer) { set.unsubscribe_slot_observer(observer); });
        }

        void structural_unsubscribe_slot_observers_noexcept(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &storage = structural_storage(owner);
            if (!owner.bound() || !storage.slot_observers_subscribed) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                structural_unsubscribe_slot_observers(owner);
                return true;
            }));
        }

        void structural_publish_sampled_transition(TSInputTargetLinkStorage &owner,
                                                   DateTime modified_time)
        {
            auto &transition = ensure_structural_transition(owner);
            transition.modified_time = modified_time;
            transition.sampled_current = true;
            structural_record_key_set_modified(owner, modified_time);
        }

        void structural_detach_target(TSInputTargetLinkStorage &owner,
                                      bool retain_structural_target,
                                      DateTime modified_time)
        {
            auto &storage = structural_storage(owner);
            structural_unsubscribe_key_set_tracking(owner);
            if (owner.state_.target.bound() && storage.slot_observers_subscribed)
            {
                storage.slot_observers.notify_clear();
                structural_unsubscribe_slot_observers(owner);
            }
            if (retain_structural_target)
            {
                auto &transition = ensure_structural_transition(owner);
                transition.previous_target = owner.state_.target;
                transition.modified_time = modified_time;
                transition.sampled_current = false;
            }
            else { structural_clear_transition(owner); }
        }

        void structural_unbind_noexcept(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &storage = structural_storage(owner);
            structural_unsubscribe_key_set_tracking(owner);
            if (owner.state_.target.bound() && storage.slot_observers_subscribed)
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    storage.slot_observers.notify_clear();
                    return true;
                }));
                structural_unsubscribe_slot_observers_noexcept(owner);
            }
            structural_clear_transition(owner);
        }

        void structural_source_invalidated(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &storage = structural_storage(owner);
            const bool notify_slot_clear = storage.slot_observers_subscribed;
            storage.slot_observers_subscribed = false;
            if (storage.structural_transition)
            {
                storage.structural_transition->key_set_subscribed = false;
                storage.structural_transition->clear();
            }
            if (notify_slot_clear)
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    storage.slot_observers.notify_clear();
                    return true;
                }));
            }
        }

        void structural_add_slot_observer(TSInputTargetLinkStorage &owner,
                                          SlotObserver *observer)
        {
            auto &storage = structural_storage(owner);
            storage.slot_observers.add(observer);
            auto rollback = make_scope_exit<true>([&] { storage.slot_observers.remove(observer); });
            if (owner.bound()) { target_slot_set(owner).subscribe_slot_observer(observer); }
            if (owner.bound()) { storage.slot_observers_subscribed = true; }
            rollback.release();
        }

        void structural_remove_slot_observer(TSInputTargetLinkStorage &owner,
                                             SlotObserver *observer)
        {
            auto &storage = structural_storage(owner);
            if (owner.bound() && storage.slot_observers_subscribed)
            {
                target_slot_set(owner).unsubscribe_slot_observer(observer);
            }
            storage.slot_observers.remove(observer);
            if (storage.slot_observers.empty()) { storage.slot_observers_subscribed = false; }
        }

        const TSDataTracking &structural_key_set_tracking(TSInputTargetLinkStorage &owner)
        {
            auto &transition = ensure_structural_transition(owner);
            structural_subscribe_key_set_tracking(owner);
            return transition.key_set_tracking;
        }

        TSDataTracking &structural_mutable_key_set_tracking(TSInputTargetLinkStorage &owner)
        {
            auto &transition = ensure_structural_transition(owner);
            structural_subscribe_key_set_tracking(owner);
            return transition.key_set_tracking;
        }

        TSDataView structural_previous_target_view(const TSInputTargetLinkStorage &owner) noexcept
        {
            const auto &transition = structural_storage(owner).structural_transition;
            return transition ? transition->previous_target.data_view() : TSDataView{};
        }

        bool structural_transition_active(const TSInputTargetLinkStorage &owner) noexcept
        {
            const auto &transition = structural_storage(owner).structural_transition;
            return transition && transition->modified_time != MIN_DT &&
                   owner.tracking.last_modified_time == transition->modified_time;
        }

        bool structural_sampled_transition(const TSInputTargetLinkStorage &owner) noexcept
        {
            const auto &transition = structural_storage(owner).structural_transition;
            return structural_transition_active(owner) && transition->sampled_current;
        }

        DateTime structural_transition_time(const TSInputTargetLinkStorage &owner) noexcept
        {
            const auto &transition = structural_storage(owner).structural_transition;
            return transition ? transition->modified_time : MIN_DT;
        }

        DynamicStorageMetrics structural_dynamic_storage_metrics(
            const TSInputTargetLinkStorage &owner) noexcept
        {
            const auto &storage = structural_storage(owner);
            DynamicStorageMetrics result = storage.slot_observers.dynamic_storage_metrics();
            if (storage.structural_transition)
            {
                result.live_bytes += sizeof(TSInputTargetLinkStructuralStorage::StructuralTransition);
                result.reserved_bytes += sizeof(TSInputTargetLinkStructuralStorage::StructuralTransition);
                result += storage.structural_transition->key_set_tracking.observers.dynamic_storage_metrics();
            }
            return result;
        }

        void structural_before_move_assignment_source(TSInputTargetLinkStorage &owner) noexcept
        {
            auto &storage = structural_storage(owner);
            if (storage.slot_observers.empty()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                storage.slot_observers.notify_clear();
                return true;
            }));
            structural_unsubscribe_slot_observers_noexcept(owner);
            storage.slot_observers.clear();
            storage.slot_observers_subscribed = false;
        }

        void structural_resubscribe_after_move_assignment(TSInputTargetLinkStorage &owner) noexcept
        {
            static_cast<void>(fallback_on_exception(false, [&] {
                structural_subscribe_slot_observers(owner);
                return true;
            }));
        }

        void no_structural_action(TSInputTargetLinkStorage &) noexcept {}
        void no_structural_action_at(TSInputTargetLinkStorage &, DateTime) {}
        void no_structural_detach(TSInputTargetLinkStorage &, bool, DateTime) {}
        void no_structural_source_invalidated(TSInputTargetLinkStorage &) noexcept {}
        void no_structural_key_source_invalidated(TSInputTargetLinkStorage &,
                                                  const TSDataTracking *) noexcept {}
        void no_structural_add(TSInputTargetLinkStorage &, SlotObserver *)
        {
            throw std::logic_error("non-structural target links do not support slot observers");
        }
        const TSDataTracking &no_structural_key_tracking(TSInputTargetLinkStorage &)
        {
            throw std::logic_error("non-structural target links do not expose key-set tracking");
        }
        TSDataTracking &no_structural_mutable_key_tracking(TSInputTargetLinkStorage &)
        {
            throw std::logic_error("non-structural target links do not expose key-set tracking");
        }
        TSDataView no_structural_previous_target(const TSInputTargetLinkStorage &) noexcept { return {}; }
        bool no_structural_flag(const TSInputTargetLinkStorage &) noexcept { return false; }
        DateTime no_structural_time(const TSInputTargetLinkStorage &) noexcept { return MIN_DT; }
        DynamicStorageMetrics no_structural_metrics(const TSInputTargetLinkStorage &) noexcept { return {}; }

        [[nodiscard]] const TSInputTargetLinkStructuralOps &target_link_structural_ops() noexcept
        {
            static const TSInputTargetLinkStructuralOps ops{
                .supports_structural = true,
                .clear_transition = &structural_clear_transition,
                .subscribe_key_set_tracking = &structural_subscribe_key_set_tracking,
                .subscribe_slot_observers = &structural_subscribe_slot_observers,
                .publish_sampled_transition = &structural_publish_sampled_transition,
                .detach_target = &structural_detach_target,
                .unbind_noexcept = &structural_unbind_noexcept,
                .source_invalidated = &structural_source_invalidated,
                .add_slot_observer = &structural_add_slot_observer,
                .remove_slot_observer = &structural_remove_slot_observer,
                .key_set_tracking = &structural_key_set_tracking,
                .mutable_key_set_tracking = &structural_mutable_key_set_tracking,
                .record_key_set_modified = &structural_record_key_set_modified,
                .key_set_source_invalidated = &structural_key_set_source_invalidated,
                .previous_target_view = &structural_previous_target_view,
                .transition_active = &structural_transition_active,
                .sampled_transition = &structural_sampled_transition,
                .transition_time = &structural_transition_time,
                .dynamic_storage_metrics = &structural_dynamic_storage_metrics,
                .before_move_assignment_source = &structural_before_move_assignment_source,
                .resubscribe_after_move_assignment = &structural_resubscribe_after_move_assignment,
            };
            return ops;
        }

        [[nodiscard]] bool project_target_path(TSDataView &current,
                                               const TSValueTypeMetaData *&current_schema,
                                               const TSInputTargetActiveNode *node)
        {
            if (node == nullptr || node->parent == nullptr) { return current.valid() && current_schema != nullptr; }
            if (!project_target_path(current, current_schema, node->parent)) { return false; }

            const auto &ops = input_endpoint_ops_for(current_schema);
            if (ops.target_child == nullptr || ops.child_schema == nullptr) { return false; }
            current = ops.target_child(current, node->slot);
            current_schema = ops.child_schema(current_schema, node->slot);
            return current.valid() && current_schema != nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *target_schema_at_node(
            const TSValueTypeMetaData *current,
            const TSInputTargetActiveNode *node) noexcept
        {
            if (node == nullptr || node->parent == nullptr) { return current; }
            const auto *parent_schema = target_schema_at_node(current, node->parent);
            if (parent_schema == nullptr) { return nullptr; }
            return fallback_on_exception<const TSValueTypeMetaData *>(nullptr, [&] {
                const auto &ops = input_endpoint_ops_for(parent_schema);
                return ops.child_schema != nullptr ? ops.child_schema(parent_schema, node->slot) : nullptr;
            });
        }

        [[nodiscard]] TSOutputHandle observation_at_path(
            const TSInputTargetLinkStorage &link,
            const TSValueTypeMetaData &schema,
            const TSInputTargetActiveNode &node)
        {
            auto observed = link.target_output_at_path(schema, &node);
            if (!observed.bound() || node.observation_kind == TSInputObservationKind::Value)
            {
                return observed;
            }

            auto structural = structural_observation_for(observed.data_view());
            return TSOutputHandle{link.target_output().output(), std::move(structural)};
        }

        void resubscribe_tree(TSInputTargetLinkStorage &link,
                              const TSValueTypeMetaData &schema,
                              TSInputTargetActiveNode &node)
        {
            auto &state = link.state_;

            if (node.locally_active)
            {
                const auto observed = observation_at_path(link, schema, node);
                if (!node.observed.same_as(observed))
                {
                    unsubscribe_node(node, state.scheduling_notifier);
                    node.observed = observed;
                    if (node.observed.bound() && state.scheduling_notifier.target() != nullptr)
                    {
                        node.observed.data_view().subscribe(&state.scheduling_notifier);
                    }
                }
            }

            node.children.for_each([&](std::size_t, TSInputTargetActiveNode &child) {
                resubscribe_tree(link, schema, child);
            });
        }
    }  // namespace

    const TSInputTargetLinkStructuralOps &target_link_no_structural_ops() noexcept
    {
        static const TSInputTargetLinkStructuralOps ops{
            .supports_structural = false,
            .clear_transition = &no_structural_action,
            .subscribe_key_set_tracking = &no_structural_action,
            .subscribe_slot_observers = &no_structural_action,
            .publish_sampled_transition = &no_structural_action_at,
            .detach_target = &no_structural_detach,
            .unbind_noexcept = &no_structural_action,
            .source_invalidated = &no_structural_source_invalidated,
            .add_slot_observer = &no_structural_add,
            .remove_slot_observer = &no_structural_add,
            .key_set_tracking = &no_structural_key_tracking,
            .mutable_key_set_tracking = &no_structural_mutable_key_tracking,
            .record_key_set_modified = &no_structural_action_at,
            .key_set_source_invalidated = &no_structural_key_source_invalidated,
            .previous_target_view = &no_structural_previous_target,
            .transition_active = &no_structural_flag,
            .sampled_transition = &no_structural_flag,
            .transition_time = &no_structural_time,
            .dynamic_storage_metrics = &no_structural_metrics,
            .before_move_assignment_source = &no_structural_action,
            .resubscribe_after_move_assignment = &no_structural_action,
        };
        return ops;
    }

    const MemoryUtils::StoragePlan &target_link_storage_plan_for(TSTypeKind kind) noexcept
    {
        if (kind == TSTypeKind::TSS || kind == TSTypeKind::TSD)
        {
            return MemoryUtils::plan_for<TSInputTargetLinkStructuralStorage>();
        }
        return MemoryUtils::plan_for<TSInputTargetLinkStorage>();
    }

    const TSInputTargetLinkStorageAccessOps &target_link_storage_access_for(
        TSTypeKind kind) noexcept
    {
        static const TSInputTargetLinkStorageAccessOps common{
            .get_const = [](const void *memory) noexcept -> const TSInputTargetLinkStorage * {
                return memory != nullptr ? MemoryUtils::cast<TSInputTargetLinkStorage>(memory) : nullptr;
            },
            .get_mutable = [](void *memory) noexcept -> TSInputTargetLinkStorage * {
                return memory != nullptr ? MemoryUtils::cast<TSInputTargetLinkStorage>(memory) : nullptr;
            },
        };
        static const TSInputTargetLinkStorageAccessOps structural{
            .get_const = [](const void *memory) noexcept -> const TSInputTargetLinkStorage * {
                return memory != nullptr
                           ? static_cast<const TSInputTargetLinkStorage *>(
                                 MemoryUtils::cast<TSInputTargetLinkStructuralStorage>(memory))
                           : nullptr;
            },
            .get_mutable = [](void *memory) noexcept -> TSInputTargetLinkStorage * {
                return memory != nullptr
                           ? static_cast<TSInputTargetLinkStorage *>(
                                 MemoryUtils::cast<TSInputTargetLinkStructuralStorage>(memory))
                           : nullptr;
            },
        };
        return kind == TSTypeKind::TSS || kind == TSTypeKind::TSD ? structural : common;
    }

    TSInputTargetActiveNode *TSInputTargetActiveNode::child_at(std::size_t slot_index) const noexcept
    {
        return children.find(slot_index);
    }

    bool TSInputTargetActiveNode::has_any_active() const noexcept
    {
        if (locally_active) { return true; }
        return children.any_of([](std::size_t, const TSInputTargetActiveNode &child) {
            return child.has_any_active();
        });
    }

    DynamicStorageMetrics TSInputTargetActiveNode::dynamic_storage_metrics() const noexcept
    {
        DynamicStorageMetrics result = children.dynamic_storage_metrics();
        children.for_each([&](std::size_t, const TSInputTargetActiveNode &child) {
            result.live_bytes += sizeof(TSInputTargetActiveNode);
            result.reserved_bytes += sizeof(TSInputTargetActiveNode);
            result += child.dynamic_storage_metrics();
        });
        return result;
    }

    TSInputTargetActiveNode &TSInputTargetActiveNode::ensure_child(std::size_t slot_index)
    {
        return children.ensure(slot_index, [&] {
            auto child = std::make_unique<TSInputTargetActiveNode>();
            child->parent = this;
            child->slot = slot_index;
            return child;
        });
    }

    void TSInputTargetActiveNode::clear_observed() noexcept
    {
        observed.reset();
        children.for_each([](std::size_t, TSInputTargetActiveNode &child) {
            child.clear_observed();
        });
    }

    void TSInputTargetLinkState::SchedulingNotifier::set_target(Notifiable *target) noexcept
    {
        target_ = target;
    }

    Notifiable *TSInputTargetLinkState::SchedulingNotifier::target() const noexcept
    {
        return target_;
    }

    void TSInputTargetLinkState::SchedulingNotifier::notify(DateTime modified_time)
    {
        if (target_ != nullptr) { target_->notify(modified_time); }
    }

    TSInputTargetLinkState::TSInputTargetLinkState(TSInputTargetLinkStorage &owner) noexcept
        : owner(&owner)
    {
    }

    TSInputTargetLinkState::~TSInputTargetLinkState() noexcept
    {
        unsubscribe_active_tree();
    }

    void TSInputTargetLinkState::move_from(TSInputTargetLinkState &other) noexcept
    {
        unsubscribe_active_tree();
        target.reset();
        active_root_node.reset();

        scheduling_notifier.set_target(other.scheduling_notifier.target());
        other.scheduling_notifier.set_target(nullptr);

        target = other.target;
        if (target.bound())
        {
            replace_observer(target, &other, this);
            other.target.reset();
        }

        if (other.active_root_node)
        {
            if (scheduling_notifier.target() != nullptr)
            {
                replace_tree_observers(*other.active_root_node, other.scheduling_notifier, scheduling_notifier);
            }
            active_root_node = std::move(other.active_root_node);
        }
    }

    void TSInputTargetLinkState::notify(DateTime modified_time)
    {
        if (owner != nullptr) { owner->record_target_modified(modified_time); }
    }

    void TSInputTargetLinkState::source_invalidated(const TSDataTracking *source) noexcept
    {
        if (owner != nullptr) { owner->source_invalidated(source); }
    }

    TSInputTargetActiveNode *TSInputTargetLinkState::active_root() const noexcept
    {
        return active_root_node.get();
    }

    TSInputTargetActiveNode &TSInputTargetLinkState::ensure_active_root()
    {
        if (!active_root_node) { active_root_node = std::make_unique<TSInputTargetActiveNode>(); }
        return *active_root_node;
    }

    void TSInputTargetLinkState::clear_active_observed() noexcept
    {
        if (active_root_node) { active_root_node->clear_observed(); }
    }

    void TSInputTargetLinkState::unsubscribe_active_tree() noexcept
    {
        if (active_root_node) { unsubscribe_tree(*active_root_node, scheduling_notifier); }
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage() noexcept
        : TSInputTargetLinkStorage(target_link_no_structural_ops())
    {
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(
        const TSInputTargetLinkStructuralOps &structural_ops) noexcept
        : state_(*this),
          structural_ops_(&structural_ops)
    {
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(const TSInputTargetLinkStorage &other)
        : TSInputTargetLinkStorage(other, target_link_no_structural_ops())
    {
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(
        const TSInputTargetLinkStorage &other,
        const TSInputTargetLinkStructuralOps &structural_ops)
        : tracking(other.tracking),
          state_(*this),
          structural_ops_(&structural_ops)
    {
    }

    TSInputTargetLinkStorage &TSInputTargetLinkStorage::operator=(const TSInputTargetLinkStorage &other)
    {
        if (this != &other)
        {
            unbind_noexcept();
            state_.active_root_node.reset();
            state_.scheduling_notifier.set_target(nullptr);
            tracking = other.tracking;
        }
        return *this;
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(TSInputTargetLinkStorage &&other) noexcept
        : TSInputTargetLinkStorage(std::move(other), target_link_no_structural_ops())
    {
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(
        TSInputTargetLinkStorage &&other,
        const TSInputTargetLinkStructuralOps &structural_ops) noexcept
        : tracking(std::move(other.tracking)),
          state_(*this),
          structural_ops_(&structural_ops)
    {
        state_.move_from(other.state_);
    }

    TSInputTargetLinkStorage &TSInputTargetLinkStorage::operator=(TSInputTargetLinkStorage &&other) noexcept
    {
        if (this != &other)
        {
            unbind_noexcept();
            state_.active_root_node.reset();
            state_.scheduling_notifier.set_target(nullptr);
            // Published target links are stable and are not move-assigned.
            // Preserve any observers of this destination identity; observers
            // of the moved-from identity see that source disappear.
            other.structural_ops_->before_move_assignment_source(other);
            tracking = std::move(other.tracking);
            state_.move_from(other.state_);
            structural_ops_->resubscribe_after_move_assignment(*this);
        }
        return *this;
    }

    TSInputTargetLinkStorage::~TSInputTargetLinkStorage() noexcept
    {
        unbind_noexcept();
    }

    void TSInputTargetLinkStorage::set_structural_ops(
        const TSInputTargetLinkStructuralOps &structural_ops) noexcept
    {
        structural_ops_ = &structural_ops;
    }

    TSInputTargetLinkStructuralStorage::TSInputTargetLinkStructuralStorage() noexcept
        : TSInputTargetLinkStorage(target_link_structural_ops())
    {
    }

    TSInputTargetLinkStructuralStorage::TSInputTargetLinkStructuralStorage(
        const TSInputTargetLinkStructuralStorage &other)
        : TSInputTargetLinkStorage(other, target_link_structural_ops())
    {
    }

    TSInputTargetLinkStructuralStorage &TSInputTargetLinkStructuralStorage::operator=(
        const TSInputTargetLinkStructuralStorage &other)
    {
        TSInputTargetLinkStorage::operator=(other);
        return *this;
    }

    TSInputTargetLinkStructuralStorage::TSInputTargetLinkStructuralStorage(
        TSInputTargetLinkStructuralStorage &&other) noexcept
        : TSInputTargetLinkStorage(std::move(other), target_link_structural_ops()),
          slot_observers(std::move(other.slot_observers)),
          slot_observers_subscribed(std::exchange(other.slot_observers_subscribed, false)),
          structural_transition(std::move(other.structural_transition))
    {
        other.slot_observers.clear();
        if (structural_transition) { structural_transition->rebind_owner(*this); }
    }

    TSInputTargetLinkStructuralStorage &TSInputTargetLinkStructuralStorage::operator=(
        TSInputTargetLinkStructuralStorage &&other) noexcept
    {
        if (this != &other)
        {
            TSInputTargetLinkStorage::operator=(std::move(other));
            structural_transition = std::move(other.structural_transition);
            if (structural_transition) { structural_transition->rebind_owner(*this); }
        }
        return *this;
    }

    TSInputTargetLinkStructuralStorage::~TSInputTargetLinkStructuralStorage() noexcept
    {
        unbind_noexcept();
        set_structural_ops(target_link_no_structural_ops());
    }

    bool TSInputTargetLinkStorage::bound() const noexcept
    {
        return target_output().bound();
    }

    void TSInputTargetLinkStorage::bind(const TSValueTypeMetaData &schema, const TSOutputView &output)
    {
        bind_impl(schema, output, MIN_DT, false, true);
    }

    void TSInputTargetLinkStorage::bind_current_value(const TSValueTypeMetaData &schema,
                                                      const TSOutputView &output,
                                                      DateTime modified_time)
    {
        if (modified_time == MIN_DT)
        {
            throw std::invalid_argument("Current-value TSInput target binding requires an evaluation time");
        }
        bind_impl(schema, output, MIN_DT, false, false);
        if (output.data_view().has_current_value()) { record_target_modified(modified_time); }
    }

    void TSInputTargetLinkStorage::bind_sampled(const TSValueTypeMetaData &schema,
                                                const TSOutputView &output,
                                                DateTime modified_time)
    {
        if (modified_time == MIN_DT)
        {
            throw std::invalid_argument("Sampled TSInput target binding requires an evaluation time");
        }
        bind_impl(schema, output, modified_time, true, false);
    }

    void TSInputTargetLinkStorage::bind_impl(const TSValueTypeMetaData &schema,
                                             const TSOutputView &output,
                                             DateTime modified_time,
                                             bool sampled,
                                             bool replay_source_time)
    {
        if (!output_view_bound(output))
        {
            throw std::invalid_argument("TSInput target binding requires a bound output view");
        }

        const bool closed_union_narrowing = is_closed_union_narrowing(schema, output.schema());
        const bool signal_from_reference =
            schema.kind == TSTypeKind::SIGNAL && output.schema() != nullptr &&
            output.schema()->kind == TSTypeKind::REF;
        // SIGNAL accepts every concrete time-series shape, but a REF source is
        // transparent at an input boundary: observe the referenced output's
        // ticks, not changes to the reference token itself.
        auto target = (schema.kind == TSTypeKind::SIGNAL && !signal_from_reference) ||
                              closed_union_narrowing
                          ? output.handle()
                          : output.binding_for(schema);
        if (schema.kind != TSTypeKind::SIGNAL && !closed_union_narrowing &&
            !time_series_schema_equivalent(target.schema(), &schema))
        {
            throw std::invalid_argument("TSInput target binding schema does not match the input slot schema");
        }

        const bool structural = structural_ops_->supports_structural;
        const bool previous_was_valid =
            sampled && state_.target.bound() && state_.target.view(modified_time).valid();
        const bool previous_has_published_state =
            sampled && structural && state_.target.bound() &&
            has_published_structural_state(state_.target.data_view(), modified_time);
        if (state_.target.bound()) { detach_target(sampled && structural, modified_time); }
        else if (!sampled || structural_transition_time() != modified_time)
        {
            structural_ops_->clear_transition(*this);
        }

        auto &state = state_;
        state.target = target;
        auto rollback = make_scope_exit<true>([this] { unbind(); });
        state.target.data_view().subscribe(&state);
        // A sampled rebind represents the source's current value at
        // modified_time. Replaying the source's historical timestamp first
        // can move a parent's delta clock backwards and erase sibling changes
        // already recorded for this cycle.
        if (replay_source_time && state.target.data_view().last_modified_time() != MIN_DT)
        {
            record_target_modified(state.target.data_view().last_modified_time());
        }
        structural_ops_->subscribe_key_set_tracking(*this);
        structural_ops_->subscribe_slot_observers(*this);
        resubscribe_active_target(schema);
        const bool publish_sampled_transition =
            sampled && (output.valid() || previous_was_valid || previous_has_published_state);
        if (publish_sampled_transition)
        {
            structural_ops_->publish_sampled_transition(*this, modified_time);
            record_target_modified(modified_time);
        }
        else if (sampled) { structural_ops_->clear_transition(*this); }
        rollback.release();
    }

    void TSInputTargetLinkStorage::unbind()
    {
        detach_target(false, MIN_DT);
    }

    void TSInputTargetLinkStorage::unbind_structural(DateTime modified_time)
    {
        if (modified_time == MIN_DT)
        {
            throw std::invalid_argument("Structural TSInput target unbinding requires an evaluation time");
        }
        if (!state_.target.bound()) { return; }
        const bool has_published_state =
            has_published_structural_state(state_.target.data_view(), modified_time);
        detach_target(has_published_state, modified_time);
        if (!has_published_state) { return; }
        record_key_set_modified(modified_time);
        record_target_modified(modified_time);
    }

    void TSInputTargetLinkStorage::detach_target(bool retain_structural_target, DateTime modified_time)
    {
        structural_ops_->detach_target(*this, retain_structural_target, modified_time);
        unsubscribe_active_target();
        if (state_.target.bound()) { state_.target.data_view().unsubscribe(&state_); }
        state_.target.reset();
    }

    void TSInputTargetLinkStorage::unbind_noexcept() noexcept
    {
        structural_ops_->unbind_noexcept(*this);
        if (state_.active_root_node) { unsubscribe_tree_noexcept(*state_.active_root_node, state_.scheduling_notifier); }
        unsubscribe_handle_noexcept(state_.target, &state_);
    }

    void TSInputTargetLinkStorage::source_invalidated(const TSDataTracking *source) noexcept
    {
        static_cast<void>(source);
        state_.clear_active_observed();
        state_.target.reset();
        structural_ops_->source_invalidated(*this);
    }

    void TSInputTargetLinkStorage::add_slot_observer(SlotObserver *observer)
    {
        structural_ops_->add_slot_observer(*this, observer);
    }

    void TSInputTargetLinkStorage::remove_slot_observer(SlotObserver *observer)
    {
        structural_ops_->remove_slot_observer(*this, observer);
    }

    void TSInputTargetLinkStorage::record_target_modified(DateTime modified_time)
    {
        if (!tracking.record_modified(modified_time)) { return; }
        tracking.parent.notify_child_modified(modified_time);
    }

    const TSDataTracking &TSInputTargetLinkStorage::key_set_tracking() const
    {
        auto &self = *const_cast<TSInputTargetLinkStorage *>(this);
        return structural_ops_->key_set_tracking(self);
    }

    TSDataTracking &TSInputTargetLinkStorage::mutable_key_set_tracking()
    {
        return structural_ops_->mutable_key_set_tracking(*this);
    }

    void TSInputTargetLinkStorage::record_key_set_modified(DateTime modified_time)
    {
        structural_ops_->record_key_set_modified(*this, modified_time);
    }

    void TSInputTargetLinkStorage::key_set_source_invalidated(const TSDataTracking *source) noexcept
    {
        structural_ops_->key_set_source_invalidated(*this, source);
    }

    TSInputTargetActiveNode &TSInputTargetLinkStorage::root_node()
    {
        return state_.ensure_active_root();
    }

    TSInputTargetActiveNode &TSInputTargetLinkStorage::child_node(TSInputTargetActiveNode *parent, std::size_t slot)
    {
        return (parent != nullptr ? *parent : root_node()).ensure_child(slot);
    }

    void TSInputTargetLinkStorage::make_active(TSInputTargetActiveNode *node,
                                               const TSDataView &observed,
                                               TSInputObservationKind observation_kind,
                                               Notifiable *target_notifier)
    {
        auto &state = state_;
        state.scheduling_notifier.set_target(target_notifier);

        auto &active_node = node != nullptr ? *node : state.ensure_active_root();
        const auto observed_handle = observed.valid()
                                         ? TSOutputHandle{state.target.output(), observed.borrowed_ref()}
                                         : TSOutputHandle{};
        if (active_node.locally_active && active_node.observation_kind == observation_kind &&
            active_node.observed.same_as(observed_handle))
        {
            return;
        }

        if (active_node.locally_active) { unsubscribe_node(active_node, state.scheduling_notifier); }
        active_node.locally_active = true;
        active_node.observation_kind = observation_kind;
        active_node.observed = observed_handle;
        if (active_node.observed.bound() && target_notifier != nullptr)
        {
            active_node.observed.data_view().subscribe(&state.scheduling_notifier);
        }
    }

    void TSInputTargetLinkStorage::make_passive(TSInputTargetActiveNode *node)
    {
        if (node == nullptr) { node = state_.active_root(); }
        if (node == nullptr || !node->locally_active) { return; }

        unsubscribe_node(*node, state_.scheduling_notifier);
        node->locally_active = false;
    }

    bool TSInputTargetLinkStorage::active(const TSInputTargetActiveNode *node) const noexcept
    {
        if (node == nullptr) { node = state_.active_root(); }
        return node != nullptr && node->locally_active;
    }

    TSOutputHandle TSInputTargetLinkStorage::target_output_at_path(const TSValueTypeMetaData &schema,
                                                                   const TSInputTargetActiveNode *node) const
    {
        if (!bound()) { return {}; }
        TSDataView current = target_view();
        const auto *current_schema = &schema;
        if (!project_target_path(current, current_schema, node)) { return {}; }
        return TSOutputHandle{target_output().output(), current};
    }

    TSOutputHandle TSInputTargetLinkStorage::resolved_target_at_path(
        const TSValueTypeMetaData &schema,
        const TSInputTargetActiveNode *node) const
    {
        const auto *active_node = node != nullptr ? node : state_.active_root();
        if (active_node != nullptr && active_node->locally_active &&
            active_node->observation_kind == TSInputObservationKind::Value &&
            active_node->observed.bound())
        {
            return active_node->observed;
        }
        return target_output_at_path(schema, node);
    }

    void TSInputTargetLinkStorage::resubscribe_active_target(const TSValueTypeMetaData &schema)
    {
        if (state_.active_root() == nullptr) { return; }
        resubscribe_tree(*this, schema, *state_.active_root());
    }

    void TSInputTargetLinkStorage::unsubscribe_active_target() noexcept
    {
        state_.unsubscribe_active_tree();
    }

    TSDataView TSInputTargetLinkStorage::target_view() const noexcept
    {
        return target_output().data_view();
    }

    TSDataView TSInputTargetLinkStorage::previous_target_view() const noexcept
    {
        return structural_ops_->previous_target_view(*this);
    }

    const TSOutputHandle &TSInputTargetLinkStorage::target_output() const noexcept
    {
        return state_.target;
    }

    bool TSInputTargetLinkStorage::structural_transition_active() const noexcept
    {
        return structural_ops_->transition_active(*this);
    }

    bool TSInputTargetLinkStorage::sampled_structural_transition() const noexcept
    {
        return structural_ops_->sampled_transition(*this);
    }

    DateTime TSInputTargetLinkStorage::structural_transition_time() const noexcept
    {
        return structural_ops_->transition_time(*this);
    }

    const TSInputTargetLinkState *TSInputTargetLinkStorage::state() const noexcept
    {
        return &state_;
    }

    DynamicStorageMetrics TSInputTargetLinkStorage::dynamic_storage_metrics() const noexcept
    {
        DynamicStorageMetrics result = structural_ops_->dynamic_storage_metrics(*this);
        if (state_.active_root_node != nullptr)
        {
            result.live_bytes += sizeof(TSInputTargetActiveNode);
            result.reserved_bytes += sizeof(TSInputTargetActiveNode);
            result += state_.active_root_node->dynamic_storage_metrics();
        }
        return result;
    }

    bool is_target_link_view(const TSDataView &view) noexcept
    {
        return target_link_context_for_ops(view.storage_type().ops()) != nullptr && view.data() != nullptr;
    }

    bool target_link_bound(const TSDataView &view) noexcept
    {
        const auto *link = target_link_storage(view);
        return link != nullptr && link->bound();
    }

    TSDataView target_link_resolve(const TSDataView &view, const TSInputTargetActiveNode *node) noexcept
    {
        const auto *schema = target_link_schema(view);
        const auto *link = target_link_storage(view);
        if (schema == nullptr || link == nullptr || !link->bound()) { return {}; }
        return fallback_on_exception(TSDataView{}, [&] {
            return link->resolved_target_at_path(*schema, node).data_view();
        });
    }

    const TSValueTypeMetaData *target_path_schema(const TSDataView &target_link,
                                                  const TSInputTargetActiveNode *node) noexcept
    {
        const TSValueTypeMetaData *current = target_link_schema(target_link);
        return target_schema_at_node(current, node);
    }

    TSInputTargetActiveNode *target_link_child_node(const TSDataView &view,
                                                    TSInputTargetActiveNode *parent,
                                                    std::size_t slot)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target navigation requires TargetLink storage"); }
        return &link->child_node(parent, slot);
    }

    void bind_target_link(const TSDataView &view, const TSOutputView &output)
    {
        auto *link = mutable_target_link_storage(view);
        const auto *schema = target_link_schema(view);
        if (link == nullptr || schema == nullptr)
        {
            throw std::logic_error("TSInput target binding requires TargetLink storage");
        }
        link->bind(*schema, output);
    }

    void bind_target_link_sampled(const TSDataView &view, const TSOutputView &output,
                                  DateTime modified_time)
    {
        auto *link = mutable_target_link_storage(view);
        const auto *schema = target_link_schema(view);
        if (link == nullptr || schema == nullptr)
        {
            throw std::logic_error("Sampled TSInput target binding requires TargetLink storage");
        }
        link->bind_sampled(*schema, output, modified_time);
    }

    void unbind_target_link(const TSDataView &view)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target unbinding requires TargetLink storage"); }
        link->unbind();
    }

    void make_target_link_active(const TSDataView &view,
                                 TSInputTargetActiveNode *node,
                                 const TSDataView &observed,
                                 TSInputObservationKind observation_kind,
                                 Notifiable *target_notifier)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target activation requires TargetLink storage"); }
        link->make_active(node, observed, observation_kind, target_notifier);
    }

    void make_target_link_passive(const TSDataView &view, TSInputTargetActiveNode *node)
    {
        auto *link = mutable_target_link_storage(view);
        if (link != nullptr) { link->make_passive(node); }
    }

    bool target_link_active(const TSDataView &view, const TSInputTargetActiveNode *node) noexcept
    {
        const auto *link = target_link_storage(view);
        return link != nullptr && link->active(node);
    }
}  // namespace hgraph::detail
