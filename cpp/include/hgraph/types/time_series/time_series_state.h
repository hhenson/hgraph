#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>

namespace hgraph
{

    struct TSOutput;
    struct TSState;
    struct TSLState;
    struct TSDState;
    struct TSBState;
    struct TSSState;
    struct TSWState;
    struct TargetLinkState;
    struct RefLinkState;
    struct SignalState;
    struct BaseState;

    /**
     * Value variant covering the concrete time-series state structs currently
     * used in the struct model.
     */
    using TimeSeriesStateV =
        std::variant<TSState, TSLState, TSDState, TSBState, TSSState, TSWState, TargetLinkState, RefLinkState, SignalState>;

    /**
     * Pointer variant covering all concrete leaf state types.
     */
    using TimeSeriesLeafStatePtr = std::variant<TSState *, TSLState *, TSDState *, TSBState *, TSSState *, TSWState *>;

    /**
     * Pointer variant covering all concrete time-series state types.
     */
    using TimeSeriesStatePtr = std::variant<TSState *, TSLState *, TSDState *, TSBState *, TSSState *, TSWState *,
                                            TargetLinkState *, RefLinkState *, SignalState *>;

    /**
     * Pointer variant covering all concrete time-series state types plus the
     * root output endpoint.
     */
    using TimeSeriesStateParentPtr = std::variant<TSLState *, TSDState *, TSBState *, TSOutput *>;

    struct HGRAPH_EXPORT BaseState
    {
        /**
         * This identifies the parent container and is used to propagate notifications and updated time to the parent.
         * If null this represents a parent container.
         * `TSOutput*` indicates the root output endpoint for a state tree.
         */
        TimeSeriesStateParentPtr parent;
        /**
         * The index of this state within the context of its parent.
         * If this is the root, then the index represents the port within the context of the
         * value's node.
         */
        size_t                           index;
        engine_time_t                    last_modified_time;
        std::unordered_set<Notifiable *> subscribers;

        /**
         * Register a subscriber for direct modification notifications from
         * this state node.
         */
        void subscribe(Notifiable *subscriber) noexcept;

        /**
         * Remove a subscriber from direct modification notifications from
         * this state node.
         */
        void unsubscribe(Notifiable *subscriber) noexcept;

        /**
         * Mark this state as modified for the supplied engine time, notify any
         * direct subscribers, and propagate that change to its parent.
         *
         * This is intended to be called by leaf state types when they become
         * modified. The current struct model does not enforce that restriction.
         */
        void mark_modified(engine_time_t modified_time) noexcept;

        /**
         * Notify the parent collection that one of its children has been
         * modified.
         *
         * Parent collection states receive the notification through
         * `child_modified()`, with this state's parent-relative index.
         * If the parent is rooted directly at a `TSOutput`, output propagation
         * will be added when the output-side state contract is defined.
         */
        void notify_parent_that_child_is_modified(engine_time_t modified_time) noexcept;
    };

    /**
     * State carried by a scalar time-series.
     */
    struct HGRAPH_EXPORT TSState : BaseState
    {};

    struct HGRAPH_EXPORT BaseCollectionState : BaseState
    {
        /**
         * Child states are indexed based on the value indexing.
         * For TSL/TSB this is a state pre-computed-sized list.
         * For TSD this is a dynamically sized list where the entries match the entries in the data.
         */
        std::vector<TimeSeriesStatePtr> child_states;
        /**
         * Used to build the delta value. This is a short-cut to identify which items have been modified in the current
         * engine cycle and is reset when the modification time advances.
         */
        std::unordered_set<size_t> modified_children;

        /**
         * Record the modified child and continue propagating the modification.
         */
        void child_modified(size_t child_index, engine_time_t modified_time) noexcept;
    };

    /**
     * State carried by a time-series list.
     */
    struct HGRAPH_EXPORT TSLState : BaseCollectionState
    {};

    /**
     * State carried by a time-series dictionary.
     */
    struct HGRAPH_EXPORT TSDState : BaseCollectionState
    {};

    /**
     * State carried by a time-series bundle.
     */
    struct HGRAPH_EXPORT TSBState : BaseCollectionState
    {};

    /**
     * State carried by a time-series set.
     */
    struct HGRAPH_EXPORT TSSState : BaseState
    {
        /**
         * Indices of items added in the current engine cycle.
         */
        std::unordered_set<size_t> added_items;

        /**
         * Indices of items removed in the current engine cycle.
         */
        std::unordered_set<size_t> removed_items;

        /**
         * Record that the item at the supplied index was added in the current
         * engine cycle.
         */
        void mark_added(size_t item_index, engine_time_t modified_time) noexcept;

        /**
         * Record that the item at the supplied index was removed in the current
         * engine cycle.
         */
        void mark_removed(size_t item_index, engine_time_t modified_time) noexcept;

      private:
        void reset_change_sets_if_time_changed(engine_time_t modified_time) noexcept;
    };

    /**
     * State carried by a time-series window.
     */
    struct HGRAPH_EXPORT TSWState : BaseState
    {};

    /**
     * State carried by a target-link leaf time-series.
     * This contains information describing how the target is linked.
     */
    struct HGRAPH_EXPORT TargetLinkState : BaseState
    {
        struct TargetLinkStateNotifiable : Notifiable
        {
            explicit TargetLinkStateNotifiable(TargetLinkState *self) noexcept;

            void notify(engine_time_t modified_time) override;

            TargetLinkState *self;
        };

        TargetLinkState() noexcept;

        /**
         * Bind this link state to the supplied target state and register for
         * modification notifications from that target.
         */
        void set_target(TimeSeriesLeafStatePtr target_state) noexcept;

        /**
         * Remove any bound target and unregister from its modification
         * notifications.
         */
        void reset_target() noexcept;

        TimeSeriesLeafStatePtr    target;
        TargetLinkStateNotifiable target_notifiable;

        [[nodiscard]] bool is_bound() const noexcept;

      private:
        void register_with_target() noexcept;
        void unregister_from_target() noexcept;
    };

    /**
     * State carried by a reference-link leaf time-series.
     * This points to two values, namely the target and the referenced link
     */
    struct HGRAPH_EXPORT RefLinkState : BaseState
    {
        [[nodiscard]] engine_time_t last_target_modified_time() const;
        [[nodiscard]] bool          is_sampled() const;
        TargetLinkState             bound_link;
    };

    /**
     * State carried by a signal leaf time-series.
     */
    struct HGRAPH_EXPORT SignalState : BaseState
    {};

}  // namespace hgraph
