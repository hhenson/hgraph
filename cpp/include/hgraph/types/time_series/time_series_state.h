#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/tagged_ptr.h>

#include <memory>
#include <vector>

namespace hgraph
{

    struct TSMeta;
    struct TSInput;
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
    struct TSContext;
    using LinkedTSContext = TSContext;

    namespace detail
    {
        struct TSDispatch;
        struct ViewDispatch;
    }  // namespace detail

    /**
     * Value variant covering the concrete time-series state structs currently
     * used in the struct model.
     */
    using TimeSeriesStateV =
        std::variant<TSState, TSLState, TSDState, TSBState, TSSState, TSWState, TargetLinkState, RefLinkState, SignalState>;

    /**
     * Pointer variant covering all concrete time-series state types.
     */
    using TimeSeriesStatePtr = std::variant<TSState *, TSLState *, TSDState *, TSBState *, TSSState *, TSWState *,
                                            TargetLinkState *, RefLinkState *, SignalState *>;

    /**
     * Pointer variant covering all concrete time-series state types plus the
     * root output endpoint.
     */
    using TimeSeriesStateParentPtr = pointer_aligned_discriminated_ptr<TSLState, TSDState, TSBState, TSInput, TSOutput>;

    /**
     * Identifies the storage backend carried by a logical TS state node.
     *
     * The represented TS shape comes from schema and dispatch. This marker
     * describes how that logical position is backed at runtime so views can
     * distinguish native storage from link-backed storage.
     */
    enum class TSStorageKind : uint8_t
    {
        Native,
        TargetLink,
        RefLink,
    };

    struct HGRAPH_EXPORT BaseState
    {
        /**
         * This identifies the parent container and is used to propagate notifications and updated time to the parent.
         * This will either be a Parent state (i.e. TSL, TSD, TSB) or a container i.e. TSInput/TSOutput.
         */
        TimeSeriesStateParentPtr parent;
        /**
         * The index (or slot in the case of a TSD) of this state within the context of its parent.
         * If this is the root, then the index represents the port within the context of the
         * value's node.
         */
        size_t                           index;
        engine_time_t                    last_modified_time;
        TSStorageKind                    storage_kind{TSStorageKind::Native};
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
         * Return the bound target context when this state is link-backed.
         *
         * Native storage positions return `nullptr`.
         */
        [[nodiscard]] LinkedTSContext *linked_target() noexcept;
        [[nodiscard]] const LinkedTSContext *linked_target() const noexcept;

        /**
         * Return the runtime state node that represents this logical position.
         *
         * Native storage resolves to `this`. Link-backed storage resolves to
         * the currently bound target state when bound, otherwise `nullptr`.
         */
        [[nodiscard]] BaseState *resolved_state() noexcept;
        [[nodiscard]] const BaseState *resolved_state() const noexcept;

        /**
         * Return the notifier boundary to use when traversing into this child.
         *
         * Native storage inherits the supplied fallback notifier. Link-backed
         * storage exposes the scheduling notifier associated with the binding
         * boundary.
         */
        [[nodiscard]] Notifiable *boundary_notifier(Notifiable *fallback) noexcept;

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

    /**
     * Shared raw context for one logical time-series position.
     *
     * This carries the schema-shaped metadata and raw storage pointers needed
     * to represent a logical TS position independent of how that position is
     * being used. View code layers navigation/value materialization on top of
     * it, while link state uses the same carrier as a bound-target handle.
     */
    struct HGRAPH_EXPORT TSContext
    {
        constexpr TSContext() noexcept = default;

        constexpr TSContext(const TSMeta *schema_,
                            const detail::ViewDispatch *value_dispatch_,
                            const detail::TSDispatch *ts_dispatch_,
                            void *value_data_,
                            BaseState *ts_state_) noexcept
            : schema(schema_),
              value_dispatch(value_dispatch_),
              ts_dispatch(ts_dispatch_),
              value_data(value_data_),
              ts_state(ts_state_)
        {
        }

        [[nodiscard]] static constexpr TSContext none() noexcept { return {}; }

        [[nodiscard]] bool is_bound() const noexcept
        {
            return schema != nullptr && value_dispatch != nullptr && ts_dispatch != nullptr && ts_state != nullptr;
        }

        void clear() noexcept { *this = {}; }

        const TSMeta               *schema{nullptr};
        const detail::ViewDispatch *value_dispatch{nullptr};
        const detail::TSDispatch   *ts_dispatch{nullptr};
        void                       *value_data{nullptr};
        BaseState                  *ts_state{nullptr};
    };

    /**
     * Bound logical TS position used by link-backed storage nodes.
     *
     * This captures the represented TS shape separately from the storage node
     * that owns the binding. A target/reference link can therefore present the
     * delegate for the shape it represents while keeping its own storage for
     * binding and notification mechanics.
     */
    struct HGRAPH_EXPORT BaseCollectionState : BaseState
    {
        BaseCollectionState() = default;
        ~BaseCollectionState();

        BaseCollectionState(BaseCollectionState &&other) noexcept;
        BaseCollectionState &operator=(BaseCollectionState &&other) noexcept;

        BaseCollectionState(const BaseCollectionState &) = delete;
        BaseCollectionState &operator=(const BaseCollectionState &) = delete;

        /**
         * Child states are indexed based on the value indexing.
         * For `TSL`/`TSB` this is a schema-shaped list.
         * For `TSD` this is indexed by the value layer's stable slot ids.
         *
         * Collections own their child nodes uniformly. A child node may be a
         * native schema-backed state node or a link-backed state node, but the
         * collection still owns that node object. External linkage is handled
         * inside the child state itself rather than by storing borrowed child
         * pointers here.
         */
        std::vector<std::unique_ptr<TimeSeriesStateV>> child_states;
        /**
         * Used to build the delta value. This is a short-cut to identify which items have been modified in the current
         * engine cycle and is reset when the modification time advances.
         */
        std::unordered_set<size_t> modified_children;

        void reset_child_states() noexcept;

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
    {
        /**
         * Last observed key hash for each stable slot.
         *
         * This is used to detect slot reuse so the corresponding child state
         * can be reset when a removed slot later holds a different key.
         */
        std::vector<size_t> slot_key_hashes;
    };

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
     * Storage carried by a target-linked logical time-series position.
     *
     * The represented TS shape is described by the bound target context. This
     * state owns only the binding/subscription mechanics for that position.
     */
    struct HGRAPH_EXPORT TargetLinkState : BaseState
    {
        /**
         * Notification path used by the target link state itself.
         *
         * This notifier is registered against the bound output-side state so
         * changes flowing through the target link mark this state modified and
         * continue propagation through the non-peered collections above it via
         * `child_modified()`.
         */
        struct TargetLinkStateNotifiable : Notifiable
        {
            explicit TargetLinkStateNotifiable(TargetLinkState *self) noexcept;

            void notify(engine_time_t modified_time) override;

            TargetLinkState *self;
        };

        /**
         * Notification path used only for scheduling the owning node.
         *
         * This notifier does not mark the target link state modified. It is
         * intended to forward scheduling notifications to the node-side
         * notifier associated to the input branch that crosses this target
         * link.
         *
         * The split between this notifier and `target_notifiable` is
         * intentional. One path is responsible for marking the target link
         * modified and propagating through non-peered parents, while this path
         * is responsible only for scheduling the owning node.
         */
        struct SchedulingNotifier : Notifiable
        {
            SchedulingNotifier() = default;

            void set_target(Notifiable *target_) noexcept { target = target_; }
            [[nodiscard]] Notifiable *get_target() const noexcept { return target; }

            void notify(engine_time_t modified_time) override;

          private:
            Notifiable *target{nullptr};
        };

        TargetLinkState() noexcept;
        ~TargetLinkState();

        TargetLinkState(TargetLinkState &&other) noexcept;
        TargetLinkState &operator=(TargetLinkState &&other) noexcept;

        TargetLinkState(const TargetLinkState &) = delete;
        TargetLinkState &operator=(const TargetLinkState &) = delete;

        /**
         * Bind this link state to the supplied target context and register for
         * modification notifications from that represented TS position.
         */
        void set_target(LinkedTSContext target_state) noexcept;

        /**
         * Remove any bound target and unregister from its modification
         * notifications.
         */
        void reset_target() noexcept;

        LinkedTSContext target;
        /**
         * Notification identity used to keep the target link state and the
         * non-peered collections above it up to date.
         */
        TargetLinkStateNotifiable target_notifiable;
        /**
         * Notification identity used only to schedule the owning node for
         * input branches below this target link.
         */
        SchedulingNotifier       scheduling_notifier;

        [[nodiscard]] bool is_bound() const noexcept;

      private:
        void register_with_target() noexcept;
        void unregister_from_target() noexcept;
    };

    /**
     * Storage carried by a reference-linked logical time-series position.
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
