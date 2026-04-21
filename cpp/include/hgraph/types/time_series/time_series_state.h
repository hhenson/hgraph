#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/value/slot_observer.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/tagged_ptr.h>

#include <deque>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

namespace hgraph
{
    struct ActiveTrieNode;

    struct TSMeta;
    struct TSInput;
    struct TSOutput;
    struct Node;
    struct TimeSeriesFeatureRegistry;
    struct TSState;
    struct TSLState;
    struct TSDState;
    struct TSBState;
    struct TSSState;
    struct TSWState;
    struct OutputLinkState;
    struct TargetLinkState;
    struct RefLinkState;
    struct SignalState;
    struct BaseState;
    struct TSContext;
    struct TSViewContext;
    using LinkedTSContext = TSContext;

    namespace detail
    {
        struct MapViewDispatch;
        struct TSDispatch;
        struct TSOutputViewOps;
        struct ViewDispatch;

        struct LinkTransitionSnapshot
        {
            const Value *previous_value{nullptr};
            engine_time_t modified_time{MIN_DT};

            [[nodiscard]] bool active() const noexcept
            {
                return previous_value != nullptr && modified_time != MIN_DT;
            }
        };

        [[nodiscard]] HGRAPH_EXPORT bool has_local_reference_binding(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT bool linked_context_valid(const LinkedTSContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT bool linked_context_all_valid(const LinkedTSContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT LinkedTSContext dereferenced_target_from_source(const LinkedTSContext &source) noexcept;
        [[nodiscard]] HGRAPH_EXPORT const Value *materialized_target_link_value(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT const Value *materialized_reference_value(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT bool reference_all_valid(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT bool linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept;
        [[nodiscard]] HGRAPH_EXPORT LinkTransitionSnapshot transition_snapshot(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT TSViewContext refresh_native_context(const TSViewContext &context) noexcept;
        [[nodiscard]] HGRAPH_EXPORT Value snapshot_target_value(const LinkedTSContext &target,
                                                               engine_time_t modified_time = MIN_DT);
        [[nodiscard]] HGRAPH_EXPORT Value empty_target_value(const LinkedTSContext &target);
    }  // namespace detail

    struct HGRAPH_EXPORT PendingDictChildContext
    {
        const TSMeta                  *parent_schema{nullptr};
        const detail::ViewDispatch    *parent_value_dispatch{nullptr};
        const detail::TSDispatch      *parent_ts_dispatch{nullptr};
        void                          *parent_value_data{nullptr};
        BaseState                     *parent_ts_state{nullptr};
        TSOutput                      *parent_owning_output{nullptr};
        const detail::TSOutputViewOps *parent_output_view_ops{nullptr};
        BaseState                     *parent_notification_state{nullptr};
        Value                          key{};

        [[nodiscard]] bool active() const noexcept
        {
            return parent_schema != nullptr && parent_value_dispatch != nullptr && parent_ts_dispatch != nullptr &&
                   key.has_value();
        }
    };

    /**
     * Value variant covering the concrete time-series state structs currently
     * used in the struct model.
     */
    using TimeSeriesStateV =
        std::variant<TSState, TSLState, TSDState, TSBState, TSSState, TSWState, OutputLinkState, TargetLinkState, RefLinkState,
                     SignalState>;

    /**
     * Pointer variant covering all concrete time-series state types.
     */
    using TimeSeriesStatePtr = std::variant<TSState *, TSLState *, TSDState *, TSBState *, TSSState *, TSWState *,
                                            OutputLinkState *, TargetLinkState *, RefLinkState *, SignalState *>;

    /**
     * Pointer variant covering all concrete time-series state types plus the
     * root output endpoint.
     */
    using TimeSeriesStateParentPtr =
        pointer_aligned_discriminated_ptr<TSLState, TSDState, TSBState, SignalState, Node, TSInput, TSOutput>;

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
        OutputLink,
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
        std::unique_ptr<TimeSeriesFeatureRegistry> feature_registry;

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
         * Return the notifier identity to use when traversing into this child.
         *
         * Native storage inherits the supplied fallback notifier (passthrough).
         * Link-backed storage switches to a boundary-local SchedulingNotifier
         * and wires its forwarding target to @p fallback.
         *
         * This identity switch is required because multiple TargetLinkStates
         * under the same non-peered collection may ultimately schedule the
         * same Node. Using the Node* directly as the subscriber identity in
         * all of them would collapse to a single entry in the subscriber set
         * (std::unordered_set<Notifiable*>), making independent
         * subscribe/unsubscribe impossible. Each TargetLinkState's
         * SchedulingNotifier provides a unique Notifiable address that
         * forwards to the Node through its target pointer. RefLinkState uses
         * the same pattern, but keyed per upstream boundary so multiple
         * dereferenced consumers can coexist under one RefLink.
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
         * Mark this state as modified for the supplied engine time and notify
         * direct subscribers without propagating to the parent collection.
         *
         * This is used when the owning collection is already publishing its
         * own structural tick, but dependent link-backed children still need
         * to see the leaf/local position update.
         */
        void mark_modified_local(engine_time_t modified_time) noexcept;

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
     * Lazy per-state registry for derived runtime surfaces.
     *
     * Feature outputs that logically belong to one TSS/TSD position should be
     * owned by that position's state rather than by an outer output wrapper.
     */
    struct HGRAPH_EXPORT TimeSeriesFeatureRegistry
    {
        virtual ~TimeSeriesFeatureRegistry();
        virtual void rebind_link_source(const LinkedTSContext *source_context, engine_time_t evaluation_time);
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
                            BaseState *ts_state_,
                            TSOutput *owning_output_ = nullptr,
                            const detail::TSOutputViewOps *output_view_ops_ = nullptr,
                            BaseState *notification_state_ = nullptr,
                            PendingDictChildContext pending_dict_child_ = {}) noexcept
            : schema(schema_),
              value_dispatch(value_dispatch_),
              ts_dispatch(ts_dispatch_),
              value_data(value_data_),
              ts_state(ts_state_),
              owning_output(owning_output_),
              output_view_ops(output_view_ops_),
              notification_state(notification_state_ != nullptr ? notification_state_ : ts_state_),
              pending_dict_child(std::move(pending_dict_child_))
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
        TSOutput                   *owning_output{nullptr};
        const detail::TSOutputViewOps *output_view_ops{nullptr};
        /**
         * Runtime state used for direct notification/subscription wiring.
         *
         * This is usually the same as `ts_state`. Alternative output views
         * can override it when the navigable view is local but wakeups must
         * follow some upstream source/root instead.
         */
        BaseState                  *notification_state{nullptr};
        PendingDictChildContext     pending_dict_child{};
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
        /**
         * When true, child updates only promote the collection once, preserving
         * opaque REF[...] semantics while still keeping direct child links live.
         */
        bool suppress_repeated_child_notifications{false};
        /**
         * Scratch storage used when a logical REF[TSL/TSB] position is backed
         * by a local child-binding tree instead of a direct peer target.
         */
        mutable std::optional<Value> materialized_reference_storage;

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
        struct SlotObserverAdapter final : detail::SlotObserver
        {
            explicit SlotObserverAdapter(TSDState *owner_ = nullptr) noexcept : owner(owner_) {}

            void on_capacity(size_t old_capacity, size_t new_capacity) override;
            void on_insert(size_t slot) override;
            void on_remove(size_t slot) override;
            void on_erase(size_t slot) override;
            void on_clear() override;

            TSDState *owner{nullptr};
        };

        TSDState() noexcept;
        TSDState(TSDState &&other) noexcept;
        TSDState &operator=(TSDState &&other) noexcept;
        TSDState(const TSDState &) = delete;
        TSDState &operator=(const TSDState &) = delete;
        ~TSDState();

        void bind_value_storage(const TSMeta &element_schema,
                                const detail::MapViewDispatch &dispatch,
                                void *value_data,
                                bool current_storage_alive = true);
        void child_modified(size_t child_index, engine_time_t modified_time) noexcept;
        void unbind_value_storage() noexcept;
        void detach_value_storage() noexcept;
        void sync_with_value_storage();
        [[nodiscard]] bool publish_value_storage_delta(engine_time_t modified_time) noexcept;

        void on_capacity(size_t old_capacity, size_t new_capacity);
        void on_insert(size_t slot);
        void on_remove(size_t slot);
        void on_erase(size_t slot);
        void on_clear();

        /**
         * Active trie nodes from bound inputs, keyed by scheduling notifier.
         *
         * Each bound input branch that has active children below this TSD
         * registers its trie node here so that slot reuse in
         * ensure_child_state can evict/resolve pending entries for ALL
         * interested inputs, not just the one currently navigating.
         *
         * The key is the scheduling notifier identity (unique per input
         * branch after TargetLinkState boundary switching). This provides
         * the notifier needed for resubscription when pending entries are
         * resolved.
         */
        std::unordered_map<Notifiable *, ActiveTrieNode *> active_tries;

        const TSMeta                  *element_schema{nullptr};
        const detail::MapViewDispatch *map_dispatch{nullptr};
        void                          *map_value_data{nullptr};
        bool                           slot_observer_registered{false};
        SlotObserverAdapter            slot_observer;
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
    {
        engine_time_t first_observed_time{MIN_DT};
        bool          ready{false};
    };

    /**
     * Storage carried by an output-linked logical time-series position.
     *
     * Unlike TargetLinkState, this is output-side only. It redirects one
     * outward-facing output position to another output position and preserves
     * normal output/subscriber semantics without any input scheduling logic.
     */
    struct HGRAPH_EXPORT OutputLinkState : BaseState
    {
        struct TargetNotifiable : Notifiable
        {
            explicit TargetNotifiable(OutputLinkState *self) noexcept;

            void notify(engine_time_t modified_time) override;

            OutputLinkState *self;
        };

        OutputLinkState() noexcept;
        ~OutputLinkState();

        OutputLinkState(OutputLinkState &&other) noexcept;
        OutputLinkState &operator=(OutputLinkState &&other) noexcept;

        OutputLinkState(const OutputLinkState &) = delete;
        OutputLinkState &operator=(const OutputLinkState &) = delete;

        void set_target(LinkedTSContext target_state, engine_time_t modified_time = MIN_DT) noexcept;
        void reset_target(engine_time_t modified_time = MIN_DT) noexcept;

        LinkedTSContext target;
        Value previous_target_value;
        engine_time_t switch_modified_time{MIN_DT};
        TargetNotifiable target_notifiable;

      private:
        void register_with_target() noexcept;
        void unregister_from_target() noexcept;
    };

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
         *
         * NOTE: This only unregisters the target_notifiable from the bound
         * output's state. If the input's ActiveTrie has locally_active nodes
         * below this link boundary with scheduling notifiers subscribed to
         * output-side child states, those subscriptions must be cleaned up
         * by the caller before calling reset_target. See
         * ActiveTrieNode::unsubscribe_all (TODO: not yet implemented).
         */
        void reset_target() noexcept;

        LinkedTSContext target;
        /**
         * Snapshot of the previously bound collection value for same-tick
         * rebinding/unbinding semantics.
         *
         * This mirrors the Python input contract for peered dynamic
         * collections: during the transition tick the input remains readable
         * from the old collection view even after the binding itself has been
         * changed or removed.
         */
        Value previous_target_value;
        engine_time_t switch_modified_time{MIN_DT};
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
        [[nodiscard]] engine_time_t last_target_modified_time() const noexcept;
        [[nodiscard]] bool is_sampled() const noexcept;

      private:
        void register_with_target() noexcept;
        void unregister_from_target() noexcept;
    };

    HGRAPH_EXPORT std::unique_ptr<TimeSeriesStateV> make_time_series_state_node(const TSMeta &schema,
                                                                                 TimeSeriesStateParentPtr parent,
                                                                                 size_t index);

    /**
     * Storage carried by a reference-linked logical time-series position.
     *
     * A RefLinkState follows two moving pieces:
     * - the REF-typed source position that stores the TimeSeriesReference value
     * - the current dereferenced target that the REF points at
     *
     * This is only used inside TSOutput alternatives for REF -> TS
     * dereferencing. Plain TSInput binding never uses RefLinkState.
     */
    struct HGRAPH_EXPORT RefLinkState : BaseState
    {
        struct BoundaryAttachment
        {
            /**
             * Boundary-local forwarding notifier subscribed on the current
             * dereferenced target for one upstream boundary crossing.
             *
             * The map key stores the upstream notifier we ultimately forward
             * to. This stored notifier is the unique local identity used when
             * subscribing below the RefLink boundary, so multiple upstream
             * consumers can coexist without collapsing onto the same
             * subscriber pointer.
             */
            TargetLinkState::SchedulingNotifier forwarding_notifier;
            /**
             * Active subtree rooted at this RefLink boundary for the
             * associated upstream notifier.
             *
             * This lets the RefLink rebuild live subscriptions when the
             * reference retargets without needing to mirror the full input
             * path above the boundary.
             */
            ActiveTrie                         active_trie;
        };

        struct RefSourceNotifiable : Notifiable
        {
            explicit RefSourceNotifiable(RefLinkState *self) noexcept;

            void notify(engine_time_t modified_time) override;

            RefLinkState *self;
        };

        struct DereferencedTargetNotifiable : Notifiable
        {
            explicit DereferencedTargetNotifiable(RefLinkState *self) noexcept;

            void notify(engine_time_t modified_time) override;

            RefLinkState *self;
        };

        RefLinkState() noexcept;
        ~RefLinkState();

        RefLinkState(RefLinkState &&other) noexcept;
        RefLinkState &operator=(RefLinkState &&other) noexcept;

        RefLinkState(const RefLinkState &) = delete;
        RefLinkState &operator=(const RefLinkState &) = delete;

        void set_source(LinkedTSContext source_state) noexcept;
        void reset_source() noexcept;

        [[nodiscard]] engine_time_t last_target_modified_time() const;
        [[nodiscard]] bool          is_sampled() const;
        LinkedTSContext             source;  // Bound REF source slot.
        RefSourceNotifiable         source_notifiable;
        DereferencedTargetNotifiable target_notifiable;
        TargetLinkState             bound_link;  // Current dereferenced target.
        /**
         * Whether ref-target switches should retain a previous-target snapshot.
         *
         * Ordinary sampled REF[TSD]/REF[TSS] semantics need this. Internal
         * bridge-only ref links used by alternative replay can disable it when
         * they already resync structurally and never consume the snapshot.
         */
        bool                        retain_transition_value{true};
        Value                       previous_target_value;
        engine_time_t               switch_modified_time{MIN_DT};
        /**
         * Boundary-local attachment state keyed by the upstream notifier to
         * forward to.
         */
        std::unordered_map<Notifiable *, BoundaryAttachment> boundary_attachments;

        [[nodiscard]] BoundaryAttachment &attachment_for(Notifiable *upstream_notifier) noexcept;
        [[nodiscard]] BaseState *current_target_root_state() const noexcept;

      private:
        void register_with_source() noexcept;
        void unregister_from_source() noexcept;
        void register_with_target() noexcept;
        void unregister_from_target() noexcept;
        void replay_boundary_attachments(bool subscribe) noexcept;
        void refresh_target(engine_time_t modified_time, bool propagate) noexcept;
    };

    /**
     * State carried by a signal time-series.
     *
     * Signals are usually leaves, but a non-peered collection bound into a
     * SIGNAL input is represented as a signal wrapper around a planned child
     * state tree for the bound shape. Any child tick promotes the parent
     * signal without exposing the bound value semantics through the SIGNAL API.
     */
    struct HGRAPH_EXPORT SignalState : BaseCollectionState
    {
        const TSMeta *bound_schema{nullptr};
    };

}  // namespace hgraph
