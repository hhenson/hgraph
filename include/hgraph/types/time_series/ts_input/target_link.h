#ifndef HGRAPH_CPP_TS_INPUT_TARGET_LINK_H
#define HGRAPH_CPP_TS_INPUT_TARGET_LINK_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_input/target_link_structural_state.h>
#include <hgraph/types/utils/small_dense_ptr_map.h>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace hgraph::detail
{
    enum class TSInputObservationKind : std::uint8_t
    {
        Value,
        Structural,
    };

    struct TSInputTargetLinkState;
    struct TSInputTargetLinkStorage;
    struct TSInputTargetLinkStructuralStorage;

    /**
     * Non-pruning invariant (RFC 0008 stage 5): an active-trie node, once
     * created, lives until its owning ``TSInputTargetLinkStorage`` is
     * destroyed or assigned over. Deactivation (``make_passive``) resets
     * ``observed`` in place but keeps the node; storage move-construction
     * repoints the tree without relocating nodes. Prepared input slots hold
     * non-owning pointers into this trie and rely on that stability; their
     * per-use trust check mirrors ``resolved_target_at_path`` (locally
     * active, value-kind observation, bound).
     */
    struct TSInputTargetActiveNode
    {
        TSInputTargetActiveNode *parent{nullptr};
        std::size_t              slot{0};
        bool locally_active{false};
        TSInputObservationKind observation_kind{TSInputObservationKind::Value};
        TSOutputHandle observed{};
        SmallDensePtrMap<std::size_t, TSInputTargetActiveNode> children{};

        [[nodiscard]] TSInputTargetActiveNode *child_at(std::size_t slot_index) const noexcept;
        [[nodiscard]] bool has_any_active() const noexcept;
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;
        TSInputTargetActiveNode &ensure_child(std::size_t slot_index);
        void clear_observed() noexcept;
    };

    struct TSInputTargetLinkState final : Notifiable
    {
        struct SchedulingNotifier final : Notifiable
        {
            SchedulingNotifier() noexcept = default;
            void set_target(Notifiable *target_) noexcept;
            [[nodiscard]] Notifiable *target() const noexcept;
            void notify(DateTime modified_time) override;

          private:
            Notifiable             *target_{nullptr};
        };

        explicit TSInputTargetLinkState(TSInputTargetLinkStorage &owner) noexcept;
        TSInputTargetLinkState(const TSInputTargetLinkState &) = delete;
        TSInputTargetLinkState &operator=(const TSInputTargetLinkState &) = delete;
        ~TSInputTargetLinkState() noexcept;

        void move_from(TSInputTargetLinkState &other) noexcept;
        void notify(DateTime modified_time) override;
        void source_invalidated(const TSDataTracking *source) noexcept override;
        [[nodiscard]] TSInputTargetActiveNode *active_root() const noexcept;
        [[nodiscard]] TSInputTargetActiveNode &ensure_active_root();
        void clear_active_observed() noexcept;
        void unsubscribe_active_tree() noexcept;

        TSInputTargetLinkStorage *owner{nullptr};
        TSOutputHandle target{};
        SchedulingNotifier  scheduling_notifier;
        std::unique_ptr<TSInputTargetActiveNode> active_root_node{};
    };

    struct TSInputTargetLinkStorage
    {
        TSInputTargetLinkStorage() noexcept;
        TSInputTargetLinkStorage(const TSInputTargetLinkStorage &other);
        TSInputTargetLinkStorage &operator=(const TSInputTargetLinkStorage &other);
        TSInputTargetLinkStorage(TSInputTargetLinkStorage &&other) noexcept;
        TSInputTargetLinkStorage &operator=(TSInputTargetLinkStorage &&other) noexcept;
        ~TSInputTargetLinkStorage() noexcept;

        [[nodiscard]] bool bound() const noexcept;
        void bind(const TSValueTypeMetaData &schema, const TSOutputView &output);
        void bind_current_value(const TSValueTypeMetaData &schema, const TSOutputView &output,
                                DateTime modified_time);
        void bind_sampled(const TSValueTypeMetaData &schema, const TSOutputView &output,
                          DateTime modified_time);
        void unbind();
        void unbind_structural(DateTime modified_time);
        void unbind_noexcept() noexcept;
        void source_invalidated(const TSDataTracking *source) noexcept;
        void record_target_modified(DateTime modified_time);
        [[nodiscard]] const TSDataTracking &key_set_tracking() const;
        [[nodiscard]] TSDataTracking &mutable_key_set_tracking();
        void record_key_set_modified(DateTime modified_time);
        void key_set_source_invalidated(const TSDataTracking *source) noexcept;
        [[nodiscard]] TSInputTargetActiveNode &root_node();
        [[nodiscard]] TSInputTargetActiveNode &child_node(TSInputTargetActiveNode *parent, std::size_t slot);
        void make_active(TSInputTargetActiveNode *node, const TSDataView &observed,
                         TSInputObservationKind observation_kind, Notifiable *target_notifier);
        void make_passive(TSInputTargetActiveNode *node);
        [[nodiscard]] bool active(const TSInputTargetActiveNode *node) const noexcept;
        [[nodiscard]] TSOutputHandle target_output_at_path(const TSValueTypeMetaData &schema,
                                                           const TSInputTargetActiveNode *node) const;
        [[nodiscard]] TSOutputHandle resolved_target_at_path(
            const TSValueTypeMetaData &schema,
            const TSInputTargetActiveNode *node) const;
        void resubscribe_active_target(const TSValueTypeMetaData &schema);
        void unsubscribe_active_target() noexcept;
        void add_slot_observer(SlotObserver *observer);
        void remove_slot_observer(SlotObserver *observer);

        [[nodiscard]] TSDataView target_view() const noexcept;
        [[nodiscard]] TSDataView previous_target_view() const noexcept;
        [[nodiscard]] const TSOutputHandle &target_output() const noexcept;
        [[nodiscard]] bool structural_transition_active() const noexcept;
        [[nodiscard]] bool sampled_structural_transition() const noexcept;
        [[nodiscard]] DateTime structural_transition_time() const noexcept;
        [[nodiscard]] const TSInputTargetLinkState *state() const noexcept;
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

        TSDataTracking tracking{};
        TSInputTargetLinkState state_;

      private:
        friend struct TSInputTargetLinkStructuralStorage;

        explicit TSInputTargetLinkStorage(
            const TSInputTargetLinkStructuralOps &structural_ops) noexcept;
        TSInputTargetLinkStorage(const TSInputTargetLinkStorage &other,
                                 const TSInputTargetLinkStructuralOps &structural_ops);
        TSInputTargetLinkStorage(TSInputTargetLinkStorage &&other,
                                 const TSInputTargetLinkStructuralOps &structural_ops) noexcept;

        void set_structural_ops(const TSInputTargetLinkStructuralOps &structural_ops) noexcept;
        void bind_impl(const TSValueTypeMetaData &schema, const TSOutputView &output,
                       DateTime modified_time, bool sampled, bool replay_source_time);
        void detach_target(bool retain_structural_target, DateTime modified_time);

        const TSInputTargetLinkStructuralOps *structural_ops_{&target_link_no_structural_ops()};
    };

    [[nodiscard]] const TSInputTargetLinkStorage *target_link_storage(const TSDataView &view) noexcept;
    [[nodiscard]] TSInputTargetLinkStorage *mutable_target_link_storage(const TSDataView &view);
    /** Target-link trie/observer bytes reachable through an input-shaped TSData tree. */
    [[nodiscard]] DynamicStorageMetrics input_target_link_dynamic_storage_metrics(
        const TSDataView &view) noexcept;
    [[nodiscard]] const TSValueTypeMetaData *target_link_schema(const TSDataView &view) noexcept;

    [[nodiscard]] bool is_target_link_view(const TSDataView &view) noexcept;
    [[nodiscard]] bool target_link_bound(const TSDataView &view) noexcept;
    [[nodiscard]] TSDataView target_link_resolve(const TSDataView &view,
                                                 const TSInputTargetActiveNode *node) noexcept;
    [[nodiscard]] const TSValueTypeMetaData *target_path_schema(const TSDataView &target_link,
                                                                const TSInputTargetActiveNode *node) noexcept;
    [[nodiscard]] TSInputTargetActiveNode *target_link_child_node(const TSDataView &view,
                                                                  TSInputTargetActiveNode *parent,
                                                                  std::size_t slot);
    void bind_target_link(const TSDataView &view, const TSOutputView &output);
    void bind_target_link_sampled(const TSDataView &view, const TSOutputView &output,
                                  DateTime modified_time);
    void unbind_target_link(const TSDataView &view);
    void make_target_link_active(const TSDataView &view,
                                 TSInputTargetActiveNode *node,
                                 const TSDataView &observed,
                                 TSInputObservationKind observation_kind,
                                 Notifiable *target_notifier);
    void make_target_link_passive(const TSDataView &view, TSInputTargetActiveNode *node);
    [[nodiscard]] bool target_link_active(const TSDataView &view,
                                          const TSInputTargetActiveNode *node) noexcept;
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_TARGET_LINK_H
