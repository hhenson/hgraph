#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <cstddef>
#include <memory>
#include <vector>

namespace hgraph
{
    class GraphView;
    class NodeView;

    namespace detail
    {
        struct TSInputChildProjection;
        struct TSInputTargetActiveNode;
        struct TSInputViewOps;

        /**
         * A prepared canonical-slot route (RFC 0008 stage 5): the per-tick
         * cache that replaces the projection walk for a static node's input
         * slot. ``data`` is the slot's target-link storage (target slots) or
         * its owned input storage (non-target slots); ``observed`` points at
         * the active-trie root's in-place-updated output handle when the slot
         * is actively observed. Non-owning throughout: acquired after input
         * activation, cleared before deactivation, validity re-checked per
         * use via ``observed->bound()``.
         */
        struct PreparedInputSlotRoute
        {
            TSDataStorageRef<>    data{};
            const TSOutputHandle *observed{nullptr};
            bool                  target{false};

            [[nodiscard]] bool ready() const noexcept { return data.has_value(); }
        };
    }

    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    /**
     * Input-side read/binding/activation view.
     *
     * A view can represent non-peered input TSData storage, a TargetLink
     * terminal, or a position inside a bound target output reached through a
     * TargetLink terminal.
     */
    class HGRAPH_EXPORT TSInputView
    {
      public:
        TSInputView() noexcept;

        TSInputView(const TSInputView &) = delete;
        TSInputView &operator=(const TSInputView &) = delete;
        TSInputView(TSInputView &&) noexcept = default;
        TSInputView &operator=(TSInputView &&) noexcept = default;

        /** Explicitly recreate a transient cursor over the same input position. */
        [[nodiscard]] TSInputView borrowed_ref() const noexcept;
        /** Recreate the same input position for a different evaluation cycle. */
        [[nodiscard]] TSInputView borrowed_ref(DateTime evaluation_time) const noexcept;

        /** Evaluation time associated with delta/modified checks. */
        [[nodiscard]] DateTime evaluation_time() const noexcept;

        /** Consuming node that owns this input endpoint, if it is graph-attached. */
        [[nodiscard]] NodeView consumer_node() const;
        [[nodiscard]] GraphView consumer_graph() const;
        [[nodiscard]] TSEndpointOwnerPort owner_port() const noexcept;

        /** Canonical type record and schema for the input-side projection. */
        [[nodiscard]] TSInputTypeRef type_ref() const;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        /** Underlying TSData projection; empty for unbound peered terminals. */
        [[nodiscard]] const TSDataView &data_view() const noexcept;

        /** True when this view or at least one structural child has a current value. */
        [[nodiscard]] bool valid() const;

        /** True when this view and all required structural descendants have current values. */
        [[nodiscard]] bool all_valid() const;

        /** Latest modification time observed at this view, including structural children. */
        [[nodiscard]] DateTime last_modified_time() const;

        /** True when this view or a structural child was modified at the view's evaluation time. */
        [[nodiscard]] bool modified() const;

        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /**
         * Convert this input projection to a reference token.
         *
         * A REF-kind input produces its VALUE - the reference it carries -
         * collapsing the ref-of-ref indirection (hgraph parity; an invalid
         * REF input produces a typed EMPTY reference of the target schema).
         * A target-link input produces a PEERED reference when bound and a
         * typed EMPTY reference when unbound. Non-peered structural prefixes
         * convert recursively through their endpoint ops; leaf shapes reached
         * without a target link produce typed EMPTY references.
         */
        [[nodiscard]] TimeSeriesReference reference() const;

        /** For bindable target-link views, true when an output target is bound. */
        [[nodiscard]] bool bound() const noexcept;

        /** True when this view is backed by TargetLink storage whose output target can be rebound. */
        [[nodiscard]] bool is_bindable() const noexcept;

        /** Bound output reached by this target-link view, or an empty handle when unbound. */
        [[nodiscard]] TSOutputView bound_output() const;

        void bind_output(const TSOutputView &output);
        /**
         * Rebind a runtime-owned route without scheduling its active consumer.
         * The caller must separately schedule every key/value whose semantic
         * input changed; this is only for migrating an unchanged endpoint.
         */
        void rebind_output_silent(const TSOutputView &output);
        /** Bind an already-valid source as a lifecycle-owned sampled input. */
        void bind_output_sampled(const TSOutputView &output, DateTime modified_time);
        void unbind_output();

        void make_active();
        /** Subscribe this input to structural changes rather than child value ticks. */
        void make_structural_active();
        void make_passive();
        [[nodiscard]] bool active() const;

        /** Shape-erased indexed child projection for TSB/TSL-like inputs. */
        [[nodiscard]] TSInputView indexed_child_at(std::size_t index) const;

        /**
         * RFC 0008 stage 5 (static-node implementation detail — not a stable
         * extension API). ``prepare_child_route`` captures the slot's route
         * once, after input activation; ``child_from_prepared`` rebuilds the
         * slot view from that cache without the per-tick projection walk. The
         * route is non-owning; behaviour is identical to
         * ``indexed_child_at`` for every binding/invalidation state.
         */
        [[nodiscard]] detail::PreparedInputSlotRoute prepare_child_route(std::size_t index) const;
        [[nodiscard]] TSInputView child_from_prepared(const detail::PreparedInputSlotRoute &route) const;

        [[nodiscard]] TSSInputView as_set() &;
        [[nodiscard]] TSSInputView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDInputView as_dict() &;
        [[nodiscard]] TSDInputView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBInputView as_bundle() &;
        [[nodiscard]] TSBInputView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLInputView as_list() &;
        [[nodiscard]] TSLInputView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWInputView as_window() &;
        [[nodiscard]] TSWInputView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

      private:
        friend class TSInput;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;
        friend struct detail::TSInputViewOps;

        struct InputDataCursor
        {
            enum class Classification
            {
                Detect,
                Known,
            };

            InputDataCursor() noexcept = default;
            InputDataCursor(TSDataView value_data,
                            TSDataView raw_data,
                            detail::TSInputTargetActiveNode *target_node,
                            Classification classification = Classification::Detect) noexcept;

            InputDataCursor(const InputDataCursor &) = delete;
            InputDataCursor &operator=(const InputDataCursor &) = delete;
            InputDataCursor(InputDataCursor &&) noexcept = default;
            InputDataCursor &operator=(InputDataCursor &&) noexcept = default;

            [[nodiscard]] InputDataCursor borrowed_ref() const noexcept;

            [[nodiscard]] bool has_storage() const noexcept;
            [[nodiscard]] bool is_target_position() const noexcept;
            [[nodiscard]] bool is_target_root() const noexcept;
            [[nodiscard]] detail::TSInputTargetActiveNode *target_path_node() const noexcept;
            [[nodiscard]] bool target_bound() const noexcept;
            [[nodiscard]] TSRoleTypeRef storage_type() const noexcept;
            [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
            [[nodiscard]] const TSValueTypeMetaData *target_path_schema() const noexcept;
            [[nodiscard]] const TSDataView &resolved_value_data() const noexcept;
            [[nodiscard]] bool value_live() const noexcept;
            [[nodiscard]] DateTime last_modified_time() const;
            [[nodiscard]] bool modified(DateTime evaluation_time) const;
            [[nodiscard]] TSDataView &checked_value_data(const char *what) const;
            [[nodiscard]] InputDataCursor target_child(TSDataView child, std::size_t index) const;
            void bind_target(const TSOutputView &output);
            void bind_target_sampled(const TSOutputView &output, DateTime modified_time);
            void unbind_target();
            void make_active(TSInput *input, Notifiable *scheduling_notifier) const;
            void make_structural_active(TSInput *input, Notifiable *scheduling_notifier) const;
            void make_passive(TSInput *input) const;
            [[nodiscard]] bool active(const TSInput *input) const;

            mutable TSDataView value_data{};
            TSDataView         raw_data{};
            // A TU-local sentinel denotes a target-link root; real pointers
            // denote descendant path nodes. Null denotes non-target storage.
            detail::TSInputTargetActiveNode *target_node{nullptr};
            // Prepared-route fast path (RFC 0008 stage 5): set only by
            // ``child_from_prepared``. Points at the active-trie root's
            // in-place-updated handle; ``bound()`` false falls back to the
            // full resolution, so rebind/invalidation semantics are kept.
            const TSOutputHandle *route_observed{nullptr};
        };

        TSInputView(TSInput                         *input,
                    TSDataView                      value_data,
                    TSDataView                      raw_data,
                    detail::TSInputTargetActiveNode *target_node,
                    Notifiable                     *scheduling_notifier,
                    DateTime                         evaluation_time,
                    InputDataCursor::Classification classification =
                        InputDataCursor::Classification::Detect) noexcept;

        [[nodiscard]] bool is_target_position() const noexcept;
        [[nodiscard]] bool inherited_sampled_transition() const noexcept;
        [[nodiscard]] bool sampled_structural_transition() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *target_path_schema() const noexcept;
        [[nodiscard]] TSDataView input_data_view() const noexcept;
        [[nodiscard]] TSDataView resolve_target_data_view() const noexcept;
        [[nodiscard]] bool target_view_live() const noexcept;
        [[nodiscard]] TSDataView &checked_target_data_view(const char *what) const;
        [[nodiscard]] TSInputView child_from_target(TSDataView child, std::size_t index) const;
        [[nodiscard]] TSInputView child_from_input(std::size_t index) const;
        /** Child projection over RESOLVED input-shaped data (a from-REF
            alternative behind a target position): the projection carries the
            per-child link tracking (sampled rebinds). */
        [[nodiscard]] TSInputView child_from_resolved_input(const TSDataView &parent, std::size_t index) const;
        [[nodiscard]] TSInputView child_from_projection(detail::TSInputChildProjection projection,
                                                        std::size_t index) const noexcept;

        TSInput                  *input_{nullptr};
        InputDataCursor           data_{};
        Notifiable               *scheduling_notifier_{nullptr};
        DateTime             evaluation_time_{MIN_DT};
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H
