#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_input/list_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>
#include <hgraph/types/time_series/ts_input/window_view.h>
#include <hgraph/types/time_series/ts_output.h>

#include <memory>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        struct TSInputActiveTarget;
        struct TSInputViewOps;
    }

    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    /**
     * Construction plan for one input bundle.
     *
     * The plan is compiled from a canonical TS schema plus a generic endpoint
     * annotation tree. TSInput requires a non-peered TSB root, with nested
     * non-peered TSB/fixed-TSL prefixes and peered terminals beneath it. A
     * peered terminal compiles to input-side TargetLink storage in the input
     * data plan.
     */
    class TSInputConstructionPlan
    {
      public:
        TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                TSEndpointSchema           endpoint_schema);

        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] const TSEndpointSchema &endpoint_schema() const noexcept;

      private:
        const TSValueTypeMetaData *schema_{nullptr};
        TSEndpointSchema          endpoint_schema_{};
    };

    class TSInputPlanFactory
    {
      public:
        [[nodiscard]] static TSInputConstructionPlan compile(
            const TSValueTypeMetaData                  &root_schema,
            const TSEndpointSchema                     &endpoint_schema);
    };

    /**
     * Cached builder for TSInput endpoint storage.
     *
     * Unlike TSOutput construction, an input builder does not allocate a copied
     * output payload for the visible time-series value. It builds the planned
     * input TSData root: non-peered TSB/fixed-TSL prefixes plus TargetLink
     * terminal storage used to borrow output TSData at runtime.
     */
    class TSInputBuilder
    {
      public:
        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] TSInput make_input() const;

      private:
        friend class TSInputBuilderFactory;
        friend class TSInput;

        explicit TSInputBuilder(TSInputConstructionPlan plan);

        TSInputConstructionPlan plan_;
        TSInputTypeRef          storage_type_{};
    };

    class TSInputBuilderFactory
    {
      public:
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSValueTypeMetaData &root_schema,
                                                               const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                       const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSInputConstructionPlan &plan);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSInputConstructionPlan &plan);

        /** Clear cached input builders and type-erased input binding contexts. */
        static void reset() noexcept;
    };

    /**
     * Owning input-side time-series endpoint.
     *
     * TSInput owns planned input TSData storage plus a sparse activation trie.
     * The root is always a non-peered TSB; peered terminals inside that tree
     * borrow TSOutput TSData through input-side TargetLink storage.
     */
    class TSInput : private TSDataParent
    {
      public:
        TSInput() noexcept;
        explicit TSInput(const TSInputBuilder &builder);
        TSInput(const TSInput &other);
        TSInput &operator=(const TSInput &other);
        TSInput(TSInput &&other) noexcept;
        TSInput &operator=(TSInput &&other) noexcept;
        ~TSInput();

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] TSInputTypeRef type_ref() const;

        /** Node owner for this endpoint, if it is attached to a runtime graph. */
        [[nodiscard]] NodeView owner_node() const;
        [[nodiscard]] GraphView owner_graph() const;
        void bind_node_parent(const NodeView &node, TSEndpointOwnerPort port);
        void clear_node_parent();

        /**
         * Root input view.
         *
         * ``scheduling_notifier`` is normally the owning node. Active views use
         * it as the final notification target.
         */
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       DateTime evaluation_time = MIN_DT);
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       DateTime evaluation_time = MIN_DT) const;

        /** Dynamic input payload, observer, activation-trie, and target-link storage. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

      private:
        friend class TSInputView;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;
        friend struct detail::TSInputViewOps;
        friend struct TSParentLink;

        explicit TSInput(const TSInputConstructionPlan &plan);

        void rebuild_from_plan(const TSInputConstructionPlan &plan);
        void attach_root_parent();
        void record_child_modified(std::size_t child_id, DateTime mutation_time) override;
        void make_active(std::vector<std::size_t> path, TSDataView observed, Notifiable *target_notifier);
        void make_passive(const std::vector<std::size_t> &path);
        [[nodiscard]] bool active(const std::vector<std::size_t> &path) const noexcept;

        /** Allocation-free twin of ``active(path)``: walks the parent chain
            recursively and descends the activity trie on the unwind, so the
            per-call ``path_from_root()`` vector disappears from the hot
            activity probe (access-path audit deferred item, 2026-08-16). */
        [[nodiscard]] bool active(const TSParentLink &leaf_link) const noexcept;

        const TSInputBuilder             *builder_{nullptr};
        const TSValueTypeMetaData        *schema_{nullptr};
        TSData                            data_{};
        std::unique_ptr<detail::TSInputActiveTarget> active_root_{};
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H
