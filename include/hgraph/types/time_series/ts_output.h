#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_output/bundle_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/list_view.h>
#include <hgraph/types/time_series/ts_output/mutation_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/time_series/ts_output/window_view.h>

#include <memory>

namespace hgraph
{
    namespace detail
    {
        class TSOutputAlternativeStore;
    }

    class TSEndpointSchema;

    /**
     * Owning output-side time-series endpoint.
     *
     * ``TSOutput`` owns the root TSData allocation and exposes lifecycle hooks
     * used by nodes to clean up transient delta state after evaluation. Root
     * subscriptions are delegated to the root TSData observer set; child-level
     * subscriptions are registered on the projected child TSData views.
     */
    class HGRAPH_EXPORT TSOutput : private TSDataParent
    {
      public:
        TSOutput() noexcept;
        explicit TSOutput(TSOutputTypeRef type);
        explicit TSOutput(const TSValueTypeMetaData &schema);
        explicit TSOutput(const TSValueTypeMetaData *schema);
        TSOutput(const TSValueTypeMetaData &schema,
                 ValueStorageVariant value_storage);
        TSOutput(const TSValueTypeMetaData *schema,
                 ValueStorageVariant value_storage);
        explicit TSOutput(const TSEndpointSchema &endpoint_schema);
        ~TSOutput() noexcept;

        TSOutput(const TSOutput &other);
        TSOutput &operator=(const TSOutput &other);
        TSOutput(TSOutput &&other) noexcept;
        TSOutput &operator=(TSOutput &&other) noexcept;

        /** True when this output owns a bound TSData root. */
        [[nodiscard]] bool has_value() const noexcept;

        /** Root TSData type record and schema. */
        [[nodiscard]] TSOutputTypeRef type_ref() const;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed root TSData view. */
        [[nodiscard]] TSDataView data_view();
        [[nodiscard]] TSDataView data_view() const;

        /** Register / remove an observer at the root TSData level. */
        void subscribe(Notifiable *observer);
        void unsubscribe(Notifiable *observer);

        /** Read view at ``evaluation_time``. */
        [[nodiscard]] TSOutputView view(DateTime evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView view(DateTime evaluation_time = MIN_DT) const;

        /** Binding data for a canonical or alternative representation of ``source``. */
        [[nodiscard]] TSOutputHandle binding_for(const TSOutputView &source,
                                                 const TSValueTypeMetaData &requested_schema) const;

        /** Dynamic output payload, observer, and alternative-binding storage. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

        /**
         * Stop-time teardown of alternative-store subscriptions/links (the
         * graph stop pass calls this while every producer is alive, so the
         * store's destructor finds no live references). No-op without a store.
         */
        void release_alternative_subscriptions(DateTime release_time) const noexcept;

        /** Begin a root mutation scope. */
        [[nodiscard]] TSOutputMutationView begin_mutation(DateTime evaluation_time);

        /** Node owner for this endpoint, if it is attached to a runtime graph. */
        [[nodiscard]] NodeView owner_node() const;
        [[nodiscard]] GraphView owner_graph() const;
        void bind_node_parent(const NodeView &node, TSEndpointOwnerPort port);
        void clear_node_parent();

      private:
        friend class TSOutputMutationView;
        friend struct TSParentLink;
        friend void notify_node_endpoint_child_modified(NodePtr             node,
                                                        TSEndpointOwnerPort port,
                                                        DateTime            mutation_time);

        static TSData checked_data_for(const TSValueTypeMetaData *schema);
        static TSData checked_data_for(const TSValueTypeMetaData *schema,
                                       ValueStorageVariant value_storage);
        static TSData checked_data_for(const TSEndpointSchema &endpoint_schema);
        static const TSData &copyable_data(const TSOutput &other);

        void invalidate_observers() noexcept;
        void attach_root_parent();
        void record_child_modified(std::size_t child_id, DateTime mutation_time) override;

        TSData                                      data_{};
        mutable std::unique_ptr<detail::TSOutputAlternativeStore> alternatives_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H
