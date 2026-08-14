#ifndef HGRAPH_CPP_TS_OUTPUT_ALTERNATIVE_H
#define HGRAPH_CPP_TS_OUTPUT_ALTERNATIVE_H

#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/utils/small_dense_ptr_map.h>

#include <memory>

namespace hgraph::detail
{
    class TSOutputAlternativeStore;
}

namespace hgraph
{
    struct TimeSeriesReference;
}

namespace hgraph::detail
{
    /**
     * Root-owned cache for output alternative binding data.
     *
     * Each cached alternative is keyed by the output view where binding starts
     * and the requested input schema. The requested schema drives the plan and
     * ops exposed by the alternative handle.
     */
    class TSOutputAlternativeStore
    {
      public:
        struct ToRefAlternativeState;
        struct RefLinkAlternativeState;
        struct InteriorFromRefAlternativeState;
        struct ToRefAlternativeDelete
        {
            void operator()(ToRefAlternativeState *) const noexcept;
        };
        struct RefLinkAlternativeDelete
        {
            void operator()(RefLinkAlternativeState *) const noexcept;
        };
        struct InteriorFromRefAlternativeDelete
        {
            void operator()(InteriorFromRefAlternativeState *) const noexcept;
        };

        TSOutputAlternativeStore() noexcept;
        TSOutputAlternativeStore(const TSOutputAlternativeStore &) = delete;
        TSOutputAlternativeStore &operator=(const TSOutputAlternativeStore &) = delete;
        TSOutputAlternativeStore(TSOutputAlternativeStore &&) noexcept;
        TSOutputAlternativeStore &operator=(TSOutputAlternativeStore &&) noexcept;
        ~TSOutputAlternativeStore() noexcept;

        [[nodiscard]] TSOutputHandle binding_for(const TSOutputView &source,
                                                 const TSValueTypeMetaData &requested_schema);

        /**
         * Stop-time subscription teardown. Alternatives subscribe to their
         * source output and (for from-ref alternatives) hold target links to
         * whatever output the reference currently designates — outputs that
         * teardown order may destroy first. The lifecycle contract is that
         * ``stop`` releases these while every producer is still alive, so the
         * store's destructor finds no live references. ``release_time`` is the
         * stop-time evaluation time (must not be ``MIN_DT``).
         */
        void release_subscriptions(DateTime release_time) noexcept;
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;
        [[nodiscard]] static TimeSeriesReference peered_reference_as(const TSValueTypeMetaData *target_schema,
                                                                     TSOutputHandle target);
        [[nodiscard]] static const TSOutputHandle &peered_reference_target(const TimeSeriesReference &reference);
        [[nodiscard]] static TSDataView child_view_with_parent(const TSDataView &parent,
                                                               const TSDataView &child,
                                                               std::size_t child_id);

      private:
        struct AlternativeKey
        {
            const TSOutput              *source_output{nullptr};
            TSRoleTypeRef             source_type{};
            const void                  *source_data{nullptr};
            const TSValueTypeMetaData   *requested_schema{nullptr};

            [[nodiscard]] bool operator==(const AlternativeKey &) const noexcept = default;
        };

        struct AlternativeKeyHash
        {
            [[nodiscard]] std::size_t operator()(const AlternativeKey &key) const noexcept;
        };

        [[nodiscard]] static AlternativeKey key_for(const TSOutputView &source,
                                                    const TSValueTypeMetaData &requested_schema) noexcept;

        [[nodiscard]] TSOutputHandle to_ref_binding(const AlternativeKey &key,
                                                    const TSOutputView &source,
                                                    const TSValueTypeMetaData &requested_schema);
        [[nodiscard]] TSOutputHandle from_ref_binding(const AlternativeKey &key,
                                                      const TSOutputView &source,
                                                      const TSValueTypeMetaData &requested_schema);
        [[nodiscard]] TSOutputHandle from_ref_interior_binding(const AlternativeKey &key,
                                                               const TSOutputView &source,
                                                               const TSValueTypeMetaData &requested_schema);

        SmallDensePtrMap<AlternativeKey, ToRefAlternativeState,
                         ToRefAlternativeDelete, AlternativeKeyHash> to_ref_alternatives_{};
        SmallDensePtrMap<AlternativeKey, RefLinkAlternativeState,
                         RefLinkAlternativeDelete, AlternativeKeyHash> ref_link_alternatives_{};
        SmallDensePtrMap<AlternativeKey, InteriorFromRefAlternativeState,
                         InteriorFromRefAlternativeDelete, AlternativeKeyHash>
            interior_from_ref_alternatives_{};
    };
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_OUTPUT_ALTERNATIVE_H
