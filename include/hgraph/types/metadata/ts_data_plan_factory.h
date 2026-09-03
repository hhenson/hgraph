#ifndef HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H
#define HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_type_ref.h>
#include <hgraph/types/utils/memory_utils.h>

#include <mutex>

#include <hgraph/types/utils/counted_mutex.h>
#include <unordered_map>

namespace hgraph
{
    /**
     * Factory that maps a time-series ``TSValueTypeMetaData`` schema to the
     * canonical ``MemoryUtils::StoragePlan`` and role-specific ``TypeRecord``
     * for the TS data component.
     *
     * ``TSOutput`` and ``TSInput`` are the top-level runtime containers for time-series
     * holders. They combine endpoint state (modification scope, validity,
     * subscribers, binding, and scheduling) with a payload/delta component
     * named ``TSData``. This factory resolves only the ``TSData`` memory plan.
     * Endpoint construction is handled by the reusable input/output builders
     * that compose this data plan with the separate endpoint state.
     *
     * Atomic TSData uses the compact value storage plan with mutable ops
     * enabled. Fixed ``TSB`` and fixed-size ``TSL`` allocate the complete
     * current value as the first canonical value-layer region, then store
     * child/parent tracking or projected child storage in a separate
     * auxiliary tree. Keyed collection TSData uses slot-oriented storage so
     * current payload and delta bookkeeping stay aligned by stable slot id.
     * Dynamic ``TSL`` uses indexed child storage with stable child TSData
     * handles. It grows and truncates: removal is tail truncation, reported
     * through the ``Bundle{removed, modified}`` delta (RFC 0031).
     *
     * Implemented synthesis paths cover atomic TSData (``TS<T>``, ``REF<T>``,
     * and ``SIGNAL``), fixed structured TSData (``TSB`` and fixed-size
     * ``TSL``), dynamic list TSData (``TSL`` with size ``0``), window TSData
     * (``TSW``), and keyed slot TSData (``TSS`` and ``TSD``). Fixed
     * structured parents can nest any implemented non-``REF`` child kind.
     *
     * The factory is a process-wide singleton via ``instance()``;
     * non-copyable and non-movable.
     */
    class HGRAPH_CLASS_EXPORT TSDataPlanFactory
    {
      public:
        /** Singleton accessor; returns a reference to the process-wide factory. */
        static TSDataPlanFactory &instance();

        TSDataPlanFactory(const TSDataPlanFactory &)            = delete;
        TSDataPlanFactory &operator=(const TSDataPlanFactory &) = delete;
        TSDataPlanFactory(TSDataPlanFactory &&)                 = delete;
        TSDataPlanFactory &operator=(TSDataPlanFactory &&)      = delete;

        /**
         * Look up or synthesise the canonical plan for ``schema``.
         *
         * Returns ``nullptr`` when ``schema`` is null.
         */
        const MemoryUtils::StoragePlan *plan_for(const TSValueTypeMetaData *schema);

        /** Look up only; never synthesises. Returns ``nullptr`` when missing. */
        const MemoryUtils::StoragePlan *find(const TSValueTypeMetaData *schema) const;

        /** Canonical standalone-data role record. */
        [[nodiscard]] TSDataTypeRef data_type_for(const TSValueTypeMetaData *schema);
        /** Canonical output role record. */
        [[nodiscard]] TSOutputTypeRef output_type_for(const TSValueTypeMetaData *schema);
        /** Output role using the value factory's requested storage variant. */
        [[nodiscard]] TSOutputTypeRef output_type_for(
            const TSValueTypeMetaData *schema,
            ValueStorageVariant requested);
        /** Atomic or fixed-TSB output role using an explicitly realized value binding. */
        [[nodiscard]] TSOutputTypeRef output_type_for(const TSValueTypeMetaData *schema,
                                                      ValueTypeRef value_binding,
                                                      ValueStorageVariant requested =
                                                          ValueStorageVariant::Native);
        /** TSD output role using an explicitly realized child TS binding. */
        [[nodiscard]] TSOutputTypeRef output_type_for(const TSValueTypeMetaData *schema,
                                                      TSRoleTypeRef element_type);
        /** TSS/TSD output role using a graph-realized key binding and, for
            TSD, its graph-realized child TS binding. */
        [[nodiscard]] TSOutputTypeRef keyed_output_type_for(
            const TSValueTypeMetaData *schema,
            ValueTypeRef key_binding,
            TSRoleTypeRef element_type = {});
        [[nodiscard]] TSDataTypeRef find_data_type(const TSValueTypeMetaData *schema) const noexcept;
        [[nodiscard]] TSOutputTypeRef find_output_type(const TSValueTypeMetaData *schema) const noexcept;

        /**
         * Drop every cached schema → plan mapping. Test-only helper used to
         * isolate tests from each other; production code must not call it.
         */
        void reset() noexcept;

      private:
        TSDataPlanFactory() = default;

        const MemoryUtils::StoragePlan *synthesise(const TSValueTypeMetaData *schema);

        mutable TypeSystemMutex                                                           mutex_;
        std::unordered_map<const TSValueTypeMetaData *, const MemoryUtils::StoragePlan *> cache_;
        std::unordered_map<const TSValueTypeMetaData *, TSDataTypeRef>                     data_type_cache_;
        std::unordered_map<const TSValueTypeMetaData *, TSOutputTypeRef>                   output_type_cache_;

        struct RealizedOutputKey
        {
            const TSValueTypeMetaData *schema{nullptr};
            ValueTypeRef value{};
            ValueStorageVariant storage{ValueStorageVariant::Native};
            bool operator==(const RealizedOutputKey &) const noexcept = default;
        };
        struct RealizedOutputKeyHash
        {
            std::size_t operator()(const RealizedOutputKey &key) const noexcept
            {
                auto seed = std::hash<const TSValueTypeMetaData *>{}(key.schema);
                seed ^= std::hash<ValueTypeRef>{}(key.value) + 0x9e3779b9U +
                        (seed << 6U) + (seed >> 2U);
                return seed ^
                       (std::hash<std::uint8_t>{}(
                            static_cast<std::uint8_t>(key.storage)) +
                        0x9e3779b9U + (seed << 6U) + (seed >> 2U));
            }
        };
        std::unordered_map<RealizedOutputKey, TSOutputTypeRef, RealizedOutputKeyHash>
            realized_output_type_cache_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H
