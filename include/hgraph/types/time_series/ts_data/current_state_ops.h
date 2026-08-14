#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_CURRENT_STATE_OPS_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_CURRENT_STATE_OPS_H

#include <hgraph/hgraph_export.h>

#include <cstdint>

namespace hgraph
{
    class TSDataView;
    class TSInputView;
    class TSOutputView;
    class Value;
    class ValueView;
    struct TSValueTypeMetaData;
    struct ValueTypeMetaData;

    /** Select how much of a source TSData tree a current-state reconciliation visits. */
    enum class TSCurrentReconcileScope : std::uint8_t
    {
        /** Visit only state changed at the target view's evaluation time. */
        Incremental,
        /** Make the target's complete live state match the source. */
        Full,
    };

    /** Wiring/runtime policy for copying live TSData state into a publication snapshot. */
    struct TSCurrentReconcileOptions
    {
        TSCurrentReconcileScope scope{TSCurrentReconcileScope::Full};
        /** Publish every visited live value even when its value is unchanged. */
        bool sample_all{false};
    };

    /**
     * Passive operations for current-state transfer through one realized TSData strategy.
     *
     * This table is deliberately separate from ordinary per-cycle delta operations:
     * current values use patch semantics for partially-valid fixed structures, while an
     * exact reconciliation must also preserve invalid children and collection removals.
     * Concrete TSData factories select one non-null table from immutable schema/plan
     * metadata. Generic operators dispatch here and must not recover the representation
     * by switching on ``TSTypeKind``.
     */
    struct TSCurrentStateOps
    {
        bool (*value_schema_compatible_impl)(const TSValueTypeMetaData &schema,
                                             const ValueTypeMetaData &value_schema);
        Value (*capture_current_delta_impl)(const TSInputView &input);
        void (*apply_current_value_impl)(const TSOutputView &output,
                                         const ValueView &value);
        bool (*delta_is_observable_impl)(const TSInputView &input,
                                         const ValueView &delta);
        void (*reconcile_input_impl)(const TSOutputView &target,
                                     const TSInputView &source,
                                     TSCurrentReconcileOptions options);
        void (*reconcile_data_impl)(const TSOutputView &target,
                                    const TSDataView &source,
                                    TSCurrentReconcileOptions options);
    };

    namespace ts_current_state_detail
    {
        /** Canonical throwing table used by default and moved-from TSData policies. */
        [[nodiscard]] HGRAPH_EXPORT const TSCurrentStateOps &missing_current_state_ops() noexcept;
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DATA_CURRENT_STATE_OPS_H
