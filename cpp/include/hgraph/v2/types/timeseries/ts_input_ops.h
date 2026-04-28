#ifndef HGRAPH_CPP_ROOT_TS_INPUT_OPS_H
#define HGRAPH_CPP_ROOT_TS_INPUT_OPS_H

#include <hgraph/v2/types/timeseries/ts_value_ops.h>
#include <hgraph/v2/types/utils/intern_table.h>

namespace hgraph::v2
{
    /**
     * Schema-shared operations for input-side TS handles and views.
     *
     * The first pass only needs the underlying `ValueTypeBinding` projection,
     * but this separate ops record gives input views a dedicated endpoint
     * binding that can grow input-specific schema behavior later.
     */
    struct TsInputOps : TsValueOps
    {
        TsInputOps() = default;
        TsInputOps(const TsValueOps &base) : TsValueOps(base) {}
    };

    namespace detail
    {
        [[nodiscard]] inline InternTable<TsValueOpsKey, TsInputOps, TsValueOpsKeyHash> &ts_input_ops_registry() noexcept {
            static InternTable<TsValueOpsKey, TsInputOps, TsValueOpsKeyHash> registry;
            return registry;
        }
    }  // namespace detail

    [[nodiscard]] inline const TsInputOps &ts_input_ops(const TSValueTypeMetaData &type, const ValueTypeBinding &value_binding) {
        return detail::ts_input_ops_registry().intern(
            detail::TsValueOpsKey{
                .type          = &type,
                .value_binding = &value_binding,
            },
            [&]() { return TsInputOps{ts_value_ops(type, value_binding)}; });
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_OPS_H
