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
    {};

    namespace detail
    {
        [[nodiscard]] inline InternTable<const ValueTypeBinding *, TsInputOps> &ts_input_ops_registry() noexcept {
            static InternTable<const ValueTypeBinding *, TsInputOps> registry;
            return registry;
        }
    }  // namespace detail

    [[nodiscard]] inline const TsInputOps &ts_input_ops(const ValueTypeBinding &value_binding) {
        TsInputOps ops;
        ops.value_binding = &value_binding;
        return detail::ts_input_ops_registry().emplace(&value_binding, ops);
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_OPS_H
