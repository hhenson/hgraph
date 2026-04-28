#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_OPS_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_OPS_H

#include <hgraph/v2/types/timeseries/ts_value_ops.h>
#include <hgraph/v2/types/utils/intern_table.h>

namespace hgraph::v2
{
    /**
     * Schema-shared operations for output-side TS handles and views.
     *
     * This starts with the same `ValueTypeBinding` projection as the common
     * TS schema ops, but keeps the output endpoint on a distinct binding track
     * so output-local schema behavior can be added later without refactoring
     * the handle identity model.
     */
    struct TsOutputOps : TsValueOps
    {};

    namespace detail
    {
        [[nodiscard]] inline InternTable<const ValueTypeBinding *, TsOutputOps> &ts_output_ops_registry() noexcept {
            static InternTable<const ValueTypeBinding *, TsOutputOps> registry;
            return registry;
        }
    }  // namespace detail

    [[nodiscard]] inline const TsOutputOps &ts_output_ops(const ValueTypeBinding &value_binding) {
        TsOutputOps ops;
        ops.value_binding = &value_binding;
        return detail::ts_output_ops_registry().emplace(&value_binding, ops);
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_OPS_H
