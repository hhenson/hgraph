#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_IMPL_CURRENT_STATE_OPS_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_IMPL_CURRENT_STATE_OPS_H

#include <hgraph/types/time_series/ts_data/current_state_ops.h>
#include <hgraph/types/time_series/ts_type_ref.h>

namespace hgraph::ts_current_state_detail
{
    /** Select the semantic current-state policy once while constructing TSData ops. */
    [[nodiscard]] HGRAPH_EXPORT const TSCurrentStateOps &current_state_ops_for(TSTypeKind kind) noexcept;
}

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DATA_IMPL_CURRENT_STATE_OPS_H
