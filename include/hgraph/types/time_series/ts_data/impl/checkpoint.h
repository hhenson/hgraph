#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_IMPL_CHECKPOINT_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_IMPL_CHECKPOINT_H

#include <hgraph/types/time_series/ts_data/checkpoint.h>

namespace hgraph::ts_checkpoint_detail
{
    [[nodiscard]] HGRAPH_EXPORT const TSCheckpointOps &atomic_checkpoint_ops() noexcept;
    [[nodiscard]] HGRAPH_EXPORT const TSCheckpointOps &fixed_checkpoint_ops() noexcept;
    /** Metadata validation shared by concrete representation implementations. */
    HGRAPH_EXPORT void validate_header(const TSDataView &, const TSCheckpointImage &);
    /** Internal recursive import after the complete validation pass. */
    HGRAPH_EXPORT void restore_validated(const TSDataView &, const TSCheckpointImage &);
}

#endif
