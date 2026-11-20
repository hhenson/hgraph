#ifndef HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_APPLY_VALUE_H
#define HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_APPLY_VALUE_H

#include <hgraph/hgraph_export.h>

namespace hgraph
{
    class TSOutputView;
    class ValueView;

    /** Apply a canonical per-cycle delta to a live output. */
    HGRAPH_EXPORT void apply_delta(const TSOutputView &out, const ValueView &delta);

    /** Apply a whole scalar or collection value to a live output. */
    HGRAPH_EXPORT void apply_current_value(const TSOutputView &out, const ValueView &value);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_APPLY_VALUE_H
