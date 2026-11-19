#pragma once

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>

namespace hgraph::ts_data_detail
{
    /** Materialize the same window change for captures and borrowed container children. */
    [[nodiscard]] Value build_window_change(ValueTypeRef binding, bool cleared, ValueView arrival,
                                            bool replacement, Range<ValueView> samples, Range<DateTime> times);
}
