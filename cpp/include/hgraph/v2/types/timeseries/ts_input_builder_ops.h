#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H

#include <hgraph/v2/types/timeseries/ts_value_builder.h>

#include <stdexcept>

namespace hgraph::v2
{
    struct TsInputBuilderOps
    {
        const TsValueBuilder *ts_value_builder{nullptr};

        [[nodiscard]] bool valid() const noexcept { return ts_value_builder != nullptr; }

        [[nodiscard]] const TsValueBuilder &checked_ts_value_builder() const {
            if (ts_value_builder != nullptr) { return *ts_value_builder; }
            throw std::logic_error("TsInputBuilderOps is missing a TS value builder");
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H
