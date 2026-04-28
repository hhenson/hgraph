#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_BUILDER_OPS_H

#include <hgraph/v2/types/timeseries/ts_value_builder_ops.h>

#include <stdexcept>

namespace hgraph::v2
{
    struct TsOutputBuilderOps
    {
        const TsOutputTypeBinding *binding{nullptr};

        [[nodiscard]] bool valid() const noexcept { return binding != nullptr; }

        [[nodiscard]] const TsOutputTypeBinding &checked_binding() const {
            if (binding != nullptr) { return *binding; }
            throw std::logic_error("TsOutputBuilderOps is missing a TS output binding");
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_BUILDER_OPS_H
