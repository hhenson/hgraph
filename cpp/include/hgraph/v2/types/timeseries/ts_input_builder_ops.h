#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H

#include <hgraph/v2/types/timeseries/ts_value_builder_ops.h>

#include <stdexcept>

namespace hgraph::v2
{
    struct TsInputBuilderOps
    {
        const TsInputTypeBinding *binding{nullptr};

        [[nodiscard]] bool valid() const noexcept { return binding != nullptr; }

        [[nodiscard]] const TsInputTypeBinding &checked_binding() const {
            if (binding != nullptr) { return *binding; }
            throw std::logic_error("TsInputBuilderOps is missing a TS input binding");
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_OPS_H
