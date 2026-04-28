#ifndef HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H

#include <hgraph/v2/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/v2/types/metadata/type_binding.h>
#include <hgraph/v2/types/timeseries/ts_input_ops.h>
#include <hgraph/v2/types/timeseries/ts_output_ops.h>
#include <hgraph/v2/types/timeseries/ts_value_ops.h>

#include <stdexcept>

namespace hgraph::v2
{
    using TsValueTypeBinding  = TypeBinding<TSValueTypeMetaData, TsValueOps>;
    using TsInputTypeBinding  = TypeBinding<TSValueTypeMetaData, TsInputOps>;
    using TsOutputTypeBinding = TypeBinding<TSValueTypeMetaData, TsOutputOps>;

    struct TsValueBuilderOps
    {
        const TsValueTypeBinding *binding{nullptr};

        [[nodiscard]] bool valid() const noexcept { return binding != nullptr; }

        [[nodiscard]] const TsValueTypeBinding &checked_binding() const {
            if (binding != nullptr) { return *binding; }
            throw std::logic_error("TsValueBuilderOps is missing a TS value binding");
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H
