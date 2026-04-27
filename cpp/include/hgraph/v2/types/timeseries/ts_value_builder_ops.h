//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H

#include <hgraph/v2/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/v2/types/metadata/type_binding.h>
#include <hgraph/v2/types/timeseries/ts_value_ops.h>

namespace hgraph::v2
{
    using TsValueTypeBinding = TypeBinding<TSValueTypeMetaData, TsValueOps>;

    struct TsValueBuilderOps
    {
        const TsValueTypeBinding *binding{nullptr};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_OPS_H
