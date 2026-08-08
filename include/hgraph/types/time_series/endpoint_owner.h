#ifndef HGRAPH_CPP_TIME_SERIES_ENDPOINT_OWNER_H
#define HGRAPH_CPP_TIME_SERIES_ENDPOINT_OWNER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node_fwd.h>
#include <hgraph/util/date_time.h>

#include <cstdint>

namespace hgraph
{
    /** Node endpoint slot owned by a runtime node allocation. */
    enum class TSEndpointOwnerPort : std::uint8_t
    {
        Input,
        Output,
        ErrorOutput,
        RecordableState,
    };

    void HGRAPH_EXPORT notify_node_endpoint_child_modified(NodePtr             node,
                                                           TSEndpointOwnerPort port,
                                                           DateTime            mutation_time);

}  // namespace hgraph

#endif  // HGRAPH_CPP_TIME_SERIES_ENDPOINT_OWNER_H
