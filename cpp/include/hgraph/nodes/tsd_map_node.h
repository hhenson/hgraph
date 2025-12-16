// Version-selecting forwarding header for tsd_map_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/tsd_map_node.h>
#else
#include <hgraph/nodes/v1/tsd_map_node.h>
#endif
