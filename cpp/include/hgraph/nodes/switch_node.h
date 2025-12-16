// Version-selecting forwarding header for switch_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/switch_node.h>
#else
#include <hgraph/nodes/v1/switch_node.h>
#endif
