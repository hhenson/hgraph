// Version-selecting forwarding header for nested_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/nested_node.h>
#else
#include <hgraph/nodes/v1/nested_node.h>
#endif
