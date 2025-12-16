// Version-selecting forwarding header for component_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/component_node.h>
#else
#include <hgraph/nodes/v1/component_node.h>
#endif
