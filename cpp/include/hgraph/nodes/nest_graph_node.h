// Version-selecting forwarding header for nest_graph_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/nest_graph_node.h>
#else
#include <hgraph/nodes/v1/nest_graph_node.h>
#endif
