// Version-selecting forwarding header for reduce_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/reduce_node.h>
#else
#include <hgraph/nodes/v1/reduce_node.h>
#endif
