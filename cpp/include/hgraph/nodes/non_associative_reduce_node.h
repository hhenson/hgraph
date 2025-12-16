// Version-selecting forwarding header for non_associative_reduce_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/non_associative_reduce_node.h>
#else
#include <hgraph/nodes/v1/non_associative_reduce_node.h>
#endif
