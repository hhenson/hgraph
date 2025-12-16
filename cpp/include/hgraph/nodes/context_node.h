// Version-selecting forwarding header for context_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/context_node.h>
#else
#include <hgraph/nodes/v1/context_node.h>
#endif
