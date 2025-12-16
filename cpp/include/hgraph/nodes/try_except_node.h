// Version-selecting forwarding header for try_except_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/try_except_node.h>
#else
#include <hgraph/nodes/v1/try_except_node.h>
#endif
