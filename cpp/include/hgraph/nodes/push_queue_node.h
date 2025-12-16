// Version-selecting forwarding header for push_queue_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/push_queue_node.h>
#else
#include <hgraph/nodes/v1/push_queue_node.h>
#endif
