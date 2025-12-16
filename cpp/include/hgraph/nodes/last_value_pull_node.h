// Version-selecting forwarding header for last_value_pull_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/last_value_pull_node.h>
#else
#include <hgraph/nodes/v1/last_value_pull_node.h>
#endif
