// Version-selecting forwarding header for mesh_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/mesh_node.h>
#else
#include <hgraph/nodes/v1/mesh_node.h>
#endif
