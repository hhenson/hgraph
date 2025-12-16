// Version-selecting forwarding header for base_python_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/base_python_node.h>
#else
#include <hgraph/nodes/v1/base_python_node.h>
#endif
