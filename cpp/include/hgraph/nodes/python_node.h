// Version-selecting forwarding header for python_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/python_node.h>
#else
#include <hgraph/nodes/v1/python_node.h>
#endif
