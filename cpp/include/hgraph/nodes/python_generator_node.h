// Version-selecting forwarding header for python_generator_node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/python_generator_node.h>
#else
#include <hgraph/nodes/v1/python_generator_node.h>
#endif
