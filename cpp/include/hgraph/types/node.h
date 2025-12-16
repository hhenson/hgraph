// Version-selecting forwarding header for node
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/types/v2/node.h>
#else
#include <hgraph/types/v1/node.h>
#endif
