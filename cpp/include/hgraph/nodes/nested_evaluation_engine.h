// Version-selecting forwarding header for nested_evaluation_engine
#pragma once

#ifdef HGRAPH_API_V2
#include <hgraph/nodes/v2/nested_evaluation_engine.h>
#else
#include <hgraph/nodes/v1/nested_evaluation_engine.h>
#endif
