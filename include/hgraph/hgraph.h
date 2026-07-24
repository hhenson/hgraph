// hgraph.h — top-level convenience header: the runtime execution surface plus
// the primitive scalar vocabulary. Authoring a node/graph additionally needs
// <hgraph/types/static_node.h> + <hgraph/types/graph_wiring.h> (and
// <hgraph/lib/std/std_operators.h> for the operator catalogue); the testing
// harness lives in <hgraph/lib/testing/>. See docs/source/user_guide/.
#pragma once

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/temporal.h>
#include <hgraph/version.h>
