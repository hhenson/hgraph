#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/nodes/python_generator_node.h>

void export_nodes(nb::module_ &m) {
    using namespace hgraph;

    NestedNode::register_with_nanobind(m);
    NestedGraphNode::register_with_nanobind(m);

    NestedEngineEvaluationClock::register_with_nanobind(m);
    NestedEvaluationEngine::register_with_nanobind(m);
    register_tsd_map_with_nanobind(m);
    register_reduce_node_with_nanobind(m);

    PythonGeneratorNode::register_with_nanobind(m);
    ComponentNode::register_with_nanobind(m);
    register_switch_node_with_nanobind(m);
    TryExceptNode::register_with_nanobind(m);
    register_non_associative_reduce_node_with_nanobind(m);
    register_mesh_node_with_nanobind(m);
    LastValuePullNode::register_with_nanobind(m);
    register_context_node_with_nanobind(m);
}