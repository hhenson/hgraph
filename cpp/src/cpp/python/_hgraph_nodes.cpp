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
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/python/cpp_node_builder_binding.h>

namespace {
    struct CppNoopSpec {
        static constexpr const char* py_factory_name = "_cpp_noop_builder";
        static void eval(hgraph::Node&) {}
    };
}

void export_nodes(nb::module_ &m) {
    using namespace hgraph;

    bind_cpp_node_builder_factories(m, CppNodeSpecList<CppNoopSpec>{});
}
