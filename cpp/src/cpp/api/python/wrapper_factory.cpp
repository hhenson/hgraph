//
// Wrapper Factory - non-time-series wrappers and utility unwrappers
//

#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

#include <utility>

namespace hgraph {
namespace {

static auto node_v = ddv::serial{
    [](LastValuePullNode*, ApiPtr<Node> ptr) {
        return nb::cast(PyLastValuePullNode(std::move(ptr)));
    },
    [](PushQueueNode*, ApiPtr<Node> ptr) {
        return nb::cast(PyPushQueueNode(std::move(ptr)));
    },
    // Mesh nodes - now non-templated
    [](MeshNode*, ApiPtr<Node> ptr) {
        return nb::cast(PyMeshNestedNode::make_mesh_node(std::move(ptr)));
    },
    // Switch nodes - now non-templated, wraps as PyNestedNode
    [](SwitchNode*, ApiPtr<Node> ptr) {
        return nb::cast(PyNestedNode(std::move(ptr)));
    },
    // Other nested nodes
    [](NestedNode*, ApiPtr<Node> ptr) {
        return nb::cast(PyNestedNode(std::move(ptr)));
    },
    // Default to base PyNode
    [](auto, ApiPtr<Node> ptr) { return nb::cast(PyNode(std::move(ptr))); }
};

}  // namespace

nb::object wrap_node(PyNode::api_ptr impl) {
    if (!impl) {
        return nb::none();
    }
    return *impl->visit(node_v, impl);
}

nb::object wrap_node(const node_s_ptr& impl) {
    return wrap_node(PyNode::api_ptr(impl));
}

nb::object wrap_graph(const Graph::s_ptr& impl) {
    if (!impl) {
        return nb::none();
    }
    return nb::cast(PyGraph(PyGraph::api_ptr(impl)));
}

nb::object wrap_node_scheduler(const NodeScheduler::s_ptr& impl) {
    if (!impl) {
        return nb::none();
    }
    return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl)));
}

node_s_ptr unwrap_node(const nb::handle& obj) {
    if (auto* py_node = nb::inst_ptr<PyNode>(obj)) {
        return unwrap_node(*py_node);
    }
    return {};
}

node_s_ptr unwrap_node(const PyNode& node_) {
    return node_._impl.control_block_typed<Node>();
}

graph_s_ptr unwrap_graph(const nb::handle& obj) {
    if (auto* py_graph = nb::inst_ptr<PyGraph>(obj)) {
        return unwrap_graph(*py_graph);
    }
    return nullptr;
}

graph_s_ptr unwrap_graph(const PyGraph& graph_) {
    return graph_._impl.control_block_typed<Graph>();
}

nb::object wrap_evaluation_engine_api(EvaluationEngineApi::s_ptr impl) {
    if (!impl) {
        return nb::none();
    }
    return nb::cast(PyEvaluationEngineApi(PyEvaluationEngineApi::api_ptr(std::move(impl))));
}

nb::object wrap_evaluation_clock(EvaluationClock::s_ptr impl) {
    if (!impl) {
        return nb::none();
    }
    return nb::cast(PyEvaluationClock(PyEvaluationClock::api_ptr(std::move(impl))));
}

nb::object wrap_traits(const Traits* impl, const control_block_ptr& control_block) {
    if (!impl) {
        return nb::none();
    }
    return nb::cast(PyTraits(PyTraits::api_ptr(impl, control_block)));
}

TSInputView unwrap_input_view(const nb::handle& obj) {
    if (auto* py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) {
        return py_input->input_view();
    }
    return {};
}

TSOutputView unwrap_output_view(const nb::handle& obj) {
    if (auto* py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) {
        return py_output->output_view();
    }
    return {};
}

}  // namespace hgraph
