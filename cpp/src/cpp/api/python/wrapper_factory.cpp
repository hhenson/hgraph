//
// Wrapper Factory Implementation
//

#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <ddv/serial_visitor.h>
#include <stdexcept>
#include <utility>

namespace hgraph
{
namespace
{
    template<typename T, typename U>
    nb::object create_wrapper_from_api(auto api_ptr) {
        auto sp{api_ptr.template control_block_typed<U>()};
        auto ptr{ApiPtr<U>(std::move(sp))};
        return nb::cast(T{std::move(ptr)});
    }

    static auto node_v = ddv::serial{
        [](LastValuePullNode*, ApiPtr<Node> ptr) {
            return create_wrapper_from_api<PyLastValuePullNode, LastValuePullNode>(std::move(ptr));
        },
        [](PushQueueNode*, ApiPtr<Node> ptr) {
            return create_wrapper_from_api<PyPushQueueNode, PushQueueNode>(std::move(ptr));
        },
        // Mesh nodes
        []<typename T>(MeshNode<T>*, ApiPtr<Node> ptr) {
            return nb::cast(PyMeshNestedNode::make_mesh_node<T>(std::move(ptr)));
        },
        // Other nested nodes
        [](NestedNode*, ApiPtr<Node> ptr) {
            return create_wrapper_from_api<PyNestedNode, NestedNode>(std::move(ptr));
        },
        // Default to base PyNode
        [](auto, ApiPtr<Node> ptr) { return nb::cast(PyNode(std::move(ptr))); }
    };

} // hidden namespace

    // Main implementation - takes ApiPtr<Node>
    nb::object wrap_node(PyNode::api_ptr impl) {
        if (!impl) { return nb::none(); }
        return *impl->visit(node_v, impl);
    }

    // Overload for shared_ptr
    nb::object wrap_node(const node_s_ptr &impl) { return wrap_node(PyNode::api_ptr(impl)); }

    nb::object wrap_graph(const Graph::s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyGraph(PyGraph::api_ptr(impl)));
    }

    nb::object wrap_node_scheduler(const NodeScheduler::s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl)));
    }

    // =========================================================================
    // Time-Series Wrapper Implementations
    // =========================================================================

    nb::object wrap_output(ts::TSOutput* output, const node_s_ptr& node) {
        if (!output || !node) { return nb::none(); }

        auto view = output->view();
        auto meta = output->meta();

        // TODO: Use meta->as_api() for specialized wrapper dispatch
        // For now, create the base wrapper
        return nb::cast(PyTimeSeriesOutput(node, std::move(view), output, meta));
    }

    nb::object wrap_input(ts::TSInput* input, const node_s_ptr& node) {
        if (!input || !node) { return nb::none(); }

        auto view = input->view();
        auto meta = input->meta();

        // TODO: Use meta->as_api() for specialized wrapper dispatch
        // For now, create the base wrapper
        return nb::cast(PyTimeSeriesInput(node, std::move(view), input, meta));
    }

    // =========================================================================
    // Unwrap Functions
    // =========================================================================

    node_s_ptr unwrap_node(const nb::handle &obj) {
        if (auto *py_node = nb::inst_ptr<PyNode>(obj)) { return unwrap_node(*py_node); }
        return {};
    }

    node_s_ptr unwrap_node(const PyNode &node_) { return node_._impl.control_block_typed<Node>(); }

    graph_s_ptr unwrap_graph(const nb::handle &obj) {
        if (auto *py_graph = nb::inst_ptr<PyGraph>(obj)) { return unwrap_graph(*py_graph); }
        return nullptr;
    }

    graph_s_ptr unwrap_graph(const PyGraph &graph_) { return graph_._impl.control_block_typed<Graph>(); }

    ts::TSInput* unwrap_input(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return nullptr;
    }

    ts::TSInput* unwrap_input(const PyTimeSeriesInput &input_) { return input_.input(); }

    node_s_ptr unwrap_output_node(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output_node(*py_output); }
        return nullptr;
    }

    node_s_ptr unwrap_output_node(const PyTimeSeriesOutput &output_) {
        return output_.node();
    }

    ts::TSOutput* unwrap_output(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return nullptr;
    }

    ts::TSOutput* unwrap_output(const PyTimeSeriesOutput &output_) {
        return output_.output();
    }

    nb::object wrap_time_series(ts::TSOutput* output) {
        // This legacy helper is called from nodes that should have access to their node pointer.
        // For now, return none if we can't determine the node.
        // TODO: The caller should be updated to use wrap_output(output, node) instead.
        if (!output) { return nb::none(); }
        // Without a node pointer, we can't create a proper wrapper
        // Return none - caller should be updated to use wrap_output with node
        throw std::runtime_error("wrap_time_series(TSOutput*) requires node context - use wrap_output(output, node) instead");
    }

    nb::object wrap_time_series(const time_series_output_s_ptr& output) {
        // V1 compatibility: Wrap a shared_ptr<TimeSeriesOutput> using V1 Python wrapper
        // This is used by V1 types like TSD that are still compiled
        if (!output) { return nb::none(); }
        // In V2 mode, we don't have V1 Python wrappers.
        // This function is only called from V1 types (like TSD) which shouldn't
        // be actively used in V2 mode.
        throw std::runtime_error("wrap_time_series(shared_ptr) not supported in V2 mode - V1 types should not be used");
    }

    nb::object wrap_evaluation_engine_api(EvaluationEngineApi::s_ptr impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyEvaluationEngineApi(PyEvaluationEngineApi::api_ptr(std::move(impl))));
    }

    nb::object wrap_evaluation_clock(EvaluationClock::s_ptr impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyEvaluationClock(PyEvaluationClock::api_ptr(std::move(impl))));
    }

    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyTraits(PyTraits::api_ptr(impl, control_block)));
    }

}  // namespace hgraph
