//
// Wrapper Factory Implementation
//
// NOTE: Legacy ApiPtr-based time-series wrapping has been removed.
// All time-series wrapping now uses the view-based system (TSView/TSMutableView).
//

#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <stdexcept>
#include <utility>

namespace hgraph
{
namespace
{
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

    // ========================================================================
    // TSView-based wrapping (the only supported system)
    // ========================================================================

    nb::object wrap_input_view(const TSView& view) {
        if (!view.valid()) { return nb::none(); }

        const auto* meta = view.ts_meta();
        if (!meta) { return nb::none(); }

        switch (meta->kind()) {
            case TSTypeKind::TS:
                return nb::cast(PyTimeSeriesValueInput(view));
            case TSTypeKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(view));
            case TSTypeKind::TSL:
            case TSTypeKind::TSD:
            case TSTypeKind::TSS:
            case TSTypeKind::TSW:
            case TSTypeKind::REF:
            case TSTypeKind::SIGNAL:
                // For now, wrap as base input - these will need specialized wrappers
                return nb::cast(PyTimeSeriesInput(view));
            default:
                throw std::runtime_error("wrap_input_view: Unknown TSTypeKind");
        }
    }

    nb::object wrap_output_view(TSMutableView view) {
        if (!view.valid()) { return nb::none(); }

        const auto* meta = view.ts_meta();
        if (!meta) { return nb::none(); }

        switch (meta->kind()) {
            case TSTypeKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(view));
            case TSTypeKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(view));
            case TSTypeKind::TSL:
            case TSTypeKind::TSD:
            case TSTypeKind::TSS:
            case TSTypeKind::TSW:
            case TSTypeKind::REF:
            case TSTypeKind::SIGNAL:
                // For now, wrap as base output - these will need specialized wrappers
                return nb::cast(PyTimeSeriesOutput(view));
            default:
                throw std::runtime_error("wrap_output_view: Unknown TSTypeKind");
        }
    }

    nb::object wrap_bundle_input_view(const TSBView& view) {
        if (!view.valid()) { return nb::none(); }
        // TSBView inherits from TSView, so we can pass it directly
        return nb::cast(PyTimeSeriesBundleInput(view));
    }

    nb::object wrap_bundle_output_view(TSBView view) {
        if (!view.valid()) { return nb::none(); }
        // TSBView is immutable, so for output we need to construct a TSMutableView
        // from the same underlying data. Use the value_view data pointer.
        const void* data = view.value_view().data();
        TSMutableView mv(const_cast<void*>(data), view.ts_meta(), view.overlay());
        return nb::cast(PyTimeSeriesBundleOutput(mv));
    }

}  // namespace hgraph
