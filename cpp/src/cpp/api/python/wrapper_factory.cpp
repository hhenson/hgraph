//
// Wrapper Factory Implementation
//

#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/py_signal.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_tsw.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
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

    using node_vt = decltype(node_v);

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

    // ========== View-Based Factory Functions ==========

    nb::object wrap_output_view(TSOutputView view) {
        if (!view) { return nb::none(); }

        const auto* meta = view.ts_meta();
        if (!meta) {
            throw std::runtime_error("wrap_output_view: TSOutputView has no TSMeta");
        }

        switch (meta->kind) {
            case TSKind::TSValue:
                return nb::cast(PyTimeSeriesValueOutput(std::move(view)));

            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(std::move(view)));

            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListOutput(std::move(view)));

            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictOutput(std::move(view)));

            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetOutput(std::move(view)));

            case TSKind::TSW:
                if (meta->is_duration_based()) {
                    return nb::cast(PyTimeSeriesTimeWindowOutput(std::move(view)));
                } else {
                    return nb::cast(PyTimeSeriesFixedWindowOutput(std::move(view)));
                }

            case TSKind::REF:
                return nb::cast(PyTimeSeriesReferenceOutput(std::move(view)));

            case TSKind::SIGNAL:
                // SIGNAL is input-only, there's no output type
                throw std::runtime_error("wrap_output_view: SIGNAL is input-only, no output type exists");

            default:
                throw std::runtime_error(
                    fmt::format("wrap_output_view: Unknown TSKind {}", static_cast<int>(meta->kind))
                );
        }
    }

    nb::object wrap_input_view(TSInputView view) {
        if (!view) { return nb::none(); }

        const auto* meta = view.ts_meta();
        if (!meta) {
            throw std::runtime_error("wrap_input_view: TSInputView has no TSMeta");
        }

        return wrap_input_view(std::move(view), meta);
    }

    nb::object wrap_input_view(TSInputView view, const TSMeta* effective_meta) {
        if (!view) { return nb::none(); }
        if (!effective_meta) {
            return wrap_input_view(std::move(view));
        }

        switch (effective_meta->kind) {
            case TSKind::TSValue:
                return nb::cast(PyTimeSeriesValueInput(std::move(view)));

            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(std::move(view)));

            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListInput(std::move(view)));

            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictInput(std::move(view)));

            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetInput(std::move(view)));

            case TSKind::TSW:
                return nb::cast(PyTimeSeriesWindowInput(std::move(view)));

            case TSKind::REF:
                return nb::cast(PyTimeSeriesReferenceInput(std::move(view)));

            case TSKind::SIGNAL:
                return nb::cast(PyTimeSeriesSignalInput(std::move(view)));

            default:
                throw std::runtime_error(
                    fmt::format("wrap_input_view: Unknown TSKind {}", static_cast<int>(effective_meta->kind))
                );
        }
    }

    TSInputView unwrap_input_view(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) {
            return py_input->input_view();
        }
        return {};
    }

    TSOutputView unwrap_output_view(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) {
            return py_output->output_view();
        }
        return {};
    }

}  // namespace hgraph
