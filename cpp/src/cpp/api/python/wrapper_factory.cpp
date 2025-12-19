//
// Wrapper Factory Implementation
//

#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_tsw.h>
#include <hgraph/api/python/py_ref.h>
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
#include <hgraph/types/time_series/ts_type_meta.h>
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

    // Helper to create the appropriate output wrapper based on TimeSeriesKind
    nb::object create_output_wrapper(node_s_ptr node, ts::TSOutputView view, ts::TSOutput* output, const TimeSeriesTypeMeta* meta) {
        if (!meta) {
            return nb::cast(PyTimeSeriesOutput(std::move(node), std::move(view), output, meta));
        }

        switch (meta->ts_kind) {
            case TimeSeriesKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::TSL:
                return nb::cast(PyTimeSeriesListOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::TSS:
                return nb::cast(PyTimeSeriesSetOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::TSD:
                return nb::cast(PyTimeSeriesDictOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::TSW:
                return nb::cast(PyTimeSeriesWindowOutput(std::move(node), std::move(view), output, meta));
            case TimeSeriesKind::REF:
                return nb::cast(PyTimeSeriesReferenceOutput(std::move(node), std::move(view), output, meta));
            default:
                return nb::cast(PyTimeSeriesOutput(std::move(node), std::move(view), output, meta));
        }
    }

    // Helper to create the appropriate input wrapper based on TimeSeriesKind
    nb::object create_input_wrapper(node_s_ptr node, ts::TSInputView view, ts::TSInput* input, const TimeSeriesTypeMeta* meta) {
        if (!meta) {
            return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), input, meta));
        }

        switch (meta->ts_kind) {
            case TimeSeriesKind::TS:
                return nb::cast(PyTimeSeriesValueInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::TSL:
                return nb::cast(PyTimeSeriesListInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::TSS:
                return nb::cast(PyTimeSeriesSetInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::TSD:
                return nb::cast(PyTimeSeriesDictInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::TSW:
                return nb::cast(PyTimeSeriesWindowInput(std::move(node), std::move(view), input, meta));
            case TimeSeriesKind::REF:
                return nb::cast(PyTimeSeriesReferenceInput(std::move(node), std::move(view), input, meta));
            default:
                return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), input, meta));
        }
    }

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

        return create_output_wrapper(node, std::move(view), output, meta);
    }

    nb::object wrap_input(ts::TSInput* input, const node_s_ptr& node) {
        if (!input || !node) { return nb::none(); }

        auto view = input->view();
        auto meta = input->meta();

        return create_input_wrapper(node, std::move(view), input, meta);
    }

    nb::object wrap_input_field(ts::TSInput* input, const std::string& field_name, const node_s_ptr& node) {
        if (!input || !node) {
            return nb::none();
        }

        auto* meta = input->meta();
        if (!meta || meta->ts_kind != TimeSeriesKind::TSB) {
            return nb::none();
        }

        // Find field index by name using TSBTypeMeta
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(meta);
        size_t field_idx = 0;
        bool found = false;
        for (const auto& field : tsb_meta->fields) {
            if (field.name == field_name) {
                found = true;
                break;
            }
            field_idx++;
        }

        if (!found) {
            return nb::none();
        }

        // Get field metadata
        const TimeSeriesTypeMeta* field_meta = field_idx < tsb_meta->fields.size()
            ? tsb_meta->fields[field_idx].type
            : nullptr;

        // Try to get field view via strategy if available
        if (auto* strategy = input->strategy()) {
            // Check if strategy is a CollectionAccessStrategy with child for this field
            if (auto* collection = dynamic_cast<ts::CollectionAccessStrategy*>(strategy)) {
                if (auto* child_strategy = collection->child(field_idx)) {
                    // Create view from child strategy
                    auto child_value = child_strategy->value();
                    auto child_tracker = child_strategy->tracker();
                    ts::TSInputView field_view(child_value, child_tracker, field_meta,
                                               ts::NavigationPath(nullptr));
                    // Note: pass nullptr for input since field wrappers don't support make_active/make_passive
                    return create_input_wrapper(node, std::move(field_view), nullptr, field_meta);
                }
            }
        }

        // Fallback to view-based navigation (for whole-input binding or unbound fields)
        auto root_view = input->view();
        if (!root_view.valid()) { return nb::none(); }

        auto field_view = root_view.field(field_name);
        if (!field_view.valid()) { return nb::none(); }

        // Create a wrapper for the field view
        // Note: pass nullptr for input since field views don't support make_active/make_passive
        return create_input_wrapper(node, std::move(field_view), nullptr, field_view.meta());
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
        // Wrap a shared_ptr<TimeSeriesOutput> - this is for legacy time-series types
        if (!output) { return nb::none(); }
        // This function is only called from legacy types which are not actively used
        throw std::runtime_error("wrap_time_series(shared_ptr) not supported - legacy types should not be used");
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
