//
// Wrapper Factory Implementation
//

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

    // Helper to create the appropriate output wrapper based on TSKind
    nb::object create_output_wrapper(node_s_ptr node, value::TSView view, ts::TSOutput* output, const TSMeta* meta) {
        if (!meta) {
            return nb::cast(PyTimeSeriesOutput(std::move(node), std::move(view), output, meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(std::move(node), std::move(view), output, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(std::move(node), std::move(view), output, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListOutput(std::move(node), std::move(view), output, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetOutput(std::move(node), std::move(view), output, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictOutput(std::move(node), std::move(view), output, meta));
            case TSKind::TSW:
                return nb::cast(PyTimeSeriesWindowOutput(std::move(node), std::move(view), output, meta));
            case TSKind::REF:
                // REF types use the base output wrapper (TimeSeriesReference is handled as a value)
                // Fall through intentional
            default:
                return nb::cast(PyTimeSeriesOutput(std::move(node), std::move(view), output, meta));
        }
    }

    // Helper to create the appropriate input wrapper based on TSKind
    nb::object create_input_wrapper(node_s_ptr node, ts::TSInputView view, ts::TSInput* input, const TSMeta* meta) {
        if (!meta) {
            return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), input, meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueInput(std::move(node), std::move(view), input, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(std::move(node), std::move(view), input, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListInput(std::move(node), std::move(view), input, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetInput(std::move(node), std::move(view), input, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictInput(std::move(node), std::move(view), input, meta));
            case TSKind::TSW:
                return nb::cast(PyTimeSeriesWindowInput(std::move(node), std::move(view), input, meta));
            case TSKind::REF:
                // REF types use the base input wrapper (TimeSeriesReference is handled as a value)
            default:
                return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), input, meta));
        }
    }

    // Helper to create input wrapper for field views with binding support
    // root_input and field_index enable binding via TSInputBindableView
    nb::object create_field_wrapper(node_s_ptr node, ts::TSInputView view, const TSMeta* meta,
                                     ts::TSInput* root_input = nullptr, size_t field_index = SIZE_MAX) {
        // All field wrappers need root_input and field_index for binding support
        // The subtype wrappers inherit from PyTimeSeriesInput which stores these

        if (!meta) {
            if (root_input && field_index != SIZE_MAX) {
                return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), root_input, field_index, meta));
            }
            return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), meta));
        }

        // For all types, use the bindable constructor variant that includes root_input and field_index
        // The subtype wrappers inherit constructors from PyTimeSeriesInput via "using PyTimeSeriesInput::PyTimeSeriesInput"
        switch (meta->ts_kind) {
            case TSKind::TS:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesValueInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesValueInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::TSB:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesBundleInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesBundleInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::TSL:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesListInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesListInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::TSS:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesSetInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesSetInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::TSD:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesDictInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesDictInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::TSW:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesWindowInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesWindowInput(std::move(node), std::move(view), nullptr, meta));
            case TSKind::REF:
            default:
                if (root_input && field_index != SIZE_MAX) {
                    return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), root_input, field_index, meta));
                }
                return nb::cast(PyTimeSeriesInput(std::move(node), std::move(view), meta));
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
        if (!meta || meta->ts_kind != TSKind::TSB) {
            return nb::none();
        }

        // Get the field's meta and index from the bundle meta
        auto* bundle_meta = static_cast<const TSBTypeMeta*>(meta);
        auto* field_meta = bundle_meta->field_meta(field_name);
        size_t field_index = bundle_meta->field_index(field_name);

        // Get the root view - it points to the strategy and navigates to child strategies
        auto root_view = input->view();
        if (!root_view.valid()) {
            // Even if root view is invalid, create a wrapper with invalid view
            // but include root_input and field_index for potential binding
            ts::TSInputView invalid_view{};
            return create_field_wrapper(node, std::move(invalid_view), field_meta, input, field_index);
        }

        // Navigate to the field - this returns a view pointing to the child strategy
        // The view handles all the navigation and fetches fresh data on each access
        auto field_view = root_view.field(field_name);

        // IMPORTANT: Even if field_view is invalid (optional input not wired),
        // still create a wrapper. Python code expects to call .valid on it.
        // The wrapper's .valid will correctly return False for invalid views.
        // Pass root_input and field_index for binding support (used by TimeSeriesReference::bind_input)
        return create_field_wrapper(node, std::move(field_view), field_meta, input, field_index);
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
