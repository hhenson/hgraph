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
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
#include <cstdio>
#include <cstdlib>
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

    using output_view_wrapper_fn = nb::object (*)(TSOutputView&& view, const TSMeta* meta);
    using input_view_wrapper_fn = nb::object (*)(TSInputView&& view);

    constexpr size_t k_ts_kind_count = static_cast<size_t>(TSKind::SIGNAL) + size_t{1};

    TSKind dispatch_kind(const TSMeta* meta) {
        if (const ts_ops* ops = get_ts_ops(meta); ops != nullptr) {
            return ops->kind;
        }
        return meta != nullptr ? meta->kind : TSKind::TSValue;
    }

    nb::object wrap_output_tsvalue(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesValueOutput(std::move(view)));
    }

    nb::object wrap_output_tsb(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesBundleOutput(std::move(view)));
    }

    nb::object wrap_output_tsl(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesListOutput(std::move(view)));
    }

    nb::object wrap_output_tsd(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesDictOutput(std::move(view)));
    }

    nb::object wrap_output_tss(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesSetOutput(std::move(view)));
    }

    nb::object wrap_output_tsw(TSOutputView&& view, const TSMeta* meta) {
        if (meta != nullptr && meta->is_duration_based()) {
            return nb::cast(PyTimeSeriesTimeWindowOutput(std::move(view)));
        }
        return nb::cast(PyTimeSeriesFixedWindowOutput(std::move(view)));
    }

    nb::object wrap_output_ref(TSOutputView&& view, const TSMeta*) {
        return nb::cast(PyTimeSeriesReferenceOutput(std::move(view)));
    }

    nb::object wrap_output_signal(TSOutputView&&, const TSMeta*) {
        throw std::runtime_error("wrap_output_view: SIGNAL is input-only, no output type exists");
    }

    constexpr output_view_wrapper_fn k_output_view_wrappers[k_ts_kind_count] = {
        &wrap_output_tsvalue,  // TSKind::TSValue
        &wrap_output_tss,      // TSKind::TSS
        &wrap_output_tsd,      // TSKind::TSD
        &wrap_output_tsl,      // TSKind::TSL
        &wrap_output_tsw,      // TSKind::TSW
        &wrap_output_tsb,      // TSKind::TSB
        &wrap_output_ref,      // TSKind::REF
        &wrap_output_signal,   // TSKind::SIGNAL
    };

    static_assert((sizeof(k_output_view_wrappers) / sizeof(k_output_view_wrappers[0])) == k_ts_kind_count,
                  "k_output_view_wrappers must cover all TSKind values");

    const char* output_wrapper_debug_name(TSKind kind, const TSMeta* meta) {
        if (kind == TSKind::TSW) {
            return meta != nullptr && meta->is_duration_based() ? "TimeSeriesTimeWindowOutput" : "TimeSeriesFixedWindowOutput";
        }

        constexpr const char* k_names[k_ts_kind_count] = {
            "TimeSeriesValueOutput",      // TSKind::TSValue
            "TimeSeriesSetOutput",        // TSKind::TSS
            "TimeSeriesDictOutput",       // TSKind::TSD
            "TimeSeriesListOutput",       // TSKind::TSL
            "TimeSeriesFixedWindowOutput",// TSKind::TSW (non-duration fallback)
            "TimeSeriesBundleOutput",     // TSKind::TSB
            "TimeSeriesReferenceOutput",  // TSKind::REF
            "SignalOutputUnsupported",    // TSKind::SIGNAL
        };
        const size_t index = static_cast<size_t>(kind);
        return index < k_ts_kind_count ? k_names[index] : "UnknownOutput";
    }

    nb::object wrap_input_tsvalue(TSInputView&& view) {
        return nb::cast(PyTimeSeriesValueInput(std::move(view)));
    }

    nb::object wrap_input_tss(TSInputView&& view) {
        return nb::cast(PyTimeSeriesSetInput(std::move(view)));
    }

    nb::object wrap_input_tsd(TSInputView&& view) {
        return nb::cast(PyTimeSeriesDictInput(std::move(view)));
    }

    nb::object wrap_input_tsl(TSInputView&& view) {
        return nb::cast(PyTimeSeriesListInput(std::move(view)));
    }

    nb::object wrap_input_tsw(TSInputView&& view) {
        return nb::cast(PyTimeSeriesWindowInput(std::move(view)));
    }

    nb::object wrap_input_tsb(TSInputView&& view) {
        return nb::cast(PyTimeSeriesBundleInput(std::move(view)));
    }

    nb::object wrap_input_ref(TSInputView&& view) {
        return nb::cast(PyTimeSeriesReferenceInput(std::move(view)));
    }

    nb::object wrap_input_signal(TSInputView&& view) {
        return nb::cast(PyTimeSeriesSignalInput(std::move(view)));
    }

    constexpr input_view_wrapper_fn k_input_view_wrappers[k_ts_kind_count] = {
        &wrap_input_tsvalue,  // TSKind::TSValue
        &wrap_input_tss,      // TSKind::TSS
        &wrap_input_tsd,      // TSKind::TSD
        &wrap_input_tsl,      // TSKind::TSL
        &wrap_input_tsw,      // TSKind::TSW
        &wrap_input_tsb,      // TSKind::TSB
        &wrap_input_ref,      // TSKind::REF
        &wrap_input_signal,   // TSKind::SIGNAL
    };

    static_assert((sizeof(k_input_view_wrappers) / sizeof(k_input_view_wrappers[0])) == k_ts_kind_count,
                  "k_input_view_wrappers must cover all TSKind values");

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

        const TSKind kind = dispatch_kind(meta);
        const bool debug_wrap_output = std::getenv("HGRAPH_DEBUG_WRAP_OUTPUT") != nullptr;
        auto debug_emit = [&](const char* wrapped) {
            if (!debug_wrap_output) {
                return;
            }
            std::fprintf(stderr,
                         "[wrap_output] path=%s kind=%d wrapped=%s\n",
                         view.short_path().to_string().c_str(),
                         static_cast<int>(kind),
                         wrapped);
        };

        const size_t index = static_cast<size_t>(kind);
        if (index >= k_ts_kind_count || k_output_view_wrappers[index] == nullptr) {
            throw std::runtime_error(
                fmt::format("wrap_output_view: Unknown TSKind {}", static_cast<int>(kind))
            );
        }

        debug_emit(output_wrapper_debug_name(kind, meta));
        return k_output_view_wrappers[index](std::move(view), meta);
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

        const TSKind kind = dispatch_kind(effective_meta);
        const size_t index = static_cast<size_t>(kind);
        if (index < k_ts_kind_count && k_input_view_wrappers[index] != nullptr) {
            return k_input_view_wrappers[index](std::move(view));
        }

        throw std::runtime_error(
            fmt::format("wrap_input_view: Unknown TSKind {}", static_cast<int>(kind))
        );
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
