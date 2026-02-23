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
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
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

    static constexpr auto ts_opaque_input_types_v =
        tp::tpack_v<TimeSeriesSignalInput, TimeSeriesListInput,
                    TimeSeriesBundleInput  //, IndexedTimeSeriesInput (not implemented yet?)
                    > +
        ts_reference_input_types_v;
    // [NOTE] order must match `ts_opaque_input_types`
    using py_ts_opaque_input_types =
        tp::tpack<PyTimeSeriesSignalInput, PyTimeSeriesListInput, PyTimeSeriesBundleInput,  //, PyIndexedTimeSeriesInput
                  PyTimeSeriesReferenceInput, PyTimeSeriesValueReferenceInput, PyTimeSeriesWindowReferenceInput,
                  PyTimeSeriesListReferenceInput, PyTimeSeriesSetReferenceInput, PyTimeSeriesDictReferenceInput,
                  PyTimeSeriesBundleReferenceInput>;

    static constexpr auto input_v = ddv::serial{
        // Non-templated TSD input
        [](TimeSeriesDictInputImpl*, ApiPtr<TimeSeriesInput> impl) {
            return create_wrapper_from_api<PyTimeSeriesDictInput, TimeSeriesDictInputImpl>(std::move(impl));
        },
        // Non-templated TSS input
        [](TimeSeriesSetInput*, ApiPtr<TimeSeriesInput> impl) {
            return create_wrapper_from_api<PyTimeSeriesSetInput, TimeSeriesSetInput>(std::move(impl));
        },
        // Non-templated TSW input
        [](TimeSeriesWindowInput*, ApiPtr<TimeSeriesInput> impl) {
            return create_wrapper_from_api<PyTimeSeriesWindowInput, TimeSeriesWindowInput>(std::move(impl));
        },
        // value inputs
        [](TimeSeriesValueInputBase*, ApiPtr<TimeSeriesInput> impl) {
            return nb::cast(PyTimeSeriesValueInput(std::move(impl)));
        },
        // opaque inputs
        []<typename TS>(TS*, ApiPtr<TimeSeriesInput> impl)
            requires(tp::contains<TS>(ts_opaque_input_types_v))
        {
            if constexpr (std::is_same_v<TS, TimeSeriesReferenceInput>) {
                // DEFENSIVE: This branch handles the base TimeSeriesReferenceInput type in the visitor pattern.
                // Since TimeSeriesReferenceInput is an abstract class (clone_blank_ref_instance is pure virtual),
                // this should never be triggered - all actual instances will be specialized types.
                // This guard exists to catch any unexpected visitor pattern matching issues.
                throw std::runtime_error(
                    "Python wrap input TS: Encountered a base TimeSeriesReferenceInput "
                    "that doesn't match any specialized type. This should not happen since "
                    "TimeSeriesReferenceInput is abstract. Check the visitor pattern matching logic.");
            } else {
                using py_type = tp::get_t<tp::find<TS>(ts_opaque_input_types_v), py_ts_opaque_input_types>;
                return create_wrapper_from_api<py_type, TS>(std::move(impl));
            }
        },
        [] { return nb::none(); }};


    static constexpr auto ts_opaque_output_types_v =
        tp::tpack_v<TimeSeriesListOutput, TimeSeriesBundleOutput  // IndexedTimeSeriesOutput
                    > +
        ts_reference_output_types_v;
    // [NOTE] order must match `ts_opaque_output_types_v`
    using py_ts_opaque_output_types =
        tp::tpack<PyTimeSeriesListOutput, PyTimeSeriesBundleOutput,  // PyIndexedTimeSeriesOutput
                  PyTimeSeriesReferenceOutput, PyTimeSeriesValueReferenceOutput, PyTimeSeriesWindowReferenceOutput,
                  PyTimeSeriesListReferenceOutput, PyTimeSeriesSetReferenceOutput, PyTimeSeriesDictReferenceOutput,
                  PyTimeSeriesBundleReferenceOutput>;

    static constexpr auto output_v = ddv::serial{
        // Non-templated TSD output
        [](TimeSeriesDictOutputImpl*, ApiPtr<TimeSeriesOutput> impl) {
            return create_wrapper_from_api<PyTimeSeriesDictOutput, TimeSeriesDictOutputImpl>(std::move(impl));
        },
        // Non-templated TSS output
        [](TimeSeriesSetOutput*, ApiPtr<TimeSeriesOutput> impl) {
            return create_wrapper_from_api<PyTimeSeriesSetOutput, TimeSeriesSetOutput>(std::move(impl));
        },
        // Non-templated TSW fixed window output
        [](TimeSeriesFixedWindowOutput*, ApiPtr<TimeSeriesOutput> impl) {
            return create_wrapper_from_api<PyTimeSeriesFixedWindowOutput, TimeSeriesFixedWindowOutput>(std::move(impl));
        },
        // Non-templated TSW time window output
        [](TimeSeriesTimeWindowOutput*, ApiPtr<TimeSeriesOutput> impl) {
            return create_wrapper_from_api<PyTimeSeriesTimeWindowOutput, TimeSeriesTimeWindowOutput>(std::move(impl));
        },
        // value outputs
        [](TimeSeriesValueOutputBase*, ApiPtr<TimeSeriesOutput> impl) {
            return nb::cast(PyTimeSeriesValueOutput(std::move(impl)));
        },
        // opaque outputs
        []<typename TS>(TS*, ApiPtr<TimeSeriesOutput> impl)
            requires(tp::contains<TS>(ts_opaque_output_types_v))
        {
            if constexpr (std::is_same_v<TS, TimeSeriesReferenceOutput>) {
                throw std::runtime_error(
                    "Python wrap output TS: Encountered a base TimeSeriesReferenceOutput "
                    "that doesn't match any specialized type. This is a bug - there should not be naked instances of "
                    "TimeSeriesReferenceOutput. Check where this output was created (likely in reduce_node.cpp::zero_node).");
            } else {
                using py_type = tp::get_t<tp::find<TS>(ts_opaque_output_types_v), py_ts_opaque_output_types>;
                return create_wrapper_from_api<py_type, TS>(std::move(impl));
            }
        },
        [] { return nb::none(); }};


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
        [](NestedGraphNode*, ApiPtr<Node> ptr) {
            return nb::cast(PyNestedNode(std::move(ptr)));
        },
        [](TryExceptNode*, ApiPtr<Node> ptr) {
            return nb::cast(PyNestedNode(std::move(ptr)));
        },
        // Default to base PyNode
        // [NOTE] first arg must be `auto`, not `Node*`: in latter case compiler needs complete nodes definitions
        [](auto, ApiPtr<Node> ptr) { return nb::cast(PyNode(std::move(ptr))); }
    };

    using node_vt = decltype(node_v);

} // hidden namespace

    /**
     * Try to create a wrapper of PyType if the api_ptr can be cast to UnderlyerType.
     * Returns std::nullopt if the cast fails.
     */
    template <typename PyType, typename UnderlyerType, typename ApiPtrType> std::optional<nb::object> try_create(ApiPtrType &impl) {
        if (impl.template dynamic_cast_<UnderlyerType>()) { return nb::cast(PyType(std::move(impl))); }
        return std::nullopt;
    }

    /**
     * Try to create a mesh node wrapper if the api_ptr can be cast to MeshNode.
     * Returns std::nullopt if the cast fails.
     */
    template <typename ApiPtrType> std::optional<nb::object> try_create_mesh_node(ApiPtrType &impl) {
        if (impl.template dynamic_cast_<MeshNode>()) { return nb::cast(PyMeshNestedNode::make_mesh_node(std::move(impl))); }
        return std::nullopt;
    }

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

    nb::object wrap_node_scheduler(const NodeScheduler *impl, const control_block_ptr &control_block) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl, control_block)));
    }

    nb::object wrap_node_scheduler(const NodeScheduler::s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl)));
    }

    nb::object wrap_input(ApiPtr<TimeSeriesInput> impl) {
        if (!impl) { return nb::none(); }
        return *impl->visit(input_v, impl);
    }

    nb::object wrap_output(ApiPtr<TimeSeriesOutput> impl) {
        if (!impl) { return nb::none(); }
        return *impl->visit(output_v, impl);
    }

    nb::object wrap_time_series(ApiPtr<TimeSeriesInput> impl) { return wrap_input(std::move(impl)); }

    nb::object wrap_time_series(ApiPtr<TimeSeriesOutput> impl) { return wrap_output(std::move(impl)); }

    nb::object wrap_output(const time_series_output_s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return wrap_output(ApiPtr<TimeSeriesOutput>(impl));
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

    time_series_input_s_ptr unwrap_input(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return {};
    }

    time_series_input_s_ptr unwrap_input(const PyTimeSeriesInput &input_) { return input_.impl_s_ptr<TimeSeriesInput>(); }

    time_series_output_s_ptr unwrap_output(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return {};
    }

    time_series_output_s_ptr unwrap_output(const PyTimeSeriesOutput &output_) { return output_.impl_s_ptr<TimeSeriesOutput>(); }

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
