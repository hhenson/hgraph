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
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/nodes/push_queue_node.h>
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

    /**
     * Generic wrapper caching function.
     * Checks for cached wrapper via intrusive_base::self_py(), creates if needed.
     *
     * @tparam ImplType The C++ implementation type (must inherit from intrusive_base)
     * @tparam Creator Function/lambda type that creates the wrapper
     * @param impl Pointer to the C++ implementation object
     * @param control_block Control block for lifetime tracking
     * @param creator Function that creates the wrapper: (ImplType*, control_block_ptr) -> Wrapper
     * @return nb::object containing the cached or newly created wrapper
     */
    template <typename ImplType, typename Creator>
    nb::object get_or_create_wrapper(const ImplType *impl, const control_block_ptr &control_block, Creator &&creator) {
        if (!impl) { return nb::none(); }

        // const_cast is safe here - we need non-const to access intrusive_base methods
        auto *mutable_impl = const_cast<ImplType *>(impl);

        // Check if we already have a cached Python wrapper
        PyObject *cached_ptr = mutable_impl->self_py();
        if (cached_ptr) { return nb::borrow(cached_ptr); }

        // Create new wrapper using the provided creator
        auto       wrapper = creator(mutable_impl, control_block);
        nb::object py_obj  = nb::cast(std::move(wrapper));

        // Cache and return
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
    }

    nb::object wrap_node(const Node *impl) { return wrap_node(impl, impl->graph()->control_block()); }

    template <typename T> std::optional<nb::object> wrap_map_node_t(const Node *node) {
        if (auto i = dynamic_cast<const MeshNode<T> *>(node); i) {
            return get_or_create_wrapper(i, node->graph()->control_block(),
                                         [](MeshNode<T> *impl, const auto &cb) {
                                             PyNode::api_ptr ptr{impl, cb};
                                             return PyMeshNestedNode::make_mesh_node<T>(std::move(ptr));
                                         });
        }
        return std::nullopt;
    }

    std::optional<nb::object> map_node(const Node *impl) {
        if (auto r = wrap_map_node_t<bool>(impl)) { return r; }
        if (auto r = wrap_map_node_t<int64_t>(impl)) { return r; }
        if (auto r = wrap_map_node_t<double>(impl)) { return r; }
        if (auto r = wrap_map_node_t<engine_date_t>(impl)) { return r; }
        if (auto r = wrap_map_node_t<engine_time_t>(impl)) { return r; }
        if (auto r = wrap_map_node_t<engine_time_delta_t>(impl)) { return r; }
        if (auto r = wrap_map_node_t<nb::object>(impl)) { return r; }
        return std::nullopt;
    }

    nb::object wrap_node(const Node *impl, const control_block_ptr &control_block) {
        // TODO: Make this better, but for now:
        if (dynamic_cast<const LastValuePullNode *>(impl)) {
            return get_or_create_wrapper(impl, control_block,
                                         [](auto impl, const auto &cb) { return PyLastValuePullNode({impl, cb}); });
        }
        if (dynamic_cast<const PushQueueNode *>(impl)) {
            return get_or_create_wrapper(impl, control_block,
                                         [](auto impl, const auto &cb) { return PyPushQueueNode({impl, cb}); });
        }
        if (auto r_val = map_node(impl)) { return *r_val; }
        if (dynamic_cast<const NestedNode *>(impl)) {
            return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNestedNode({impl, cb}); });
        }
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNode({impl, cb}); });
    }

    nb::object wrap_graph(const Graph *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyGraph({impl, cb}); });
    }

    nb::object wrap_node_scheduler(const NodeScheduler *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNodeScheduler({impl, cb}); });
    }

    static constexpr auto ts_opaque_input_types_v = tp::tpack_v<
        TimeSeriesSignalInput, TimeSeriesListInput, TimeSeriesBundleInput //, IndexedTimeSeriesInput (not implemented yet?)
    > + ts_reference_input_types_v;
    // [NOTE] order must match `ts_opaque_input_types`
    using py_ts_opaque_input_types = tp::tpack<
        PyTimeSeriesSignalInput, PyTimeSeriesListInput, PyTimeSeriesBundleInput, //, PyIndexedTimeSeriesInput
        PyTimeSeriesReferenceInput, PyTimeSeriesValueReferenceInput, PyTimeSeriesWindowReferenceInput,
        PyTimeSeriesListReferenceInput, PyTimeSeriesSetReferenceInput, PyTimeSeriesDictReferenceInput,
        PyTimeSeriesBundleReferenceInput
    >;

    static constexpr auto input_v = ddv::serial{
        // typed inputs
        []<typename T>(TimeSeriesDictInput_T<T>* input, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<T>>(input, std::move(cb)));
        },
        []<typename T>(TimeSeriesSetInput_T<T>* input, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<T>>(input, std::move(cb)));
        },
        []<typename T>(TimeSeriesWindowInput<T>* input, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesWindowInput<T>(input, std::move(cb)));
        },
        // value inputs
        [](TimeSeriesValueInputBase* input, control_block_ptr cb) { return nb::cast(PyTimeSeriesValueInput(input, std::move(cb))); },
        // opaque inputs
        []<typename TS>(TS* input, control_block_ptr cb) requires (tp::contains<TS>(ts_opaque_input_types_v)) {
            if constexpr (std::is_same_v<TS, TimeSeriesReferenceInput>) {
                // BUG: Encountered a base TimeSeriesReferenceInput that doesn't match any specialized type.
                // There should not be naked instances of TimeSeriesReferenceInput - they should always be
                // one of the specialized types. This indicates a bug where a base TimeSeriesReferenceInput
                // was created instead of a specialized type.
                throw std::runtime_error(
                    "Python wrap input TS: Encountered a base TimeSeriesReferenceInput "
                    "that doesn't match any specialized type. This is a bug - there should not be naked instances of "
                    "TimeSeriesReferenceInput. Check where this input was created (likely in reduce_node.cpp::zero_node).");
            }
            else {
                using py_type = tp::get_t<tp::find<TS>(ts_opaque_input_types_v), py_ts_opaque_input_types>;
                return nb::cast(py_type(input, std::move(cb)));
            }
        },
        [] { return nb::none(); }
    };

    static constexpr auto ts_opaque_output_types_v = tp::tpack_v<
        TimeSeriesListOutput, TimeSeriesBundleOutput // IndexedTimeSeriesOutput
    > + ts_reference_output_types_v;
    // [NOTE] order must match `ts_opaque_output_types_v`
    using py_ts_opaque_output_types = tp::tpack<
        PyTimeSeriesListOutput, PyTimeSeriesBundleOutput, // PyIndexedTimeSeriesOutput
        PyTimeSeriesReferenceOutput, PyTimeSeriesValueReferenceOutput, PyTimeSeriesWindowReferenceOutput,
        PyTimeSeriesListReferenceOutput, PyTimeSeriesSetReferenceOutput, PyTimeSeriesDictReferenceOutput,
        PyTimeSeriesBundleReferenceOutput
    >;

    static constexpr auto output_v = ddv::serial{
        // typed outputs
        []<typename T>(TimeSeriesDictOutput_T<T>* output, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<T>>(output, std::move(cb)));
        },
        []<typename T>(TimeSeriesSetOutput_T<T>* output, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<T>>(output, std::move(cb)));
        },
        []<typename T>(TimeSeriesFixedWindowOutput<T>* output, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<T>>(output, std::move(cb)));
        },
        []<typename T>(TimeSeriesTimeWindowOutput<T>* output, control_block_ptr cb) {
            return nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<T>>(output, std::move(cb)));
        },
        // value outputs
        [](TimeSeriesValueOutputBase* output, control_block_ptr cb) { return nb::cast(PyTimeSeriesValueOutput(output, std::move(cb))); },
        // opaque outputs
        []<typename TS>(TS* output, control_block_ptr cb) requires (tp::contains<TS>(ts_opaque_output_types_v)) {
            if constexpr (std::is_same_v<TS, TimeSeriesReferenceOutput>) {
                throw std::runtime_error(
                    "Python wrap output TS: Encountered a base TimeSeriesReferenceOutput "
                    "that doesn't match any specialized type. This is a bug - there should not be naked instances of "
                    "TimeSeriesReferenceOutput. Check where this output was created (likely in reduce_node.cpp::zero_node).");
            }
            else {
                using py_type = tp::get_t<tp::find<TS>(ts_opaque_output_types_v), py_ts_opaque_output_types>;
                return nb::cast(py_type(output, std::move(cb)));
            }
        },
        [] { return nb::none(); }
    };

    nb::object wrap_input(const TimeSeriesInput *impl, control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto* impl, const auto &cb) {
            return *impl->visit(input_v, cb);
        });
    }

    nb::object wrap_input(const TimeSeriesInput *impl) { return wrap_input(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_output(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto* impl, const auto &cb) {
            return *impl->visit(output_v, cb);
        });
    }

    // wrap when no control block is readily available.
    nb::object wrap_output(const TimeSeriesOutput *impl) { return wrap_output(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_time_series(const TimeSeriesInput *impl, const control_block_ptr &control_block) {
        return wrap_input(impl, control_block);
    }

    nb::object wrap_time_series(const TimeSeriesInput *impl) { return wrap_input(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_time_series(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return wrap_output(impl, control_block);
    }

    nb::object wrap_time_series(const TimeSeriesOutput *impl) { return wrap_output(impl, impl->owning_graph()->control_block()); }

    Node *unwrap_node(const nb::handle &obj) {
        if (auto *py_node = nb::inst_ptr<PyNode>(obj)) { unwrap_node(*py_node); }
        return nullptr;
    }

    Node *unwrap_node(const PyNode &node_) { return node_._impl.get(); }

    TimeSeriesInput *unwrap_input(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return nullptr;
    }

    TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_) {
        // return input_._impl.get();
        return input_.impl();
    }

    TimeSeriesOutput *unwrap_output(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return nullptr;
    }

    TimeSeriesOutput *unwrap_output(const PyTimeSeriesOutput &output_) { return output_.impl(); }

    nb::object wrap_evaluation_engine_api(const EvaluationEngineApi *impl, control_block_ptr control_block) {
        if (!impl) { return nb::none(); }
        return get_or_create_wrapper(impl, std::move(control_block), [](EvaluationEngineApi *impl, const auto &cb) {
            return PyEvaluationEngineApi({impl, cb});
        });
    }

    nb::object wrap_evaluation_clock(const EvaluationClock *impl, control_block_ptr control_block) {
        if (!impl) { return nb::none(); }
        return get_or_create_wrapper(impl, std::move(control_block), [](EvaluationClock *mutable_impl, const auto &cb) {
            return PyEvaluationClock({mutable_impl, cb});
        });
    }

    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block) {
        // Don't cache traits wrappers - traits is a member of Graph, not a separate heap object
        // Caching on intrusive_base could cause issues during graph teardown
        if (!impl) { return nb::none(); }
        auto *mutable_impl = const_cast<Traits *>(impl);
        auto  wrapper      = PyTraits({mutable_impl, std::move(control_block)});
        return nb::cast(std::move(wrapper));
    }

}  // namespace hgraph
