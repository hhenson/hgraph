//
// Wrapper Factory Implementation
//

#include "hgraph/api/python/py_ref.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
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

    nb::object wrap_node(const Node *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNode({impl, cb}); });
    }

    nb::object wrap_graph(const Graph *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyGraph({impl, cb}); });
    }

    nb::object wrap_node_scheduler(const NodeScheduler *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNodeScheduler({impl, cb}); });
    }

    // CRTP visitor for wrapping TimeSeriesInput objects
    // Now that concrete input types have CRTP accept() methods, we can use the more efficient CRTP pattern
    struct WrapInputVisitor : TimeSeriesInputVisitorCRTP<WrapInputVisitor>
    {
        control_block_ptr control_block;
        nb::object        wrapped_visitor;

        explicit WrapInputVisitor(control_block_ptr control_block_) : control_block(std::move(control_block_)) {}

        // Handle value inputs (template)
        template <typename T> void visit(TimeSeriesValueInput<T> &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesInput(&source, control_block));
        }

        // Handle reference inputs
        void visit(TimeSeriesValueReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesBundleReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesSetReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesListReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesDictReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesWindowReferenceInput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceInput(&source, control_block));
        }

        // Handle other input types (fallback/stub implementations)
        void visit(TimeSeriesBundleInput &source) {
            // TODO: Implement
        }

        template <typename K> void visit(TimeSeriesSetInput_T<K> &source) {
            // TODO: Implement
        }

        void visit(TimeSeriesListInput &source) {
            // TODO: Implement
        }

        template <typename K> void visit(TimeSeriesDictInput_T<K> &source) {
            // TODO: Implement
        }

        void visit(TimeSeriesSignalInput &source) {
            // TODO: Implement
        }

        template <typename T> void visit(TimeSeriesWindowInput<T> &source) {
            // TODO: Implement
        }
    };

    nb::object wrap_input(const TimeSeriesInput *impl) { return wrap_input(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_input(const TimeSeriesInput *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) {
            WrapInputVisitor visitor(cb);
            visit_timeseries(visitor, *impl);
            return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
        });
    }

    // CRTP visitor for wrapping TimeSeriesInput objects
    // Now that concrete input types have CRTP accept() methods, we can use the more efficient CRTP pattern
    struct WrapOutputVisitor : TimeSeriesOutputVisitorCRTP<WrapOutputVisitor>
    {
        control_block_ptr control_block;
        nb::object        wrapped_visitor;

        explicit WrapOutputVisitor(control_block_ptr control_block_) : control_block(std::move(control_block_)) {}

        // Handle value inputs (template)
        template <typename T> void visit(TimeSeriesValueOutput<T> &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesOutput(&source, control_block));
        }

        // Handle reference inputs
        void visit(TimeSeriesValueReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesBundleReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesSetReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesListReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesDictReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesWindowReferenceOutput &source) {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceOutput(&source, control_block));
        }

        // Handle other input types (fallback/stub implementations)
        void visit(TimeSeriesBundleOutput &source) {
            // TODO: Implement
        }

        template <typename K> void visit(TimeSeriesSetOutput_T<K> &source) {
            // TODO: Implement
        }

        void visit(TimeSeriesListOutput &source) {
            // TODO: Implement
        }

        template <typename K> void visit(TimeSeriesDictOutput_T<K> &source) {
            // TODO: Implement
        }

        template <typename T> void visit(TimeSeriesFixedWindowOutput<T> &source) {
            // TODO: Implement
        }

        template <typename T> void visit(TimeSeriesTimeWindowOutput<T> &source) {
            // TODO: Implement
        }
    };

    // wrap when no control block is readily available.
    nb::object wrap_output(const TimeSeriesOutput *impl) { return wrap_output(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_output(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) {
            WrapOutputVisitor visitor(cb);
            visit_timeseries(visitor, *impl);
            return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
        });
    }

    nb::object wrap_time_series(const TimeSeriesInput *impl, const control_block_ptr &control_block) {
        return wrap_input(impl, control_block);
    }

    nb::object wrap_time_series(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return wrap_output(impl, control_block);
    }

    Node *unwrap_node(const nb::object &obj) {
        if (auto *py_node = nb::inst_ptr<PyNode>(obj)) { return py_node->_impl.get(); }
        return nullptr;
    }

    TimeSeriesInput *unwrap_input(const nb::object &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return nullptr;
    }

    TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_) {
        // return input_._impl.get();
        return input_.impl();
    }

    TimeSeriesOutput *unwrap_output(const nb::object &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return nullptr;
    }

    TimeSeriesOutput *unwrap_output(const PyTimeSeriesOutput &output_) { return output_.impl(); }

    // nb::object wrap_evaluation_engine_api(const EvaluationEngineApi *impl, control_block_ptr control_block) {
    //     return get_or_create_wrapper(impl, std::move(control_block), [](EvaluationEngineApi *impl, control_block_ptr cb)
    //     {
    //         return PyEvaluationEngineApi(impl, std::move(cb));
    //     });
    // }
    //
    // nb::object wrap_evaluation_clock(const EvaluationClock *impl, control_block_ptr control_block) {
    //     if (impl == nullptr) { return nb::none(); }
    //
    //     return get_or_create_wrapper(impl, control_block, [](EvaluationClock *mutable_impl, control_block_ptr cb) {
    //         return PyEvaluationClock(mutable_impl, std::move(cb));
    //     });
    // }

    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block) {
        // Don't cache traits wrappers - traits is a member of Graph, not a separate heap object
        // Caching on intrusive_base could cause issues during graph teardown
        if (!impl) { return nb::none(); }
        auto *mutable_impl = const_cast<Traits *>(impl);
        auto  wrapper      = PyTraits({mutable_impl, std::move(control_block)});
        return nb::cast(std::move(wrapper));
    }

}  // namespace hgraph
